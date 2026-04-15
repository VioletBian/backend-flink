package com.dataprocessor.flink.service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dataprocessor.flink.planner.ExecutionConfig;
import com.dataprocessor.flink.planner.OperationSpec;
import com.dataprocessor.flink.planner.PipelineContractNormalizer;
import com.dataprocessor.flink.planner.PipelinePlan;
import com.dataprocessor.flink.planner.StagePlan;
import com.dataprocessor.flink.planner.StagePlanner;
import com.dataprocessor.flink.runtime.CsvRuntimeTableCodec;
import com.dataprocessor.flink.runtime.RuntimeTable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

@Service
public class PipelineRunService {

    private static final String PYTHON_FALLBACK_FAILURE_PREFIX = "Python fallback bridge failed:";
    private static final Pattern PYTHON_STEP_FAILURE_PATTERN = Pattern.compile(
        "^Step\\s+(\\d+)\\s+\\(([^)]+)\\)\\s+failed:\\s*(.*)$",
        Pattern.DOTALL
    );

    private final PipelineContractNormalizer pipelineContractNormalizer;
    private final StagePlanner stagePlanner;
    private final NativeStageExecutor nativeStageExecutor;
    private final PythonFallbackBridge pythonFallbackBridge;
    private final CsvRuntimeTableCodec csvRuntimeTableCodec;
    private final ObjectMapper objectMapper;

    public PipelineRunService(
        PipelineContractNormalizer pipelineContractNormalizer,
        StagePlanner stagePlanner,
        NativeStageExecutor nativeStageExecutor,
        PythonFallbackBridge pythonFallbackBridge,
        CsvRuntimeTableCodec csvRuntimeTableCodec,
        ObjectMapper objectMapper
    ) {
        this.pipelineContractNormalizer = pipelineContractNormalizer;
        this.stagePlanner = stagePlanner;
        this.nativeStageExecutor = nativeStageExecutor;
        this.pythonFallbackBridge = pythonFallbackBridge;
        this.csvRuntimeTableCodec = csvRuntimeTableCodec;
        this.objectMapper = objectMapper;
    }

    // 中文说明：这里是 Java /run 的唯一运行链，统一负责 execution enrich、segment 规划、native/fallback 执行和 debug 回填。
    public Map<String, Object> runPipeline(byte[] fileBytes, String pipelineJson, boolean enableParallel, boolean debug) {
        List<Map<String, Object>> normalizedPipeline = pipelineContractNormalizer.normalizeJson(pipelineJson);
        String defaultExecutionStrategy = enableParallel ? ExecutionConfig.AUTO : ExecutionConfig.SERIAL;
        List<Map<String, Object>> preparedPipeline = stagePlanner.materializeRunSteps(normalizedPipeline, defaultExecutionStrategy);
        List<OperationSpec> specs = stagePlanner.parsePipelineSpecs(preparedPipeline, defaultExecutionStrategy);

        RuntimeTable runtimeTable = csvRuntimeTableCodec.read(fileBytes);
        List<StagePlan> stages = new ArrayList<>();
        int pointer = 0;
        int stageIndex = 0;

        while (pointer < specs.size()) {
            StagePlanner.CandidateSegment candidateSegment = stagePlanner.collectCandidateSegment(specs, pointer);
            StagePlan stagePlan = stagePlanner.chooseStage(runtimeTable, candidateSegment, stageIndex++);
            stagePlanner.applyStageExecution(preparedPipeline, stagePlan);
            runtimeTable = executeStage(runtimeTable, stagePlan, preparedPipeline);
            stages.add(stagePlan);
            pointer = candidateSegment.getNextIndex();
        }

        PipelinePlan pipelinePlan = stagePlanner.buildPlan(normalizedPipeline, preparedPipeline, specs, stages);
        Map<String, Object> response = buildBaseResponse(runtimeTable);
        if (debug) {
            enrichDebugPayload(response, fileBytes, pipelinePlan);
        }
        return response;
    }

    private RuntimeTable executeStage(
        RuntimeTable runtimeTable,
        StagePlan stagePlan,
        List<Map<String, Object>> preparedPipeline
    ) {
        if (nativeStageExecutor.supports(stagePlan)) {
            return nativeStageExecutor.execute(runtimeTable, stagePlan);
        }
        List<Map<String, Object>> stagePipeline = new ArrayList<>();
        for (int stepIndex : stagePlan.getLogicalStepIndexes()) {
            stagePipeline.add(new LinkedHashMap<>(preparedPipeline.get(stepIndex)));
        }
        try {
            Map<String, Object> response = pythonFallbackBridge.run(
                csvRuntimeTableCodec.write(runtimeTable).getBytes(StandardCharsets.UTF_8),
                objectMapper.writeValueAsString(stagePipeline),
                false
            );
            Object csv = response.get("csv");
            if (csv == null) {
                throw new IllegalStateException("Python fallback stage did not return csv output.");
            }
            return csvRuntimeTableCodec.read(String.valueOf(csv));
        } catch (IllegalStateException exception) {
            throw wrapPythonFallbackFailure(stagePlan, preparedPipeline, exception);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize stage pipeline JSON.", exception);
        }
    }

    private Map<String, Object> buildBaseResponse(RuntimeTable runtimeTable) {
        LinkedHashMap<String, Object> response = new LinkedHashMap<>();
        response.put("columns", runtimeTable.getColumns());
        response.put("rows", csvRuntimeTableCodec.toResponseRows(runtimeTable));
        response.put("csv", csvRuntimeTableCodec.write(runtimeTable));
        return response;
    }

    // 中文说明：debug 优先保证与 Python backend 的外部契约一致，因此 step_snapshots 仍复用 Python prepared pipeline 跑一遍。
    private void enrichDebugPayload(Map<String, Object> response, byte[] fileBytes, PipelinePlan pipelinePlan) {
        try {
            Map<String, Object> pythonDebug = pythonFallbackBridge.run(
                fileBytes,
                objectMapper.writeValueAsString(pipelinePlan.getPreparedPipeline()),
                true
            );
            response.put("step_snapshots", pythonDebug.get("step_snapshots"));
            response.put("normalized_pipeline", pipelinePlan.getNormalizedPipeline());
            response.put("prepared_pipeline", pipelinePlan.getPreparedPipeline());
            response.put("execution_plan", pipelinePlan.toExecutionPlanPayload());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize prepared pipeline JSON for debug mode.", exception);
        }
    }

    private RuntimeException wrapPythonFallbackFailure(
        StagePlan stagePlan,
        List<Map<String, Object>> preparedPipeline,
        IllegalStateException exception
    ) {
        String message = exception.getMessage();
        if (message == null || !message.startsWith(PYTHON_FALLBACK_FAILURE_PREFIX)) {
            return exception;
        }

        String detail = message.substring(PYTHON_FALLBACK_FAILURE_PREFIX.length()).trim();
        int resolvedStepIndex = stagePlan.getLogicalStepIndexes().isEmpty() ? 0 : stagePlan.getLogicalStepIndexes().get(0);
        String resolvedOperatorType = stagePlan.getSpecs().isEmpty() ? "unknown" : stagePlan.getSpecs().get(0).getType();

        Matcher matcher = PYTHON_STEP_FAILURE_PATTERN.matcher(detail);
        if (matcher.matches()) {
            int stageLocalStepIndex = Integer.parseInt(matcher.group(1));
            List<Integer> logicalStepIndexes = stagePlan.getLogicalStepIndexes();
            if (stageLocalStepIndex >= 0 && stageLocalStepIndex < logicalStepIndexes.size()) {
                resolvedStepIndex = logicalStepIndexes.get(stageLocalStepIndex);
                Object rawType = preparedPipeline.get(resolvedStepIndex).get("type");
                if (rawType != null) {
                    resolvedOperatorType = String.valueOf(rawType);
                }
            }
            detail = matcher.group(3).trim();
        }

        return new PipelineStepExecutionException(resolvedStepIndex, resolvedOperatorType, detail, exception);
    }
}
