package com.dataprocessor.flink.service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.planner.PipelineContractNormalizer;
import com.dataprocessor.flink.planner.PipelinePlan;
import com.dataprocessor.flink.planner.StagePlanner;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class HybridPipelineService {

    private final PipelineContractNormalizer pipelineContractNormalizer;
    private final StagePlanner stagePlanner;
    private final PythonFallbackBridge pythonFallbackBridge;
    private final ObjectMapper objectMapper;
    private final BatchTableEnvironmentFactory batchTableEnvironmentFactory;

    public HybridPipelineService(
        PipelineContractNormalizer pipelineContractNormalizer,
        StagePlanner stagePlanner,
        PythonFallbackBridge pythonFallbackBridge,
        ObjectMapper objectMapper,
        BatchTableEnvironmentFactory batchTableEnvironmentFactory
    ) {
        this.pipelineContractNormalizer = pipelineContractNormalizer;
        this.stagePlanner = stagePlanner;
        this.pythonFallbackBridge = pythonFallbackBridge;
        this.objectMapper = objectMapper;
        this.batchTableEnvironmentFactory = batchTableEnvironmentFactory;
    }

    public List<Map<String, Object>> normalizePipelineForStorage(Object payload) {
        return pipelineContractNormalizer.normalizePayload(payload);
    }

    public Map<String, Object> runPipeline(MultipartFile file, String pipelineJson, boolean debug) {
        try {
            List<Map<String, Object>> canonicalPipeline = pipelineContractNormalizer.normalizeJson(pipelineJson);
            PipelinePlan pipelinePlan = stagePlanner.plan(canonicalPipeline);

            // 当前先用 Python reference runner 保证语义完全一致，同时保留 Flink batch 环境工厂作为 native path 落地入口。
            String canonicalPipelineJson = objectMapper.writeValueAsString(canonicalPipeline);
            Map<String, Object> response = pythonFallbackBridge.run(file.getBytes(), canonicalPipelineJson, debug);

            if (debug) {
                Map<String, Object> engineDebug = new LinkedHashMap<>();
                engineDebug.put("execution_mode", "PYTHON_REFERENCE_FALLBACK");
                engineDebug.put("native_candidate_count", pipelinePlan.getNativeCandidateCount());
                engineDebug.put("fallback_count", pipelinePlan.getFallbackCount());
                engineDebug.put("operators", pipelinePlan.getOperators());
                engineDebug.put("stages", pipelinePlan.getStages());
                response.put("engine_debug", engineDebug);
            }

            return response;
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize canonical pipeline JSON.", exception);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read uploaded input file.", exception);
        }
    }

    public BatchTableEnvironmentFactory getBatchTableEnvironmentFactory() {
        return batchTableEnvironmentFactory;
    }
}
