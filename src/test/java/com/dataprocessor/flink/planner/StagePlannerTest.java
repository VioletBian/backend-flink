package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.runtime.RuntimeRow;
import com.dataprocessor.flink.runtime.RuntimeTable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StagePlannerTest {

    private final PipelineContractNormalizer normalizer = new PipelineContractNormalizer(new ObjectMapper());
    private final StagePlanner planner = new StagePlanner(new OperatorCapabilityRegistry());

    @Test
    void materializesRuntimeExecutionFromEnableParallelIntent() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "formatter",
                "params": {
                  "method": "StringPrefix",
                  "columns": ["Price"],
                  "value_expr": "USD "
                }
              }
            ]
            """);

        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);

        Assertions.assertEquals(
            ExecutionConfig.AUTO,
            ((Map<?, ?>) prepared.get(0).get("execution")).get("strategy")
        );
    }

    @Test
    void choosesColumnStageForWideFormatterPipeline() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson(buildWideFormatterPipeline(40));
        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);
        List<OperationSpec> specs = planner.parsePipelineSpecs(prepared, ExecutionConfig.AUTO);
        StagePlanner.CandidateSegment candidateSegment = planner.collectCandidateSegment(specs, 0);

        StagePlan stagePlan = planner.chooseStage(buildWideRuntimeTable(8, 40), candidateSegment, 0);

        Assertions.assertEquals(ExecutionConfig.COLUMNS, stagePlan.getStrategy());
        Assertions.assertTrue(stagePlan.isNativeCapable());
        Assertions.assertEquals("FLINK_NATIVE", stagePlan.getExecutorKind());
    }

    @Test
    void keepsBarrierOperatorsOnSerialFallback() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "aggregate",
                "params": {
                  "by": ["Client Account"],
                  "actions": {
                    "method": "sum",
                    "on": ["Price"]
                  }
                }
              }
            ]
            """);
        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);
        List<OperationSpec> specs = planner.parsePipelineSpecs(prepared, ExecutionConfig.AUTO);
        StagePlanner.CandidateSegment candidateSegment = planner.collectCandidateSegment(specs, 0);

        StagePlan stagePlan = planner.chooseStage(buildWideRuntimeTable(4, 2), candidateSegment, 0);

        Assertions.assertEquals(ExecutionConfig.SERIAL, stagePlan.getStrategy());
        Assertions.assertFalse(stagePlan.isNativeCapable());
        Assertions.assertEquals("barrier-operator", stagePlan.getFallbackReason());
    }

    private String buildWideFormatterPipeline(int columnCount) {
        List<String> columns = new ArrayList<>();
        for (int index = 0; index < columnCount; index++) {
            columns.add("col_" + index);
        }
        return """
            [
              {
                "type": "formatter",
                "params": {
                  "method": "ToUpperCase",
                  "columns": %s,
                  "value_expr": ""
                }
              }
            ]
            """.formatted(new ObjectMapper().valueToTree(columns).toString());
    }

    private RuntimeTable buildWideRuntimeTable(int rowCount, int columnCount) {
        List<String> columns = new ArrayList<>();
        for (int index = 0; index < columnCount; index++) {
            columns.add("col_" + index);
        }

        List<RuntimeRow> rows = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            for (String column : columns) {
                values.put(column, "value_" + rowIndex);
            }
            rows.add(new RuntimeRow(rowIndex, values));
        }
        return new RuntimeTable(columns, rows);
    }
}
