package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.runtime.RowExpressionEvaluator;
import com.dataprocessor.flink.runtime.RuntimeRow;
import com.dataprocessor.flink.runtime.RuntimeTable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StagePlannerTest {

    private final PipelineContractNormalizer normalizer = new PipelineContractNormalizer(new ObjectMapper());
    private final StagePlanner planner = new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator());

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
    void keepsNativeAggregateOnSerialStage() {
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
        Assertions.assertTrue(stagePlan.isNativeCapable());
        Assertions.assertEquals("FLINK_NATIVE", stagePlan.getExecutorKind());
    }

    @Test
    void simpleFilterIsPlannedAsNativeRowStage() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "filter",
                "params": {
                  "requiredCols": ["Client Account", "Price"],
                  "condition": "`Price` > 10 and `Client Account` != null"
                }
              }
            ]
            """);
        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);
        List<OperationSpec> specs = planner.parsePipelineSpecs(prepared, ExecutionConfig.AUTO);
        StagePlanner.CandidateSegment candidateSegment = planner.collectCandidateSegment(specs, 0);

        StagePlan stagePlan = planner.chooseStage(buildFilterRuntimeTable(), candidateSegment, 0);

        Assertions.assertEquals(ExecutionConfig.SERIAL, stagePlan.getStrategy());
        Assertions.assertTrue(stagePlan.isNativeCapable());
    }

    @Test
    void pythonStyleFilterFallsBackToPythonStage() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "filter",
                "params": {
                  "requiredCols": ["Client Account"],
                  "condition": "`Client Account`.str.contains('700')"
                }
              }
            ]
            """);
        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);
        List<OperationSpec> specs = planner.parsePipelineSpecs(prepared, ExecutionConfig.AUTO);
        StagePlanner.CandidateSegment candidateSegment = planner.collectCandidateSegment(specs, 0);

        StagePlan stagePlan = planner.chooseStage(buildFilterRuntimeTable(), candidateSegment, 0);

        Assertions.assertFalse(stagePlan.isNativeCapable());
        Assertions.assertEquals("filter-condition-requires-python-fallback", stagePlan.getFallbackReason());
    }

    @Test
    void vectorizedColAssignIsPlannedAsNativeRowStage() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "col_assign",
                "params": {
                  "method": "vectorized",
                  "col_name": "Alert",
                  "value_expr": "`Price` * 2",
                  "condition": "`Client Account` != null"
                }
              }
            ]
            """);
        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);
        List<OperationSpec> specs = planner.parsePipelineSpecs(prepared, ExecutionConfig.AUTO);
        StagePlanner.CandidateSegment candidateSegment = planner.collectCandidateSegment(specs, 0);

        StagePlan stagePlan = planner.chooseStage(buildFilterRuntimeTable(), candidateSegment, 0);

        Assertions.assertTrue(stagePlan.isNativeCapable());
        Assertions.assertEquals(ExecutionConfig.SERIAL, stagePlan.getStrategy());
    }

    @Test
    void lambdaColAssignFallsBackToPythonStage() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "col_assign",
                "params": {
                  "method": "lambda",
                  "col_name": "Alert",
                  "value_expr": "lambda row: row['Price'] * 2",
                  "condition": "`Client Account` != null"
                }
              }
            ]
            """);
        List<Map<String, Object>> prepared = planner.materializeRunSteps(normalized, ExecutionConfig.AUTO);
        List<OperationSpec> specs = planner.parsePipelineSpecs(prepared, ExecutionConfig.AUTO);
        StagePlanner.CandidateSegment candidateSegment = planner.collectCandidateSegment(specs, 0);

        StagePlan stagePlan = planner.chooseStage(buildFilterRuntimeTable(), candidateSegment, 0);

        Assertions.assertFalse(stagePlan.isNativeCapable());
        Assertions.assertEquals("col-assign-method-requires-python-fallback", stagePlan.getFallbackReason());
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

    private RuntimeTable buildFilterRuntimeTable() {
        List<String> columns = List.of("Client Account", "Price");
        List<RuntimeRow> rows = List.of(
            new RuntimeRow(0L, new LinkedHashMap<>(Map.of("Client Account", "7001", "Price", 11L))),
            new RuntimeRow(1L, new LinkedHashMap<>(Map.of("Client Account", "7002", "Price", 9L)))
        );
        return new RuntimeTable(columns, rows);
    }
}
