package com.dataprocessor.flink.service;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.planner.OperatorCapabilityRegistry;
import com.dataprocessor.flink.planner.PipelineContractNormalizer;
import com.dataprocessor.flink.planner.StagePlanner;
import com.dataprocessor.flink.runtime.CsvRuntimeTableCodec;
import com.dataprocessor.flink.runtime.RowExpressionEvaluator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PipelineRunServiceTest {

    @Test
    void returnsBackendCompatibleDebugPayloadForPreparedPipeline() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);
        when(pythonFallbackBridge.run(any(), anyString(), eq(true))).thenReturn(
            Map.of("step_snapshots", List.of(Map.of("step_index", 0, "type", "formatter")))
        );

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "Client Account,Price\nabc,12.3\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "formatter",
                    "params": {
                      "method": "ToUpperCase",
                      "columns": ["Client Account"],
                      "value_expr": ""
                    }
                  }
                ]
                """,
            false,
            true
        );

        Assertions.assertEquals(List.of("Client Account", "Price"), response.get("columns"));
        Assertions.assertEquals(List.of(List.of("ABC", 12.3)), response.get("data"));
        Assertions.assertTrue(response.containsKey("prepared_pipeline"));
        Assertions.assertTrue(response.containsKey("execution_plan"));
        Assertions.assertTrue(response.containsKey("step_snapshots"));
        Assertions.assertFalse(response.containsKey("engine_debug"));
        Assertions.assertFalse(response.containsKey("csv"));
    }

    @Test
    void nativePipelineDebugDoesNotFailWhenPythonSnapshotsAreUnavailable() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);
        when(pythonFallbackBridge.run(any(), anyString(), eq(true))).thenThrow(
            new IllegalStateException("Python fallback bridge failed: python executable not found")
        );

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "Client Account,Price\n7001,12\n7002,9\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "tag",
                    "params": {
                      "conditions": ["`Price` >= 10"],
                      "tag_col_name": "Alert",
                      "tags": ["HIGH"],
                      "default_tag": "LOW"
                    }
                  }
                ]
                """,
            false,
            true
        );

        Assertions.assertEquals(List.of("Client Account", "Price", "Alert"), response.get("columns"));
        Assertions.assertEquals(List.of(List.of(7001L, 12L, "HIGH"), List.of(7002L, 9L, "LOW")), response.get("data"));
        Assertions.assertTrue(response.containsKey("normalized_pipeline"));
        Assertions.assertTrue(response.containsKey("prepared_pipeline"));
        Assertions.assertTrue(response.containsKey("execution_plan"));
        Assertions.assertEquals(
            "Unable to collect python parity step_snapshots: Python fallback bridge failed: python executable not found",
            response.get("debug_warning")
        );
        Assertions.assertFalse(response.containsKey("step_snapshots"));
        verify(pythonFallbackBridge, never()).run(any(), anyString(), eq(false));
    }

    @Test
    void nativePipelineDoesNotInvokePythonFallbackWhenDebugIsDisabled() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "Client Account,Price\n7001,12\n7002,9\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "tag",
                    "params": {
                      "conditions": ["`Price` >= 10"],
                      "tag_col_name": "Alert",
                      "tags": ["HIGH"],
                      "default_tag": "LOW"
                    }
                  }
                ]
                """,
            false,
            false
        );

        Assertions.assertEquals(List.of("Client Account", "Price", "Alert"), response.get("columns"));
        Assertions.assertEquals(List.of(List.of(7001L, 12L, "HIGH"), List.of(7002L, 9L, "LOW")), response.get("data"));
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
    }

    @Test
    void tagWithSimpleStringConditionsStaysOnNativePath() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "A,B,C\n3,1,0\n1,3,5\n0,0,4\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "tag",
                    "params": {
                      "conditions": "A > 2 & B < 2, B > 2, C > 3",
                      "tag_col_name": "RuleTag",
                      "tags": "MATCH_AB, MATCH_B, MATCH_C",
                      "default_tag": "OTHER"
                    }
                  }
                ]
                """,
            false,
            false
        );

        Assertions.assertEquals(List.of("A", "B", "C", "RuleTag"), response.get("columns"));
        Assertions.assertEquals(
            List.of(
                List.of(3L, 1L, 0L, "MATCH_AB"),
                List.of(1L, 3L, 5L, "MATCH_B"),
                List.of(0L, 0L, 4L, "MATCH_C")
            ),
            response.get("data")
        );
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
    }

    @Test
    void constantAndValueMappingPipelineStaysOnNativePath() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "Client Account,Desk\n7001,SH\n7002,LDN\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "constant",
                    "params": {
                      "columns": {
                        "Region": "APAC"
                      }
                    }
                  },
                  {
                    "type": "value_mapping",
                    "params": {
                      "mode": "map",
                      "mappings": {
                        "Desk": {
                          "SH": "CN"
                        }
                      },
                      "default": "OTHER"
                    }
                  }
                ]
                """,
            false,
            false
        );

        Assertions.assertEquals(List.of("Client Account", "Desk", "Region", "Desk_mapped"), response.get("columns"));
        Assertions.assertEquals(
            List.of(List.of(7001L, "SH", "APAC", "CN"), List.of(7002L, "LDN", "APAC", "OTHER")),
            response.get("data")
        );
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
    }

    @Test
    void aggregateWithDisplayCaseMethodStaysOnNativePath() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "Client Account,Contract,Quantity\nA,C1,10\nA,C1,15\nB,C2,7\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "aggregate",
                    "params": {
                      "by": ["Client Account", "Contract"],
                      "actions": {
                        "method": "Sum",
                        "on": ["Quantity"]
                      }
                    }
                  }
                ]
                """,
            false,
            false
        );

        Assertions.assertEquals(List.of("Client Account", "Contract", "Quantity"), response.get("columns"));
        Assertions.assertEquals(
            List.of(List.of("A", "C1", 25L), List.of("B", "C2", 7L)),
            response.get("data")
        );
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
    }

    @Test
    void aggregateWithStringFieldsStaysOnNativePath() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "Client Account,Contract,Quantity\nA,C1,10\nA,C1,15\nB,C2,7\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "aggregate",
                    "params": {
                      "by": "Client Account,Contract",
                      "actions": {
                        "method": "sum",
                        "on": "Quantity",
                        "rename": ""
                      }
                    }
                  }
                ]
                """,
            false,
            false
        );

        Assertions.assertEquals(List.of("Client Account", "Contract", "Quantity"), response.get("columns"));
        Assertions.assertEquals(
            List.of(List.of("A", "C1", 25L), List.of("B", "C2", 7L)),
            response.get("data")
        );
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
    }

    @Test
    void fallbackStageReadsCompactPythonResponse() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);
        when(pythonFallbackBridge.run(any(), anyString(), eq(false))).thenReturn(
            Map.of(
                "columns", List.of("A", "Shifted"),
                "data", List.of(Arrays.asList(1L, null), List.of(2L, 1L))
            )
        );

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        Map<String, Object> response = pipelineRunService.runPipeline(
            "A\n1\n2\n".getBytes(StandardCharsets.UTF_8),
            """
                [
                  {
                    "type": "series_transform",
                    "params": {
                      "on": ["A"],
                      "transform_expr": "lambda x: x.shift(1)",
                      "rename": ["Shifted"]
                    }
                  }
                ]
                """,
            false,
            false
        );

        Assertions.assertEquals(List.of("A", "Shifted"), response.get("columns"));
        Assertions.assertEquals(List.of(Arrays.asList(1L, null), List.of(2L, 1L)), response.get("data"));
    }

    @Test
    void nativeMissingSortColumnRaisesStepScopedBadRequest() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> pipelineRunService.runPipeline(
                "A,B,C,D\n1,2,3,4\n".getBytes(StandardCharsets.UTF_8),
                """
                    [
                      {
                        "type": "sort",
                        "params": {
                          "by": ["E"],
                          "ascending": [true]
                        }
                      }
                    ]
                    """,
                false,
                false
            )
        );

        Assertions.assertEquals("Step 1 (sort) failed: Column `E` does not exist.", exception.getMessage());
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
    }

    @Test
    void fallbackFailureIsRemappedToOriginalPipelineStep() {
        PythonFallbackBridge pythonFallbackBridge = Mockito.mock(PythonFallbackBridge.class);
        when(pythonFallbackBridge.run(any(), anyString(), eq(false))).thenThrow(
            new IllegalStateException("Python fallback bridge failed: Step 0 (series_transform) failed: column `E` does not exist.")
        );

        PipelineRunService pipelineRunService = new PipelineRunService(
            new PipelineContractNormalizer(new ObjectMapper()),
            new StagePlanner(new OperatorCapabilityRegistry(), new RowExpressionEvaluator()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class), new RowExpressionEvaluator()),
            pythonFallbackBridge,
            new CsvRuntimeTableCodec(),
            new ObjectMapper()
        );

        IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> pipelineRunService.runPipeline(
                "A,B,C,D\n1,2,3,4\n".getBytes(StandardCharsets.UTF_8),
                """
                    [
                      {
                        "type": "series_transform",
                        "params": {
                          "on": ["E"],
                          "transform_expr": "lambda x: x.shift(1)"
                        }
                      }
                    ]
                    """,
                false,
                false
            )
        );

        Assertions.assertEquals("Step 1 (series_transform) failed: column `E` does not exist.", exception.getMessage());
    }
}
