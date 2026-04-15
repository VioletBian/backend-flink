package com.dataprocessor.flink.service;

import java.nio.charset.StandardCharsets;
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
        Assertions.assertTrue(response.containsKey("prepared_pipeline"));
        Assertions.assertTrue(response.containsKey("execution_plan"));
        Assertions.assertTrue(response.containsKey("step_snapshots"));
        Assertions.assertFalse(response.containsKey("engine_debug"));
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
        verify(pythonFallbackBridge, never()).run(any(), anyString(), anyBoolean());
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
