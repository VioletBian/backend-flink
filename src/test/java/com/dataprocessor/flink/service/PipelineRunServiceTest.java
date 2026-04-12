package com.dataprocessor.flink.service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.planner.OperatorCapabilityRegistry;
import com.dataprocessor.flink.planner.PipelineContractNormalizer;
import com.dataprocessor.flink.planner.StagePlanner;
import com.dataprocessor.flink.runtime.CsvRuntimeTableCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
            new StagePlanner(new OperatorCapabilityRegistry()),
            new NativeStageExecutor(Mockito.mock(BatchTableEnvironmentFactory.class)),
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
}
