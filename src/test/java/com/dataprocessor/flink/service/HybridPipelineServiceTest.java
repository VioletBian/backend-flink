package com.dataprocessor.flink.service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.planner.PipelineContractNormalizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HybridPipelineServiceTest {

    @Mock
    private PipelineRunService pipelineRunService;

    private HybridPipelineService hybridPipelineService;

    @BeforeEach
    void setUp() {
        hybridPipelineService = new HybridPipelineService(
            new PipelineContractNormalizer(new com.fasterxml.jackson.databind.ObjectMapper()),
            pipelineRunService
        );
    }

    @Test
    void delegatesRunRequestWithEnableParallelFlag() {
        when(pipelineRunService.runPipeline(any(), anyString(), anyBoolean(), anyBoolean())).thenReturn(
            Map.of(
                "columns", List.of("Client Account"),
                "data", List.of()
            )
        );

        MockMultipartFile input = new MockMultipartFile(
            "file",
            "sample.csv",
            "text/csv",
            "Client Account\n7000000403\n".getBytes(StandardCharsets.UTF_8)
        );

        Map<String, Object> response = hybridPipelineService.runPipeline(
            input,
            """
                [{"type":"formatter","params":{"method":"ToUpperCase","columns":["Client Account"],"value_expr":""}}]
                """,
            true,
            false
        );

        Assertions.assertEquals(List.of("Client Account"), response.get("columns"));
    }
}
