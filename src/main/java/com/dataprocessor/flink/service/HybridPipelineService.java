package com.dataprocessor.flink.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.planner.PipelineContractNormalizer;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class HybridPipelineService {

    private final PipelineContractNormalizer pipelineContractNormalizer;
    private final PipelineRunService pipelineRunService;

    public HybridPipelineService(
        PipelineContractNormalizer pipelineContractNormalizer,
        PipelineRunService pipelineRunService
    ) {
        this.pipelineContractNormalizer = pipelineContractNormalizer;
        this.pipelineRunService = pipelineRunService;
    }

    public List<Map<String, Object>> normalizePipelineForStorage(Object payload) {
        return pipelineContractNormalizer.normalizePayload(payload);
    }

    public Map<String, Object> runPipeline(
        MultipartFile file,
        String pipelineJson,
        boolean enableParallel,
        boolean debug
    ) {
        try {
            return pipelineRunService.runPipeline(file.getBytes(), pipelineJson, enableParallel, debug);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read uploaded input file.", exception);
        }
    }
}
