package com.dataprocessor.flink.controller;

import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.model.PipelineSaveRequest;
import com.dataprocessor.flink.service.HybridPipelineService;
import com.dataprocessor.flink.service.PipelineStoreService;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/dp/pipeline")
public class PipelineController {

    private final HybridPipelineService hybridPipelineService;
    private final PipelineStoreService pipelineStoreService;

    public PipelineController(
        HybridPipelineService hybridPipelineService,
        PipelineStoreService pipelineStoreService
    ) {
        this.hybridPipelineService = hybridPipelineService;
        this.pipelineStoreService = pipelineStoreService;
    }

    @PostMapping("/save")
    public Map<String, Object> savePipeline(@Valid @RequestBody PipelineSaveRequest request) {
        List<Map<String, Object>> canonicalPipeline = hybridPipelineService.normalizePipelineForStorage(request.getPipeline());
        return pipelineStoreService.savePipeline(request.getName(), canonicalPipeline);
    }

    @GetMapping("/get")
    public Map<String, Object> getPipeline(@RequestParam("name") String name) {
        return pipelineStoreService.getLatestPipeline(name);
    }

    @GetMapping("/versions")
    public Map<String, Object> getPipelineVersions(
        @RequestParam("name") String name,
        @RequestParam(name = "limit", defaultValue = "10") int limit
    ) {
        return pipelineStoreService.getPipelineVersions(name, limit);
    }

    @PostMapping(value = "/run", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> runPipeline(
        @RequestPart("file") MultipartFile file,
        @RequestPart("pipeline_json") String pipelineJson,
        @RequestParam(name = "enableParallel", defaultValue = "false") boolean enableParallel,
        @RequestParam(name = "debug", defaultValue = "false") boolean debug
    ) {
        return hybridPipelineService.runPipeline(file, pipelineJson, enableParallel, debug);
    }
}
