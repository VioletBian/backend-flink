package com.dataprocessor.flink.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class PipelineSaveRequest {

    @NotBlank
    private String name;

    @NotNull
    private Object pipeline;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getPipeline() {
        return pipeline;
    }

    public void setPipeline(Object pipeline) {
        this.pipeline = pipeline;
    }
}
