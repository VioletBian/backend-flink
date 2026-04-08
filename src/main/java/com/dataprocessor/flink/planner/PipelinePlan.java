package com.dataprocessor.flink.planner;

import java.util.List;
import java.util.Map;

public class PipelinePlan {

    private final List<Map<String, Object>> normalizedPipeline;
    private final List<PlannedOperator> operators;
    private final List<StagePlan> stages;
    private final int nativeCandidateCount;
    private final int fallbackCount;

    public PipelinePlan(
        List<Map<String, Object>> normalizedPipeline,
        List<PlannedOperator> operators,
        List<StagePlan> stages,
        int nativeCandidateCount,
        int fallbackCount
    ) {
        this.normalizedPipeline = normalizedPipeline;
        this.operators = operators;
        this.stages = stages;
        this.nativeCandidateCount = nativeCandidateCount;
        this.fallbackCount = fallbackCount;
    }

    public List<Map<String, Object>> getNormalizedPipeline() {
        return normalizedPipeline;
    }

    public List<PlannedOperator> getOperators() {
        return operators;
    }

    public List<StagePlan> getStages() {
        return stages;
    }

    public int getNativeCandidateCount() {
        return nativeCandidateCount;
    }

    public int getFallbackCount() {
        return fallbackCount;
    }
}
