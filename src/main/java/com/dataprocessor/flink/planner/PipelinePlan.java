package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 中文说明：PipelinePlan 聚合一次 /run 的 runtime planning 结果，供 debug 响应和测试共用。
public class PipelinePlan {

    private final List<Map<String, Object>> normalizedPipeline;
    private final List<Map<String, Object>> preparedPipeline;
    private final List<PlannedOperator> operators;
    private final List<StagePlan> stages;

    public PipelinePlan(
        List<Map<String, Object>> normalizedPipeline,
        List<Map<String, Object>> preparedPipeline,
        List<PlannedOperator> operators,
        List<StagePlan> stages
    ) {
        this.normalizedPipeline = new ArrayList<>(normalizedPipeline);
        this.preparedPipeline = new ArrayList<>(preparedPipeline);
        this.operators = new ArrayList<>(operators);
        this.stages = new ArrayList<>(stages);
    }

    public List<Map<String, Object>> getNormalizedPipeline() {
        return new ArrayList<>(normalizedPipeline);
    }

    public List<Map<String, Object>> getPreparedPipeline() {
        return new ArrayList<>(preparedPipeline);
    }

    public List<PlannedOperator> getOperators() {
        return new ArrayList<>(operators);
    }

    public List<StagePlan> getStages() {
        return new ArrayList<>(stages);
    }

    public Map<String, Object> toExecutionPlanPayload() {
        List<Map<String, Object>> segments = new ArrayList<>();
        for (StagePlan stage : stages) {
            LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
            payload.put("strategy", stage.getStrategy());
            payload.put("step_indexes", stage.getStepIndexes());
            payload.put("operation_types", stage.getOperationTypes());
            payload.put("max_workers", stage.getMaxWorkers());
            segments.add(payload);
        }
        LinkedHashMap<String, Object> plan = new LinkedHashMap<>();
        plan.put("segments", segments);
        return plan;
    }
}
