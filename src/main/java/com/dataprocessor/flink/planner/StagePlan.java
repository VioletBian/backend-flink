package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

// 中文说明：StagePlan 对齐 Python 的 segment 概念，既保留 spec 级信息，也保留 run/debug 需要的摘要字段。
public class StagePlan {

    private final int stageIndex;
    private final String strategy;
    private final List<Integer> stepIndexes;
    private final List<String> operationTypes;
    private final List<OperationSpec> specs;
    private final int maxWorkers;
    private final boolean nativeCapable;
    private final String fallbackReason;

    public StagePlan(
        int stageIndex,
        String strategy,
        List<Integer> stepIndexes,
        List<String> operationTypes,
        List<OperationSpec> specs,
        int maxWorkers,
        boolean nativeCapable,
        String fallbackReason
    ) {
        this.stageIndex = stageIndex;
        this.strategy = strategy;
        this.stepIndexes = new ArrayList<>(stepIndexes);
        this.operationTypes = new ArrayList<>(operationTypes);
        this.specs = new ArrayList<>(specs);
        this.maxWorkers = maxWorkers;
        this.nativeCapable = nativeCapable;
        this.fallbackReason = fallbackReason;
    }

    public int getStageIndex() {
        return stageIndex;
    }

    public String getStrategy() {
        return strategy;
    }

    public List<Integer> getStepIndexes() {
        return new ArrayList<>(stepIndexes);
    }

    public List<Integer> getLogicalStepIndexes() {
        Set<Integer> unique = new LinkedHashSet<>(stepIndexes);
        return new ArrayList<>(unique);
    }

    public List<String> getOperationTypes() {
        return new ArrayList<>(operationTypes);
    }

    public List<OperationSpec> getSpecs() {
        return new ArrayList<>(specs);
    }

    public int getMaxWorkers() {
        return maxWorkers;
    }

    public boolean isNativeCapable() {
        return nativeCapable;
    }

    public String getFallbackReason() {
        return fallbackReason;
    }

    public String getExecutorKind() {
        return nativeCapable ? "FLINK_NATIVE" : "PYTHON_FALLBACK";
    }
}
