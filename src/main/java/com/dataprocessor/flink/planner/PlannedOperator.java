package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.List;

// 中文说明：PlannedOperator 保留 spec 级诊断信息，方便 debug 时解释某个算子为什么 native 或 fallback。
public class PlannedOperator {

    private final int specIndex;
    private final int sourceStepIndex;
    private final String operatorType;
    private final String requestedStrategy;
    private final String resolvedStrategy;
    private final boolean nativeCapable;
    private final String fallbackReason;
    private final List<String> supportedStrategies;

    public PlannedOperator(
        int specIndex,
        int sourceStepIndex,
        String operatorType,
        String requestedStrategy,
        String resolvedStrategy,
        boolean nativeCapable,
        String fallbackReason,
        List<String> supportedStrategies
    ) {
        this.specIndex = specIndex;
        this.sourceStepIndex = sourceStepIndex;
        this.operatorType = operatorType;
        this.requestedStrategy = requestedStrategy;
        this.resolvedStrategy = resolvedStrategy;
        this.nativeCapable = nativeCapable;
        this.fallbackReason = fallbackReason;
        this.supportedStrategies = new ArrayList<>(supportedStrategies);
    }

    public int getSpecIndex() {
        return specIndex;
    }

    public int getSourceStepIndex() {
        return sourceStepIndex;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public String getRequestedStrategy() {
        return requestedStrategy;
    }

    public String getResolvedStrategy() {
        return resolvedStrategy;
    }

    public boolean isNativeCapable() {
        return nativeCapable;
    }

    public String getFallbackReason() {
        return fallbackReason;
    }

    public List<String> getSupportedStrategies() {
        return new ArrayList<>(supportedStrategies);
    }
}
