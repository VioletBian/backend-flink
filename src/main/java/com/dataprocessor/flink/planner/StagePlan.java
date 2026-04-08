package com.dataprocessor.flink.planner;

import java.util.List;

public class StagePlan {

    private final int stageIndex;
    private final String executionMode;
    private final List<Integer> operatorIndexes;
    private final List<String> operatorTypes;
    private final boolean rowSplitEligible;
    private final boolean columnSplitEligible;
    private final String optimizationHint;

    public StagePlan(
        int stageIndex,
        String executionMode,
        List<Integer> operatorIndexes,
        List<String> operatorTypes,
        boolean rowSplitEligible,
        boolean columnSplitEligible,
        String optimizationHint
    ) {
        this.stageIndex = stageIndex;
        this.executionMode = executionMode;
        this.operatorIndexes = operatorIndexes;
        this.operatorTypes = operatorTypes;
        this.rowSplitEligible = rowSplitEligible;
        this.columnSplitEligible = columnSplitEligible;
        this.optimizationHint = optimizationHint;
    }

    public int getStageIndex() {
        return stageIndex;
    }

    public String getExecutionMode() {
        return executionMode;
    }

    public List<Integer> getOperatorIndexes() {
        return operatorIndexes;
    }

    public List<String> getOperatorTypes() {
        return operatorTypes;
    }

    public boolean isRowSplitEligible() {
        return rowSplitEligible;
    }

    public boolean isColumnSplitEligible() {
        return columnSplitEligible;
    }

    public String getOptimizationHint() {
        return optimizationHint;
    }
}
