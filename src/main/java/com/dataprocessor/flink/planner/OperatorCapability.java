package com.dataprocessor.flink.planner;

public class OperatorCapability {

    private final String operatorType;
    private final boolean rowLocal;
    private final boolean crossRow;
    private final boolean groupSensitive;
    private final boolean sortBarrier;
    private final boolean allowsRowSplit;
    private final boolean allowsColumnSplit;

    public OperatorCapability(
        String operatorType,
        boolean rowLocal,
        boolean crossRow,
        boolean groupSensitive,
        boolean sortBarrier,
        boolean allowsRowSplit,
        boolean allowsColumnSplit
    ) {
        this.operatorType = operatorType;
        this.rowLocal = rowLocal;
        this.crossRow = crossRow;
        this.groupSensitive = groupSensitive;
        this.sortBarrier = sortBarrier;
        this.allowsRowSplit = allowsRowSplit;
        this.allowsColumnSplit = allowsColumnSplit;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public boolean isRowLocal() {
        return rowLocal;
    }

    public boolean isCrossRow() {
        return crossRow;
    }

    public boolean isGroupSensitive() {
        return groupSensitive;
    }

    public boolean isSortBarrier() {
        return sortBarrier;
    }

    public boolean isAllowsRowSplit() {
        return allowsRowSplit;
    }

    public boolean isAllowsColumnSplit() {
        return allowsColumnSplit;
    }
}
