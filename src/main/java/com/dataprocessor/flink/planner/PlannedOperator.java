package com.dataprocessor.flink.planner;

import java.util.List;

public class PlannedOperator {

    private final int operatorIndex;
    private final String operatorType;
    private final String executionMode;
    private final List<String> readColumns;
    private final List<String> writeColumns;
    private final boolean rowSplitEligible;
    private final boolean columnSplitEligible;
    private final boolean barrier;
    private final String barrierReason;

    public PlannedOperator(
        int operatorIndex,
        String operatorType,
        String executionMode,
        List<String> readColumns,
        List<String> writeColumns,
        boolean rowSplitEligible,
        boolean columnSplitEligible,
        boolean barrier,
        String barrierReason
    ) {
        this.operatorIndex = operatorIndex;
        this.operatorType = operatorType;
        this.executionMode = executionMode;
        this.readColumns = readColumns;
        this.writeColumns = writeColumns;
        this.rowSplitEligible = rowSplitEligible;
        this.columnSplitEligible = columnSplitEligible;
        this.barrier = barrier;
        this.barrierReason = barrierReason;
    }

    public int getOperatorIndex() {
        return operatorIndex;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public String getExecutionMode() {
        return executionMode;
    }

    public List<String> getReadColumns() {
        return readColumns;
    }

    public List<String> getWriteColumns() {
        return writeColumns;
    }

    public boolean isRowSplitEligible() {
        return rowSplitEligible;
    }

    public boolean isColumnSplitEligible() {
        return columnSplitEligible;
    }

    public boolean isBarrier() {
        return barrier;
    }

    public String getBarrierReason() {
        return barrierReason;
    }
}
