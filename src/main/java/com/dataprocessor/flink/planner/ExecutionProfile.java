package com.dataprocessor.flink.planner;

import java.util.LinkedHashSet;
import java.util.Set;

// 中文说明：ExecutionProfile 只保留 planner 需要的执行画像，避免在多个类里重复推导能力矩阵。
public class ExecutionProfile {

    private final Set<String> supportedStrategies;
    private final boolean barrier;
    private final int columnComponentCount;

    public ExecutionProfile(Set<String> supportedStrategies, boolean barrier, int columnComponentCount) {
        this.supportedStrategies = new LinkedHashSet<>(supportedStrategies);
        this.barrier = barrier;
        this.columnComponentCount = columnComponentCount;
    }

    public Set<String> getSupportedStrategies() {
        return new LinkedHashSet<>(supportedStrategies);
    }

    public boolean isBarrier() {
        return barrier;
    }

    public int getColumnComponentCount() {
        return columnComponentCount;
    }
}
