package com.dataprocessor.flink.planner;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

// 中文说明：把逻辑 step 展开成可执行 spec，后续 planner 与 executor 都围绕这个稳定单元工作。
public class OperationSpec implements Serializable {

    private final String type;
    private final Map<String, Object> params;
    private final String condition;
    private final int sourceStepIndex;
    private final ExecutionConfig execution;

    public OperationSpec(
        String type,
        Map<String, Object> params,
        String condition,
        int sourceStepIndex,
        ExecutionConfig execution
    ) {
        this.type = type;
        this.params = new LinkedHashMap<>(params);
        this.condition = condition;
        this.sourceStepIndex = sourceStepIndex;
        this.execution = execution;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getParams() {
        return new LinkedHashMap<>(params);
    }

    public String getCondition() {
        return condition;
    }

    public int getSourceStepIndex() {
        return sourceStepIndex;
    }

    public ExecutionConfig getExecution() {
        return execution;
    }
}
