package com.dataprocessor.flink.planner;

import java.io.Serializable;

// 中文说明：运行期 execution 只服务于本次 /run，不进持久化 DSL，因此单独抽成轻量配置对象。
public class ExecutionConfig implements Serializable {

    public static final String AUTO = "auto";
    public static final String SERIAL = "serial";
    public static final String ROWS = "rows";
    public static final String COLUMNS = "columns";

    private final String strategy;
    private final Integer maxWorkers;

    public ExecutionConfig(String strategy, Integer maxWorkers) {
        this.strategy = strategy;
        this.maxWorkers = maxWorkers;
    }

    public String getStrategy() {
        return strategy;
    }

    public Integer getMaxWorkers() {
        return maxWorkers;
    }
}
