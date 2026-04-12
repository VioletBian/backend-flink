package com.dataprocessor.flink.planner;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

@Component
public class OperatorCapabilityRegistry {

    private final Map<String, OperatorCapability> capabilities = new LinkedHashMap<>();

    public OperatorCapabilityRegistry() {
        // 中文说明：这里直接对齐 Python planner 当前的轴支持矩阵，而不是沿用早期 Java 骨架里的宽松假设。
        capabilities.put("filter", new OperatorCapability("filter", Set.of(ExecutionConfig.ROWS), false));
        capabilities.put("rename", new OperatorCapability("rename", Set.of(ExecutionConfig.ROWS), false));
        capabilities.put("aggregate", new OperatorCapability("aggregate", Set.of(), true));
        capabilities.put("sort", new OperatorCapability("sort", Set.of(), true));
        capabilities.put("tag", new OperatorCapability("tag", Set.of(ExecutionConfig.ROWS), false));
        capabilities.put("col_assign", new OperatorCapability("col_assign", Set.of(ExecutionConfig.ROWS), false));
        capabilities.put("col_apply", new OperatorCapability("col_apply", Set.of(), true));
        capabilities.put("series_transform", new OperatorCapability("series_transform", Set.of(ExecutionConfig.COLUMNS), false));
        capabilities.put(
            "formatter",
            new OperatorCapability("formatter", Set.of(ExecutionConfig.ROWS, ExecutionConfig.COLUMNS), false)
        );
        capabilities.put(
            "date_formatter",
            new OperatorCapability("date_formatter", Set.of(ExecutionConfig.ROWS, ExecutionConfig.COLUMNS), false)
        );
    }

    public OperatorCapability get(String operatorType) {
        OperatorCapability capability = capabilities.get(operatorType);
        if (capability == null) {
            throw new IllegalArgumentException("Unsupported operator `" + operatorType + "`.");
        }
        return capability;
    }
}
