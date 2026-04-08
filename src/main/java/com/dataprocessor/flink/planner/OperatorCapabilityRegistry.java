package com.dataprocessor.flink.planner;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class OperatorCapabilityRegistry {

    private final Map<String, OperatorCapability> capabilities = new LinkedHashMap<>();

    public OperatorCapabilityRegistry() {
        capabilities.put("filter", new OperatorCapability("filter", true, false, false, false, true, true));
        capabilities.put("rename", new OperatorCapability("rename", true, false, false, false, true, true));
        capabilities.put("aggregate", new OperatorCapability("aggregate", false, false, true, false, false, false));
        capabilities.put("sort", new OperatorCapability("sort", false, false, false, true, false, false));
        capabilities.put("tag", new OperatorCapability("tag", true, false, false, false, true, true));
        capabilities.put("col_assign", new OperatorCapability("col_assign", true, false, false, false, true, false));
        capabilities.put("col_apply", new OperatorCapability("col_apply", false, true, false, false, false, false));
        capabilities.put("series_transform", new OperatorCapability("series_transform", false, true, false, false, false, false));
        capabilities.put("formatter", new OperatorCapability("formatter", true, false, false, false, true, true));
        capabilities.put("date_formatter", new OperatorCapability("date_formatter", true, false, false, false, true, true));
    }

    public OperatorCapability get(String operatorType) {
        OperatorCapability capability = capabilities.get(operatorType);
        if (capability == null) {
            throw new IllegalArgumentException("Unsupported operator `" + operatorType + "`.");
        }
        return capability;
    }
}
