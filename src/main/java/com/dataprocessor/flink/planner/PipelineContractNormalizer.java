package com.dataprocessor.flink.planner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class PipelineContractNormalizer {

    private static final Set<String> SUPPORTED_OPERATOR_TYPES = Set.of(
        "filter",
        "rename",
        "aggregate",
        "sort",
        "tag",
        "constant",
        "value_mapping",
        "col_assign",
        "col_apply",
        "series_transform",
        "formatter",
        "date_formatter"
    );

    private final ObjectMapper objectMapper;

    public PipelineContractNormalizer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<Map<String, Object>> normalizeJson(String pipelineJson) {
        try {
            Object decoded = objectMapper.readValue(pipelineJson, Object.class);
            return normalizePayload(decoded);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid pipeline JSON: " + exception.getOriginalMessage(), exception);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> normalizePayload(Object payload) {
        Object decoded = payload;
        if (decoded instanceof Map<?, ?> mapPayload && mapPayload.containsKey("pipeline")) {
            decoded = mapPayload.get("pipeline");
        }

        if (!(decoded instanceof List<?> rawSteps)) {
            throw new IllegalArgumentException(
                "Pipeline payload must be a JSON array or an object containing `pipeline`."
            );
        }

        return rawSteps.stream().map(this::normalizeStep).toList();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeStep(Object rawStep) {
        if (!(rawStep instanceof Map<?, ?> rawMap)) {
            throw new IllegalArgumentException("Each pipeline step must be an object.");
        }

        Map<String, Object> step = new LinkedHashMap<>((Map<String, Object>) rawMap);
        Object typeObject = step.get("type");
        if (!(typeObject instanceof String stepType) || !SUPPORTED_OPERATOR_TYPES.contains(stepType)) {
            throw new IllegalArgumentException(
                "Unsupported operator `" + typeObject + "`. "
                    + "First-stage parity only covers operators implemented in backend/operations.py."
            );
        }

        Object rawParams = step.get("params");
        Map<String, Object> params = rawParams instanceof Map<?, ?>
            ? new LinkedHashMap<>((Map<String, Object>) rawParams)
            : new LinkedHashMap<>();

        // 兼容旧字段方言，统一在 Java 侧存储 / 执行前转成 canonical DSL。
        if ("filter".equals(stepType) && !params.containsKey("requiredCols") && params.containsKey("by")) {
            params.put("requiredCols", params.remove("by"));
        }

        if ("tag".equals(stepType) && !params.containsKey("tag_col_name") && params.containsKey("col_name")) {
            params.put("tag_col_name", params.remove("col_name"));
        }

        if ("aggregate".equals(stepType)) {
            Object actions = params.get("actions");
            if (actions instanceof List<?> actionList) {
                if (actionList.size() != 1) {
                    throw new IllegalArgumentException(
                        "Aggregate `actions` must be a single object in the shared DSL contract."
                    );
                }
                params.put("actions", actionList.get(0));
            }
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        normalized.put("type", stepType);
        normalized.put("params", params);
        return normalized;
    }
}
