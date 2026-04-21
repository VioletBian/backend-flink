package com.dataprocessor.flink.planner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
        "value_assign",
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
        if ("tag".equals(stepType)) {
            // 中文说明：tag 编辑态里 conditions / tags 可能被前端序列化成逗号分隔字符串，
            // 这里统一转成 canonical 字符串数组，避免 planner 因字段形态不一致误判为 Python fallback。
            params.put("conditions", normalizeStringListLike(params.get("conditions")));
            params.put("tags", normalizeStringListLike(params.get("tags")));
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
            // 中文说明：aggregate 在前端编辑态里经常把 by/on/rename 序列化成逗号分隔字符串，
            // 这里统一归一化成共享 DSL 约定的字符串数组，避免 planner 因字段形态不一致误判为 Python fallback。
            params.put("by", normalizeStringListLike(params.get("by")));
            // 中文说明：前端 aggregate method 可能带展示态大小写（如 `Sum`），这里统一收敛成小写 canonical，
            // 避免 planner/native executor 因大小写不一致误判为需要回退 Python。
            if (params.get("actions") instanceof Map<?, ?> actionMap) {
                LinkedHashMap<String, Object> canonicalActions = new LinkedHashMap<>((Map<String, Object>) actionMap);
                canonicalActions.put("on", normalizeStringListLike(canonicalActions.get("on")));
                canonicalActions.put("rename", normalizeStringListLike(canonicalActions.get("rename")));
                Object rawMethod = canonicalActions.get("method");
                if (rawMethod != null) {
                    canonicalActions.put("method", String.valueOf(rawMethod).trim().toLowerCase(Locale.ROOT));
                }
                params.put("actions", canonicalActions);
            }
        }

        if ("value_assign".equals(stepType)) {
            params.put("map", normalizeScalarLiteralMap(params.get("map")));
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        normalized.put("type", stepType);
        normalized.put("params", params);
        return normalized;
    }

    private List<String> normalizeStringListLike(Object rawValue) {
        if (rawValue instanceof List<?> rawList) {
            return rawList.stream()
                .map(String::valueOf)
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
        }
        if (rawValue instanceof String rawString) {
            String trimmed = rawString.trim();
            if (trimmed.isEmpty()) {
                return List.of();
            }
            return List.of(trimmed.split(",")).stream()
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
        }
        return List.of();
    }

    private Map<String, Object> normalizeScalarLiteralMap(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            return Map.of();
        }
        // 中文说明：前端 map 编辑器当前会把 value 先序列化成字符串，
        // 这里统一把 JSON 标量字面量还原成真实类型，避免 Java/Python 对同一 DSL 结果不一致。
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        rawMap.forEach((key, value) -> values.put(String.valueOf(key), normalizeScalarLiteral(value)));
        return values;
    }

    private Object normalizeScalarLiteral(Object rawValue) {
        if (!(rawValue instanceof String rawString)) {
            return rawValue;
        }
        String trimmed = rawString.trim();
        if (trimmed.isEmpty()) {
            return rawValue;
        }
        try {
            Object parsed = objectMapper.readValue(trimmed, Object.class);
            if (parsed instanceof Map<?, ?> || parsed instanceof List<?>) {
                return rawValue;
            }
            return parsed;
        } catch (JsonProcessingException exception) {
            return rawValue;
        }
    }
}
