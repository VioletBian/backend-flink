package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

@Component
public class StagePlanner {

    private final OperatorCapabilityRegistry capabilityRegistry;

    public StagePlanner(OperatorCapabilityRegistry capabilityRegistry) {
        this.capabilityRegistry = capabilityRegistry;
    }

    @SuppressWarnings("unchecked")
    public PipelinePlan plan(List<Map<String, Object>> normalizedPipeline) {
        List<PlannedOperator> plannedOperators = new ArrayList<>();

        for (int index = 0; index < normalizedPipeline.size(); index++) {
            Map<String, Object> step = normalizedPipeline.get(index);
            String operatorType = String.valueOf(step.get("type"));
            Map<String, Object> params = (Map<String, Object>) step.getOrDefault("params", Map.of());
            OperatorCapability capability = capabilityRegistry.get(operatorType);
            String executionMode = resolveExecutionMode(operatorType, params);
            boolean barrier = capability.isSortBarrier()
                || capability.isCrossRow()
                || capability.isGroupSensitive()
                || "PYTHON_FALLBACK".equals(executionMode);
            boolean rowSplitEligible = "NATIVE".equals(executionMode) && capability.isAllowsRowSplit() && !barrier;
            boolean columnSplitEligible = "NATIVE".equals(executionMode) && capability.isAllowsColumnSplit() && !barrier;

            plannedOperators.add(
                new PlannedOperator(
                    index,
                    operatorType,
                    executionMode,
                    extractReadColumns(operatorType, params),
                    extractWriteColumns(operatorType, params),
                    rowSplitEligible,
                    columnSplitEligible,
                    barrier,
                    barrier ? resolveBarrierReason(operatorType, executionMode, capability) : null
                )
            );
        }

        List<StagePlan> stages = buildStages(plannedOperators);
        int nativeCandidateCount = (int) plannedOperators.stream()
            .filter(operator -> "NATIVE".equals(operator.getExecutionMode()))
            .count();

        return new PipelinePlan(
            normalizedPipeline,
            plannedOperators,
            stages,
            nativeCandidateCount,
            plannedOperators.size() - nativeCandidateCount
        );
    }

    private String resolveExecutionMode(String operatorType, Map<String, Object> params) {
        if ("col_assign".equals(operatorType)) {
            return "vectorized".equals(String.valueOf(params.getOrDefault("method", "")))
                ? "NATIVE"
                : "PYTHON_FALLBACK";
        }

        if ("col_apply".equals(operatorType)) {
            return "PYTHON_FALLBACK";
        }

        if ("series_transform".equals(operatorType)) {
            String expression = String.valueOf(params.getOrDefault("transform_expr", ""));
            return expression.contains("shift(") || expression.contains("diff(") || expression.contains("rolling(")
                ? "NATIVE"
                : "PYTHON_FALLBACK";
        }

        return "NATIVE";
    }

    private String resolveBarrierReason(String operatorType, String executionMode, OperatorCapability capability) {
        if ("PYTHON_FALLBACK".equals(executionMode)) {
            return "python-reference-barrier";
        }
        if (capability.isGroupSensitive()) {
            return "group-sensitive-barrier";
        }
        if (capability.isSortBarrier()) {
            return "stable-sort-barrier";
        }
        if (capability.isCrossRow()) {
            return "cross-row-barrier";
        }
        return "execution-barrier";
    }

    private List<StagePlan> buildStages(List<PlannedOperator> plannedOperators) {
        List<StagePlan> stages = new ArrayList<>();
        List<PlannedOperator> currentBlock = new ArrayList<>();
        int stageIndex = 0;

        for (PlannedOperator operator : plannedOperators) {
            if (operator.isBarrier()) {
                if (!currentBlock.isEmpty()) {
                    stages.add(createStage(stageIndex++, currentBlock));
                    currentBlock = new ArrayList<>();
                }
                stages.add(createStage(stageIndex++, List.of(operator)));
                continue;
            }

            if (!currentBlock.isEmpty()
                && !currentBlock.get(0).getExecutionMode().equals(operator.getExecutionMode())) {
                stages.add(createStage(stageIndex++, currentBlock));
                currentBlock = new ArrayList<>();
            }

            currentBlock.add(operator);
        }

        if (!currentBlock.isEmpty()) {
            stages.add(createStage(stageIndex, currentBlock));
        }

        return stages;
    }

    private StagePlan createStage(int stageIndex, List<PlannedOperator> stageOperators) {
        List<Integer> operatorIndexes = stageOperators.stream().map(PlannedOperator::getOperatorIndex).toList();
        List<String> operatorTypes = stageOperators.stream().map(PlannedOperator::getOperatorType).toList();
        String executionMode = stageOperators.get(0).getExecutionMode();
        boolean rowSplitEligible = stageOperators.stream().allMatch(PlannedOperator::isRowSplitEligible);
        boolean columnSplitEligible = stageOperators.stream().allMatch(PlannedOperator::isColumnSplitEligible);

        String optimizationHint = resolveOptimizationHint(stageOperators, rowSplitEligible, columnSplitEligible);
        return new StagePlan(
            stageIndex,
            executionMode,
            operatorIndexes,
            operatorTypes,
            rowSplitEligible,
            columnSplitEligible,
            optimizationHint
        );
    }

    private String resolveOptimizationHint(
        List<PlannedOperator> stageOperators,
        boolean rowSplitEligible,
        boolean columnSplitEligible
    ) {
        if (stageOperators.size() == 1) {
            String operatorType = stageOperators.get(0).getOperatorType();
            if ("aggregate".equals(operatorType)) {
                return "two-phase-aggregate";
            }
            if ("sort".equals(operatorType)) {
                return "stable-sort-barrier";
            }
            if ("series_transform".equals(operatorType)) {
                return "ordered-series-transform";
            }
            if ("PYTHON_FALLBACK".equals(stageOperators.get(0).getExecutionMode())) {
                return "python-reference-barrier";
            }
        }

        if (rowSplitEligible && columnSplitEligible) {
            return "row-and-column-splittable";
        }
        if (rowSplitEligible) {
            return "row-splittable";
        }
        if (columnSplitEligible) {
            return "column-splittable";
        }
        return "serial-native";
    }

    @SuppressWarnings("unchecked")
    private List<String> extractReadColumns(String operatorType, Map<String, Object> params) {
        Set<String> readColumns = new LinkedHashSet<>();

        switch (operatorType) {
            case "filter" -> {
                readColumns.addAll(asStringList(params.get("requiredCols")));
                if (params.get("map") instanceof Map<?, ?> renameMap) {
                    readColumns.addAll(renameMap.keySet().stream().map(String::valueOf).toList());
                    readColumns.addAll(renameMap.values().stream().map(String::valueOf).toList());
                }
            }
            case "rename" -> {
                if (params.get("map") instanceof Map<?, ?> renameMap) {
                    readColumns.addAll(renameMap.keySet().stream().map(String::valueOf).toList());
                }
            }
            case "aggregate" -> {
                readColumns.addAll(asStringList(params.get("by")));
                if (params.get("actions") instanceof Map<?, ?> actions) {
                    readColumns.addAll(asStringList(actions.get("on")));
                }
            }
            case "sort" -> readColumns.addAll(asStringList(params.get("by")));
            case "tag" -> readColumns.addAll(asStringList(params.get("on")));
            case "col_assign" -> readColumns.addAll(asStringList(params.get("on")));
            case "col_apply" -> readColumns.addAll(asStringList(params.get("on")));
            case "series_transform" -> readColumns.addAll(asStringList(params.get("on")));
            case "formatter", "date_formatter" -> readColumns.addAll(asStringList(params.get("columns")));
            default -> {
            }
        }

        return new ArrayList<>(readColumns);
    }

    @SuppressWarnings("unchecked")
    private List<String> extractWriteColumns(String operatorType, Map<String, Object> params) {
        Set<String> writeColumns = new LinkedHashSet<>();

        switch (operatorType) {
            case "rename" -> {
                if (params.get("map") instanceof Map<?, ?> renameMap) {
                    writeColumns.addAll(renameMap.values().stream().map(String::valueOf).toList());
                }
            }
            case "aggregate" -> {
                writeColumns.addAll(asStringList(params.get("by")));
                if (params.get("actions") instanceof Map<?, ?> actions) {
                    List<String> rename = asStringList(actions.get("rename"));
                    if (!rename.isEmpty()) {
                        writeColumns.addAll(rename);
                    } else {
                        writeColumns.addAll(asStringList(actions.get("on")));
                    }
                }
            }
            case "tag" -> writeColumns.add(String.valueOf(params.getOrDefault("tag_col_name", "")));
            case "col_assign" -> writeColumns.add(String.valueOf(params.getOrDefault("col_name", "")));
            case "col_apply" -> {
                List<String> mapped = asStringList(params.get("value_expr"));
                if (!mapped.isEmpty()) {
                    writeColumns.addAll(mapped);
                } else {
                    writeColumns.addAll(asStringList(params.get("on")));
                }
            }
            case "series_transform" -> {
                List<String> rename = asStringList(params.get("rename"));
                if (!rename.isEmpty()) {
                    writeColumns.addAll(rename);
                } else {
                    writeColumns.addAll(asStringList(params.get("on")));
                }
            }
            case "formatter", "date_formatter" -> writeColumns.addAll(asStringList(params.get("columns")));
            default -> {
            }
        }

        writeColumns.remove("");
        return new ArrayList<>(writeColumns);
    }

    @SuppressWarnings("unchecked")
    private List<String> asStringList(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> rawList) {
            return rawList.stream().map(String::valueOf).toList();
        }
        if (value instanceof Collection<?> rawCollection) {
            return rawCollection.stream().map(String::valueOf).toList();
        }
        if (value instanceof Map<?, ?> rawMap) {
            return rawMap.values().stream().map(String::valueOf).toList();
        }
        String rawValue = String.valueOf(value).trim();
        if (rawValue.isEmpty()) {
            return List.of();
        }
        return List.of(rawValue.split("\\s*,\\s*"));
    }
}
