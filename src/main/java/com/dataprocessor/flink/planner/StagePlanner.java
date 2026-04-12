package com.dataprocessor.flink.planner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.dataprocessor.flink.runtime.RuntimeTable;
import org.springframework.stereotype.Component;

@Component
public class StagePlanner {

    private static final String DEFAULT_CONDITION = "index > -1";
    private static final int DEFAULT_MAX_WORKERS = Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), 4));
    private static final int ROW_PARALLEL_THRESHOLD = 200000;
    private static final int COLUMN_PARALLEL_THRESHOLD = 32;
    private static final Set<String> SUPPORTED_EXECUTION_STRATEGIES = Set.of(
        ExecutionConfig.AUTO,
        ExecutionConfig.SERIAL,
        ExecutionConfig.ROWS,
        ExecutionConfig.COLUMNS
    );
    private static final Set<String> ROW_NATIVE_TYPES = Set.of("rename", "formatter", "date_formatter");
    private static final Set<String> COLUMN_NATIVE_TYPES = Set.of("formatter", "date_formatter");

    private final OperatorCapabilityRegistry capabilityRegistry;

    public StagePlanner(OperatorCapabilityRegistry capabilityRegistry) {
        this.capabilityRegistry = capabilityRegistry;
    }

    // 中文说明：prepared pipeline 是运行态 enrich 结果，execution 在这里补齐，但不会回写到持久化 DSL。
    public List<Map<String, Object>> materializeRunSteps(
        List<Map<String, Object>> normalizedPipeline,
        String defaultExecutionStrategy
    ) {
        normalizeDefaultExecutionStrategy(defaultExecutionStrategy);
        List<Map<String, Object>> preparedSteps = new ArrayList<>();
        for (int index = 0; index < normalizedPipeline.size(); index++) {
            preparedSteps.add(materializeInitialStep(normalizedPipeline.get(index), defaultExecutionStrategy));
        }
        return preparedSteps;
    }

    @SuppressWarnings("unchecked")
    public List<OperationSpec> parsePipelineSpecs(
        List<Map<String, Object>> preparedSteps,
        String defaultExecutionStrategy
    ) {
        normalizeDefaultExecutionStrategy(defaultExecutionStrategy);
        List<OperationSpec> specs = new ArrayList<>();

        for (int stepIndex = 0; stepIndex < preparedSteps.size(); stepIndex++) {
            Map<String, Object> rawStep = preparedSteps.get(stepIndex);
            String operatorType = String.valueOf(rawStep.get("type"));
            Map<String, Object> params = rawStep.get("params") instanceof Map<?, ?>
                ? new LinkedHashMap<>((Map<String, Object>) rawStep.get("params"))
                : new LinkedHashMap<>();
            ExecutionConfig execution = buildRuntimeExecutionConfig(rawStep, defaultExecutionStrategy, stepIndex, operatorType);

            if ("filter".equals(operatorType)) {
                String condition = normalizeCondition(params.get("condition"));
                if (params.get("map") instanceof Map<?, ?> renameMap) {
                    LinkedHashMap<String, Object> renameParams = new LinkedHashMap<>();
                    renameParams.put("map", new LinkedHashMap<>((Map<String, Object>) renameMap));
                    specs.add(new OperationSpec("rename", renameParams, DEFAULT_CONDITION, stepIndex, execution));

                    LinkedHashMap<String, Object> filterParams = new LinkedHashMap<>();
                    filterParams.put("requiredCols", renameMap.values().stream().map(String::valueOf).toList());
                    specs.add(new OperationSpec("filter", filterParams, condition, stepIndex, execution));
                } else {
                    LinkedHashMap<String, Object> filterParams = new LinkedHashMap<>();
                    filterParams.put("requiredCols", asStringList(params.get("requiredCols")));
                    specs.add(new OperationSpec("filter", filterParams, condition, stepIndex, execution));
                }
                continue;
            }

            if ("rename".equals(operatorType)) {
                LinkedHashMap<String, Object> renameParams = new LinkedHashMap<>();
                renameParams.put("map", asStringMap(params.get("map")));
                specs.add(new OperationSpec("rename", renameParams, DEFAULT_CONDITION, stepIndex, execution));
                continue;
            }

            if ("aggregate".equals(operatorType)) {
                LinkedHashMap<String, Object> aggregateParams = new LinkedHashMap<>();
                aggregateParams.put("by", asStringList(params.get("by")));
                aggregateParams.put("actions", params.get("actions") instanceof Map<?, ?> actions
                    ? new LinkedHashMap<>((Map<String, Object>) actions)
                    : Map.of());
                specs.add(new OperationSpec("aggregate", aggregateParams, DEFAULT_CONDITION, stepIndex, execution));
                continue;
            }

            if ("sort".equals(operatorType)) {
                LinkedHashMap<String, Object> sortParams = new LinkedHashMap<>();
                sortParams.put("by", asStringList(params.get("by")));
                sortParams.put("ascending", asBooleanList(params.get("ascending")));
                specs.add(new OperationSpec("sort", sortParams, DEFAULT_CONDITION, stepIndex, execution));
                continue;
            }

            if ("tag".equals(operatorType)) {
                LinkedHashMap<String, Object> tagParams = new LinkedHashMap<>();
                tagParams.put("conditions", asStringList(params.get("conditions")));
                tagParams.put("tag_col_name", String.valueOf(params.get("tag_col_name")));
                tagParams.put("tags", asStringList(params.get("tags")));
                tagParams.put("default_tag", params.get("default_tag"));
                specs.add(new OperationSpec("tag", tagParams, DEFAULT_CONDITION, stepIndex, execution));
                continue;
            }

            if ("col_assign".equals(operatorType)) {
                LinkedHashMap<String, Object> assignParams = new LinkedHashMap<>();
                assignParams.put("method", String.valueOf(params.get("method")));
                assignParams.put("col_name", String.valueOf(params.get("col_name")));
                assignParams.put("value_expr", params.get("value_expr"));
                specs.add(new OperationSpec("col_assign", assignParams, normalizeCondition(params.get("condition")), stepIndex, execution));
                continue;
            }

            if ("col_apply".equals(operatorType)) {
                LinkedHashMap<String, Object> applyParams = new LinkedHashMap<>();
                applyParams.put("on", asStringList(params.get("on")));
                applyParams.put("method", String.valueOf(params.get("method")));
                applyParams.put("value_expr", params.get("value_expr"));
                specs.add(new OperationSpec("col_apply", applyParams, normalizeCondition(params.get("condition")), stepIndex, execution));
                continue;
            }

            if ("series_transform".equals(operatorType)) {
                LinkedHashMap<String, Object> transformParams = new LinkedHashMap<>();
                transformParams.put("on", asStringList(params.get("on")));
                transformParams.put("transform_expr", String.valueOf(params.get("transform_expr")));
                transformParams.put("rename", params.get("rename"));
                specs.add(
                    new OperationSpec("series_transform", transformParams, normalizeCondition(params.get("condition")), stepIndex, execution)
                );
                continue;
            }

            if ("formatter".equals(operatorType) || "date_formatter".equals(operatorType)) {
                LinkedHashMap<String, Object> stageParams = new LinkedHashMap<>(params);
                stageParams.put("columns", asStringList(params.get("columns")));
                specs.add(new OperationSpec(operatorType, stageParams, DEFAULT_CONDITION, stepIndex, execution));
                continue;
            }

            throw new IllegalArgumentException("Unsupported operator `" + operatorType + "`.");
        }

        return specs;
    }

    public CandidateSegment collectCandidateSegment(List<OperationSpec> specs, int startIndex) {
        OperationSpec firstSpec = specs.get(startIndex);
        ExecutionProfile firstProfile = resolveExecutionProfile(firstSpec);
        if (firstProfile.isBarrier() || ExecutionConfig.SERIAL.equals(firstSpec.getExecution().getStrategy())) {
            return new CandidateSegment(startIndex + 1, List.of(firstSpec), Set.of(), null);
        }

        List<OperationSpec> currentSpecs = new ArrayList<>();
        currentSpecs.add(firstSpec);
        Set<String> commonStrategies = new LinkedHashSet<>(firstProfile.getSupportedStrategies());
        String forcedStrategy = forcedStrategy(firstSpec.getExecution().getStrategy());
        int nextIndex = startIndex + 1;

        while (nextIndex < specs.size()) {
            OperationSpec candidate = specs.get(nextIndex);
            ExecutionProfile candidateProfile = resolveExecutionProfile(candidate);
            String candidateForced = forcedStrategy(candidate.getExecution().getStrategy());

            if (candidateProfile.isBarrier() || ExecutionConfig.SERIAL.equals(candidate.getExecution().getStrategy())) {
                break;
            }

            Set<String> nextCommon = new LinkedHashSet<>(commonStrategies);
            nextCommon.retainAll(candidateProfile.getSupportedStrategies());
            if (nextCommon.isEmpty()) {
                break;
            }

            String mergedForced = forcedStrategy != null ? forcedStrategy : candidateForced;
            if (forcedStrategy != null && candidateForced != null && !forcedStrategy.equals(candidateForced)) {
                break;
            }
            if (mergedForced != null && !nextCommon.contains(mergedForced)) {
                break;
            }

            currentSpecs.add(candidate);
            commonStrategies = nextCommon;
            forcedStrategy = mergedForced;
            nextIndex += 1;
        }

        return new CandidateSegment(nextIndex, currentSpecs, commonStrategies, forcedStrategy);
    }

    // 中文说明：真正的段策略要基于当前中间表形状动态判断，不能只看原始输入表。
    public StagePlan chooseStage(
        RuntimeTable runtimeTable,
        CandidateSegment candidateSegment,
        int stageIndex
    ) {
        List<OperationSpec> candidateSpecs = candidateSegment.getSpecs();
        String strategy = resolveStageStrategy(runtimeTable, candidateSegment);
        int maxWorkers = ExecutionConfig.SERIAL.equals(strategy) ? 1 : resolveMaxWorkers(candidateSpecs);
        boolean nativeCapable = isNativeCapable(candidateSpecs, strategy);

        return new StagePlan(
            stageIndex,
            strategy,
            candidateSpecs.stream().map(OperationSpec::getSourceStepIndex).toList(),
            candidateSpecs.stream().map(OperationSpec::getType).toList(),
            candidateSpecs,
            maxWorkers,
            nativeCapable,
            nativeCapable ? null : resolveFallbackReason(candidateSpecs, strategy)
        );
    }

    public void applyStageExecution(List<Map<String, Object>> preparedSteps, StagePlan stagePlan) {
        Set<Integer> updatedStepIndexes = new LinkedHashSet<>();
        for (int stepIndex : stagePlan.getStepIndexes()) {
            if (!updatedStepIndexes.add(stepIndex)) {
                continue;
            }
            Map<String, Object> step = new LinkedHashMap<>(preparedSteps.get(stepIndex));
            LinkedHashMap<String, Object> execution = new LinkedHashMap<>();
            execution.put("strategy", stagePlan.getStrategy());
            if ((ExecutionConfig.ROWS.equals(stagePlan.getStrategy()) || ExecutionConfig.COLUMNS.equals(stagePlan.getStrategy()))
                && stagePlan.getMaxWorkers() > 0) {
                execution.put("max_workers", stagePlan.getMaxWorkers());
            }
            step.put("execution", execution);
            preparedSteps.set(stepIndex, step);
        }
    }

    public PipelinePlan buildPlan(
        List<Map<String, Object>> normalizedPipeline,
        List<Map<String, Object>> preparedPipeline,
        List<OperationSpec> specs,
        List<StagePlan> stages
    ) {
        List<PlannedOperator> operators = new ArrayList<>();
        int specIndex = 0;
        for (StagePlan stage : stages) {
            for (OperationSpec spec : stage.getSpecs()) {
                operators.add(
                    new PlannedOperator(
                        specIndex++,
                        spec.getSourceStepIndex(),
                        spec.getType(),
                        spec.getExecution().getStrategy(),
                        stage.getStrategy(),
                        stage.isNativeCapable(),
                        stage.getFallbackReason(),
                        capabilityRegistry.get(spec.getType()).getSupportedStrategies().stream().sorted().toList()
                    )
                );
            }
        }
        return new PipelinePlan(normalizedPipeline, preparedPipeline, operators, stages);
    }

    private Map<String, Object> materializeInitialStep(Map<String, Object> rawStep, String defaultExecutionStrategy) {
        Map<String, Object> materialized = new LinkedHashMap<>();
        materialized.put("type", rawStep.get("type"));
        materialized.put("params", rawStep.get("params") instanceof Map<?, ?> params
            ? new LinkedHashMap<>((Map<String, Object>) params)
            : new LinkedHashMap<>());

        Map<String, Object> execution = rawStep.get("execution") instanceof Map<?, ?> runtimeExecution
            ? new LinkedHashMap<>((Map<String, Object>) runtimeExecution)
            : new LinkedHashMap<>();
        execution.putIfAbsent("strategy", defaultExecutionStrategy);
        materialized.put("execution", execution);
        return materialized;
    }

    private ExecutionProfile resolveExecutionProfile(OperationSpec spec) {
        OperatorCapability capability = capabilityRegistry.get(spec.getType());
        return new ExecutionProfile(
            capability.getSupportedStrategies(),
            capability.isBarrier(),
            resolveColumnComponentCount(spec)
        );
    }

    private String resolveStageStrategy(RuntimeTable runtimeTable, CandidateSegment candidateSegment) {
        List<OperationSpec> candidateSpecs = candidateSegment.getSpecs();
        if (candidateSpecs.size() == 1) {
            ExecutionProfile profile = resolveExecutionProfile(candidateSpecs.get(0));
            if (profile.isBarrier() || ExecutionConfig.SERIAL.equals(candidateSpecs.get(0).getExecution().getStrategy())) {
                return ExecutionConfig.SERIAL;
            }
        }

        if (ExecutionConfig.COLUMNS.equals(candidateSegment.getForcedStrategy())) {
            return ExecutionConfig.COLUMNS;
        }
        if (ExecutionConfig.ROWS.equals(candidateSegment.getForcedStrategy())) {
            return ExecutionConfig.ROWS;
        }
        if (candidateSegment.getCommonStrategies().contains(ExecutionConfig.COLUMNS)
            && resolveColumnComponentCount(candidateSpecs) >= COLUMN_PARALLEL_THRESHOLD) {
            return ExecutionConfig.COLUMNS;
        }
        if (candidateSegment.getCommonStrategies().contains(ExecutionConfig.ROWS)
            && runtimeTable.getRowCount() >= ROW_PARALLEL_THRESHOLD) {
            return ExecutionConfig.ROWS;
        }
        return ExecutionConfig.SERIAL;
    }

    private boolean isNativeCapable(List<OperationSpec> specs, String resolvedStrategy) {
        Set<String> allowedTypes = ExecutionConfig.COLUMNS.equals(resolvedStrategy) ? COLUMN_NATIVE_TYPES : ROW_NATIVE_TYPES;
        for (OperationSpec spec : specs) {
            if (!allowedTypes.contains(spec.getType())) {
                return false;
            }
        }
        return true;
    }

    private String resolveFallbackReason(List<OperationSpec> specs, String resolvedStrategy) {
        if (specs.stream().anyMatch(spec -> Set.of("aggregate", "sort", "col_apply").contains(spec.getType()))) {
            return "barrier-operator";
        }
        if (ExecutionConfig.COLUMNS.equals(resolvedStrategy)) {
            return "operator-not-column-native";
        }
        return "operator-requires-python-fallback";
    }

    private int resolveColumnComponentCount(List<OperationSpec> specs) {
        LinkedHashSet<String> touchedColumns = new LinkedHashSet<>();
        for (OperationSpec spec : specs) {
            touchedColumns.addAll(resolveTouchedColumns(spec));
        }
        return touchedColumns.size();
    }

    private int resolveColumnComponentCount(OperationSpec spec) {
        return resolveTouchedColumns(spec).size();
    }

    private List<String> resolveTouchedColumns(OperationSpec spec) {
        if ("formatter".equals(spec.getType()) || "date_formatter".equals(spec.getType())) {
            return asStringList(spec.getParams().get("columns"));
        }
        if ("series_transform".equals(spec.getType())) {
            Object rawRename = spec.getParams().get("rename");
            if (rawRename instanceof List<?> rename) {
                return rename.stream().map(String::valueOf).toList();
            }
            return asStringList(spec.getParams().get("on"));
        }
        return List.of();
    }

    private int resolveMaxWorkers(List<OperationSpec> specs) {
        return specs.stream()
            .map(spec -> Optional.ofNullable(spec.getExecution().getMaxWorkers()).orElse(DEFAULT_MAX_WORKERS))
            .min(Comparator.naturalOrder())
            .orElse(DEFAULT_MAX_WORKERS);
    }

    @SuppressWarnings("unchecked")
    private ExecutionConfig buildRuntimeExecutionConfig(
        Map<String, Object> rawStep,
        String defaultExecutionStrategy,
        int stepIndex,
        String operatorType
    ) {
        Map<String, Object> rawExecution = rawStep.get("execution") instanceof Map<?, ?> execution
            ? new LinkedHashMap<>((Map<String, Object>) execution)
            : new LinkedHashMap<>();
        rawExecution.putIfAbsent("strategy", defaultExecutionStrategy);

        String strategy = String.valueOf(rawExecution.get("strategy"));
        if (!SUPPORTED_EXECUTION_STRATEGIES.contains(strategy)) {
            throw new IllegalArgumentException(
                "Step " + stepIndex + " (" + operatorType + ") has unsupported `execution.strategy`: " + strategy + "."
            );
        }

        Integer maxWorkers = null;
        Object rawMaxWorkers = rawExecution.get("max_workers");
        if (rawMaxWorkers != null) {
            try {
                maxWorkers = Integer.parseInt(String.valueOf(rawMaxWorkers));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(
                    "Step " + stepIndex + " (" + operatorType + ") has invalid `execution.max_workers`.",
                    exception
                );
            }
            if (maxWorkers <= 0) {
                throw new IllegalArgumentException(
                    "Step " + stepIndex + " (" + operatorType + ") has invalid `execution.max_workers`."
                );
            }
        }

        if ((ExecutionConfig.ROWS.equals(strategy) || ExecutionConfig.COLUMNS.equals(strategy))
            && !capabilityRegistry.get(operatorType).getSupportedStrategies().contains(strategy)) {
            throw new IllegalArgumentException(
                "Step " + stepIndex + " (" + operatorType + ") does not support `" + strategy + "` execution strategy."
            );
        }

        return new ExecutionConfig(strategy, maxWorkers);
    }

    private String normalizeCondition(Object rawCondition) {
        if (rawCondition == null || String.valueOf(rawCondition).isBlank()) {
            return DEFAULT_CONDITION;
        }
        return String.valueOf(rawCondition);
    }

    private void normalizeDefaultExecutionStrategy(String defaultExecutionStrategy) {
        if (!SUPPORTED_EXECUTION_STRATEGIES.contains(defaultExecutionStrategy)) {
            throw new IllegalArgumentException("Unsupported default execution strategy: " + defaultExecutionStrategy + ".");
        }
    }

    private String forcedStrategy(String strategy) {
        if (ExecutionConfig.ROWS.equals(strategy) || ExecutionConfig.COLUMNS.equals(strategy)) {
            return strategy;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> asStringMap(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            return Map.of();
        }
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        rawMap.forEach((key, value) -> values.put(String.valueOf(key), String.valueOf(value)));
        return values;
    }

    private List<String> asStringList(Object rawValue) {
        if (!(rawValue instanceof List<?> rawList)) {
            return List.of();
        }
        return rawList.stream().map(String::valueOf).toList();
    }

    private List<Boolean> asBooleanList(Object rawValue) {
        if (!(rawValue instanceof List<?> rawList)) {
            return List.of();
        }
        return rawList.stream().map(value -> Boolean.valueOf(String.valueOf(value))).toList();
    }

    public static class CandidateSegment {
        private final int nextIndex;
        private final List<OperationSpec> specs;
        private final Set<String> commonStrategies;
        private final String forcedStrategy;

        public CandidateSegment(int nextIndex, List<OperationSpec> specs, Set<String> commonStrategies, String forcedStrategy) {
            this.nextIndex = nextIndex;
            this.specs = new ArrayList<>(specs);
            this.commonStrategies = new LinkedHashSet<>(commonStrategies);
            this.forcedStrategy = forcedStrategy;
        }

        public int getNextIndex() {
            return nextIndex;
        }

        public List<OperationSpec> getSpecs() {
            return new ArrayList<>(specs);
        }

        public Set<String> getCommonStrategies() {
            return new LinkedHashSet<>(commonStrategies);
        }

        public String getForcedStrategy() {
            return forcedStrategy;
        }
    }
}
