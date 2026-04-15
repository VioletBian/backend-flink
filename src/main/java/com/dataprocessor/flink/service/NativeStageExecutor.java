package com.dataprocessor.flink.service;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dataprocessor.flink.planner.ExecutionConfig;
import com.dataprocessor.flink.planner.OperationSpec;
import com.dataprocessor.flink.planner.StagePlan;
import com.dataprocessor.flink.runtime.RowExpressionEvaluator;
import com.dataprocessor.flink.runtime.RuntimeRow;
import com.dataprocessor.flink.runtime.RuntimeTable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

// 中文说明：这里把“已经能静态证明语义安全”的算子尽量收进 Java native；
// 仍依赖 Python callable / eval 语义的部分继续回退 Python bridge。
@Component
public class NativeStageExecutor {

    private static final Map<String, String> DATE_FORMATS = Map.of(
        "yyyyMMdd", "yyyyMMdd",
        "yyyy-MM-dd", "yyyy-MM-dd",
        "yyyy/MM/dd", "yyyy/MM/dd",
        "yyyyMMddHHmmss", "yyyyMMddHHmmss",
        "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss",
        "dd/MM/yyyy", "dd/MM/yyyy",
        "MM/dd/yyyy", "MM/dd/yyyy"
    );
    private static final Set<String> COLUMN_NATIVE_TYPES = Set.of("constant", "value_mapping", "formatter", "date_formatter");
    private final BatchTableEnvironmentFactory batchTableEnvironmentFactory;
    private final RowExpressionEvaluator rowExpressionEvaluator;

    public NativeStageExecutor(
        BatchTableEnvironmentFactory batchTableEnvironmentFactory,
        RowExpressionEvaluator rowExpressionEvaluator
    ) {
        this.batchTableEnvironmentFactory = batchTableEnvironmentFactory;
        this.rowExpressionEvaluator = rowExpressionEvaluator;
    }

    public boolean supports(StagePlan stagePlan) {
        return stagePlan.isNativeCapable();
    }

    public RuntimeTable execute(RuntimeTable inputTable, StagePlan stagePlan) {
        if (ExecutionConfig.COLUMNS.equals(stagePlan.getStrategy())) {
            return executeColumnStage(inputTable, stagePlan);
        }
        if (ExecutionConfig.ROWS.equals(stagePlan.getStrategy())) {
            return executeRowStage(inputTable, stagePlan);
        }
        return executeSerialStage(inputTable, stagePlan.getSpecs());
    }

    // 中文说明：行式 stage 只放 row-local 算子，真正交给 Flink batch DataStream 并行跑。
    private RuntimeTable executeRowStage(RuntimeTable inputTable, StagePlan stagePlan) {
        if (stagePlan.getMaxWorkers() <= 1 || inputTable.getRowCount() < 2) {
            return executeSerialStage(inputTable, stagePlan.getSpecs());
        }

        List<String> currentColumns = new ArrayList<>(inputTable.getColumns());
        StreamExecutionEnvironment env = batchTableEnvironmentFactory.createBatchStreamExecutionEnvironment(stagePlan.getMaxWorkers());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<RuntimeRow> stream = env.fromCollection(inputTable.getRows()).setParallelism(stagePlan.getMaxWorkers());
        for (OperationSpec spec : stagePlan.getSpecs()) {
            try {
                validateRequiredInputColumns(currentColumns, spec);
                if ("filter".equals(spec.getType())) {
                    List<String> availableColumns = new ArrayList<>(currentColumns);
                    RowExpressionEvaluator.CompiledBooleanExpression compiledExpression =
                        rowExpressionEvaluator.compileBooleanExpression(spec.getCondition(), availableColumns);
                    List<String> projectedColumns = projectedColumns(currentColumns, spec);
                    stream = stream.filter(row -> rowExpressionEvaluator.evaluateBoolean(row, compiledExpression))
                        .setParallelism(stagePlan.getMaxWorkers());
                    if (!projectedColumns.isEmpty()) {
                        stream = stream.map(row -> projectRow(row, projectedColumns)).setParallelism(stagePlan.getMaxWorkers());
                        currentColumns = projectedColumns;
                    }
                    continue;
                }
                if ("rename".equals(spec.getType())) {
                    List<String> beforeColumns = new ArrayList<>(currentColumns);
                    Map<String, String> renameMap = asStringMap(spec.getParams().get("map"));
                    stream = stream.map(row -> renameRow(row, beforeColumns, renameMap)).setParallelism(stagePlan.getMaxWorkers());
                    currentColumns = renameColumns(currentColumns, renameMap);
                    continue;
                }
                if ("tag".equals(spec.getType())) {
                    List<String> availableColumns = new ArrayList<>(currentColumns);
                    List<RowExpressionEvaluator.CompiledBooleanExpression> compiledConditions = compileConditions(
                        asStringList(spec.getParams().get("conditions")),
                        availableColumns
                    );
                    List<String> tags = asStringList(spec.getParams().get("tags"));
                    String tagColumn = String.valueOf(spec.getParams().get("tag_col_name"));
                    Object defaultTag = spec.getParams().get("default_tag");
                    stream = stream.map(
                        row -> applyTag(row, tagColumn, compiledConditions, tags, defaultTag)
                    ).setParallelism(stagePlan.getMaxWorkers());
                    currentColumns = appendColumn(currentColumns, tagColumn);
                    continue;
                }
                if ("constant".equals(spec.getType())) {
                    stream = stream.map(row -> applyConstant(row, spec)).setParallelism(stagePlan.getMaxWorkers());
                    currentColumns = appendColumns(currentColumns, asStringList(spec.getParams().get("columns")));
                    continue;
                }
                if ("value_mapping".equals(spec.getType())) {
                    stream = stream.map(row -> applyValueMapping(row, spec)).setParallelism(stagePlan.getMaxWorkers());
                    if (!isReplaceMode(spec.getParams().get("mode"))) {
                        currentColumns = appendColumns(currentColumns, asStringList(spec.getParams().get("result_columns")));
                    }
                    continue;
                }
                if ("col_assign".equals(spec.getType())) {
                    List<String> availableColumns = new ArrayList<>(currentColumns);
                    RowExpressionEvaluator.CompiledBooleanExpression compiledCondition =
                        rowExpressionEvaluator.compileBooleanExpression(spec.getCondition(), availableColumns);
                    RowExpressionEvaluator.CompiledValueExpression compiledValue =
                        rowExpressionEvaluator.compileValueExpression(String.valueOf(spec.getParams().get("value_expr")), availableColumns);
                    String columnName = String.valueOf(spec.getParams().get("col_name"));
                    stream = stream.map(
                        row -> applyVectorizedColAssign(row, columnName, compiledCondition, compiledValue)
                    ).setParallelism(stagePlan.getMaxWorkers());
                    currentColumns = appendColumn(currentColumns, columnName);
                    continue;
                }
                if ("formatter".equals(spec.getType())) {
                    stream = stream.map(row -> applyFormatter(row, spec)).setParallelism(stagePlan.getMaxWorkers());
                    continue;
                }
                if ("date_formatter".equals(spec.getType())) {
                    stream = stream.map(row -> applyDateFormatter(row, spec)).setParallelism(stagePlan.getMaxWorkers());
                    continue;
                }
                throw new IllegalStateException("Unsupported native row stage operator: " + spec.getType());
            } catch (RuntimeException exception) {
                throw PipelineStepExecutionException.wrap(spec, exception);
            }
        }

        List<RuntimeRow> resultRows = new ArrayList<>();
        try (var iterator = stream.executeAndCollect()) {
            while (iterator.hasNext()) {
                resultRows.add(iterator.next());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to execute Flink native row stage.", exception);
        }
        resultRows.sort(Comparator.comparingLong(RuntimeRow::getRowId));
        return new RuntimeTable(currentColumns, resultRows);
    }

    // 中文说明：列式并发当前仍聚焦逐列无副作用算子，避免把互相依赖的列拆散后产生语义漂移。
    private RuntimeTable executeColumnStage(RuntimeTable inputTable, StagePlan stagePlan) {
        List<String> touchedColumns = resolveTouchedColumns(stagePlan.getSpecs(), inputTable.getColumns());
        if (stagePlan.getMaxWorkers() <= 1 || touchedColumns.size() <= 1) {
            return executeSerialStage(inputTable, stagePlan.getSpecs());
        }

        List<List<String>> columnGroups = chunkColumns(touchedColumns, stagePlan.getMaxWorkers());
        if (columnGroups.size() <= 1) {
            return executeSerialStage(inputTable, stagePlan.getSpecs());
        }

        ExecutorService executorService = Executors.newFixedThreadPool(Math.min(stagePlan.getMaxWorkers(), columnGroups.size()));
        try {
            List<CompletableFuture<RuntimeTable>> futures = columnGroups.stream()
                .map(columns -> CompletableFuture.supplyAsync(
                    () -> executeSerialStage(inputTable.projectColumns(columns), subsetSpecsForColumns(stagePlan.getSpecs(), columns)),
                    executorService
                ))
                .toList();

            List<RuntimeTable> partialResults = futures.stream().map(CompletableFuture::join).toList();
            return mergeColumnResults(inputTable, partialResults, previewColumnsAfterSpecs(inputTable.getColumns(), stagePlan.getSpecs()));
        } finally {
            executorService.shutdown();
        }
    }

    private RuntimeTable executeSerialStage(RuntimeTable inputTable, List<OperationSpec> specs) {
        RuntimeTable currentTable = inputTable;
        for (OperationSpec spec : specs) {
            try {
                validateRequiredInputColumns(currentTable.getColumns(), spec);
                currentTable = executeSerialSpec(currentTable, spec);
            } catch (RuntimeException exception) {
                throw PipelineStepExecutionException.wrap(spec, exception);
            }
        }
        return currentTable;
    }

    private RuntimeTable executeSerialSpec(RuntimeTable inputTable, OperationSpec spec) {
        if ("filter".equals(spec.getType())) {
            return applyFilter(inputTable, spec);
        }
        if ("rename".equals(spec.getType())) {
            return applyRename(inputTable, spec);
        }
        if ("aggregate".equals(spec.getType())) {
            return applyAggregate(inputTable, spec);
        }
        if ("sort".equals(spec.getType())) {
            return applySort(inputTable, spec);
        }
        if ("tag".equals(spec.getType())) {
            return applyTag(inputTable, spec);
        }
        if ("constant".equals(spec.getType())) {
            return applyConstant(inputTable, spec);
        }
        if ("value_mapping".equals(spec.getType())) {
            return applyValueMapping(inputTable, spec);
        }
        if ("col_assign".equals(spec.getType())) {
            return applyVectorizedColAssign(inputTable, spec);
        }
        if ("formatter".equals(spec.getType())) {
            return applyFormatter(inputTable, spec);
        }
        if ("date_formatter".equals(spec.getType())) {
            return applyDateFormatter(inputTable, spec);
        }
        throw new IllegalStateException("Unsupported native serial stage operator: " + spec.getType());
    }

    private RuntimeTable applyFilter(RuntimeTable inputTable, OperationSpec spec) {
        List<String> availableColumns = inputTable.getColumns();
        RowExpressionEvaluator.CompiledBooleanExpression compiledExpression =
            rowExpressionEvaluator.compileBooleanExpression(spec.getCondition(), availableColumns);
        List<RuntimeRow> filteredRows = inputTable.getRows().stream()
            .filter(row -> rowExpressionEvaluator.evaluateBoolean(row, compiledExpression))
            .toList();
        List<String> projectedColumns = projectedColumns(inputTable.getColumns(), spec);
        if (projectedColumns.isEmpty()) {
            return new RuntimeTable(inputTable.getColumns(), filteredRows);
        }
        return new RuntimeTable(projectedColumns, filteredRows.stream().map(row -> projectRow(row, projectedColumns)).toList());
    }

    private RuntimeTable applyRename(RuntimeTable inputTable, OperationSpec spec) {
        List<String> currentColumns = inputTable.getColumns();
        Map<String, String> renameMap = asStringMap(spec.getParams().get("map"));
        List<RuntimeRow> renamedRows = inputTable.getRows().stream()
            .map(row -> renameRow(row, currentColumns, renameMap))
            .toList();
        return new RuntimeTable(renameColumns(currentColumns, renameMap), renamedRows);
    }

    private RuntimeTable applyTag(RuntimeTable inputTable, OperationSpec spec) {
        List<String> currentColumns = inputTable.getColumns();
        List<RowExpressionEvaluator.CompiledBooleanExpression> compiledConditions = compileConditions(
            asStringList(spec.getParams().get("conditions")),
            currentColumns
        );
        List<String> tags = asStringList(spec.getParams().get("tags"));
        String tagColumn = String.valueOf(spec.getParams().get("tag_col_name"));
        Object defaultTag = spec.getParams().get("default_tag");
        List<RuntimeRow> taggedRows = inputTable.getRows().stream()
            .map(row -> applyTag(row, tagColumn, compiledConditions, tags, defaultTag))
            .toList();
        return new RuntimeTable(appendColumn(currentColumns, tagColumn), taggedRows);
    }

    private RuntimeTable applyConstant(RuntimeTable inputTable, OperationSpec spec) {
        List<RuntimeRow> resultRows = inputTable.getRows().stream().map(row -> applyConstant(row, spec)).toList();
        return new RuntimeTable(appendColumns(inputTable.getColumns(), asStringList(spec.getParams().get("columns"))), resultRows);
    }

    private RuntimeTable applyValueMapping(RuntimeTable inputTable, OperationSpec spec) {
        List<RuntimeRow> resultRows = inputTable.getRows().stream().map(row -> applyValueMapping(row, spec)).toList();
        List<String> resultColumns = isReplaceMode(spec.getParams().get("mode"))
            ? inputTable.getColumns()
            : appendColumns(inputTable.getColumns(), asStringList(spec.getParams().get("result_columns")));
        return new RuntimeTable(resultColumns, resultRows);
    }

    private RuntimeTable applyVectorizedColAssign(RuntimeTable inputTable, OperationSpec spec) {
        List<String> currentColumns = inputTable.getColumns();
        String columnName = String.valueOf(spec.getParams().get("col_name"));
        RowExpressionEvaluator.CompiledBooleanExpression compiledCondition =
            rowExpressionEvaluator.compileBooleanExpression(spec.getCondition(), currentColumns);
        RowExpressionEvaluator.CompiledValueExpression compiledValue =
            rowExpressionEvaluator.compileValueExpression(String.valueOf(spec.getParams().get("value_expr")), currentColumns);

        List<RuntimeRow> resultRows = inputTable.getRows().stream()
            .map(row -> applyVectorizedColAssign(row, columnName, compiledCondition, compiledValue))
            .toList();
        return new RuntimeTable(appendColumn(currentColumns, columnName), resultRows);
    }

    private RuntimeTable applySort(RuntimeTable inputTable, OperationSpec spec) {
        List<String> sortColumns = asStringList(spec.getParams().get("by"));
        List<Boolean> ascending = normalizeAscending(sortColumns, spec.getParams().get("ascending"));
        List<RuntimeRow> currentRows = inputTable.getRows();
        Map<Long, Integer> positionByRowId = new LinkedHashMap<>();
        for (int index = 0; index < currentRows.size(); index++) {
            positionByRowId.put(currentRows.get(index).getRowId(), index);
        }

        List<RuntimeRow> sortedRows = new ArrayList<>(currentRows);
        sortedRows.sort((left, right) -> {
            Map<String, Object> leftValues = left.getValues();
            Map<String, Object> rightValues = right.getValues();
            for (int index = 0; index < sortColumns.size(); index++) {
                int comparison = compareValues(
                    leftValues.get(sortColumns.get(index)),
                    rightValues.get(sortColumns.get(index)),
                    ascending.get(index)
                );
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(
                positionByRowId.getOrDefault(left.getRowId(), Integer.MAX_VALUE),
                positionByRowId.getOrDefault(right.getRowId(), Integer.MAX_VALUE)
            );
        });
        return new RuntimeTable(inputTable.getColumns(), sortedRows);
    }

    @SuppressWarnings("unchecked")
    private RuntimeTable applyAggregate(RuntimeTable inputTable, OperationSpec spec) {
        List<String> byColumns = asStringList(spec.getParams().get("by"));
        Map<String, Object> actions = spec.getParams().get("actions") instanceof Map<?, ?> rawActions
            ? new LinkedHashMap<>((Map<String, Object>) rawActions)
            : new LinkedHashMap<>();
        String method = String.valueOf(actions.get("method"));
        List<String> onColumns = asStringList(actions.get("on"));
        List<String> renameColumns = asStringList(actions.get("rename"));
        List<String> outputColumns = renameColumns.isEmpty() ? onColumns : renameColumns;

        Map<GroupKey, List<RuntimeRow>> groups = new LinkedHashMap<>();
        for (RuntimeRow row : inputTable.getRows()) {
            List<Object> keyValues = byColumns.stream().map(column -> row.getValues().get(column)).toList();
            if (keyValues.stream().anyMatch(Objects::isNull)) {
                continue;
            }
            groups.computeIfAbsent(new GroupKey(keyValues), ignored -> new ArrayList<>()).add(row);
        }

        List<GroupKey> orderedKeys = new ArrayList<>(groups.keySet());
        orderedKeys.sort((left, right) -> compareGroupKeys(left.values(), right.values()));

        List<String> resultColumns = new ArrayList<>(byColumns);
        resultColumns.addAll(outputColumns);
        List<RuntimeRow> resultRows = new ArrayList<>();
        long nextRowId = 0L;
        for (GroupKey key : orderedKeys) {
            LinkedHashMap<String, Object> aggregated = new LinkedHashMap<>();
            for (int index = 0; index < byColumns.size(); index++) {
                aggregated.put(byColumns.get(index), key.values().get(index));
            }

            List<RuntimeRow> groupRows = groups.get(key);
            for (int index = 0; index < onColumns.size(); index++) {
                String sourceColumn = onColumns.get(index);
                String targetColumn = outputColumns.get(index);
                aggregated.put(targetColumn, aggregateValues(groupRows, sourceColumn, method));
            }
            resultRows.add(new RuntimeRow(nextRowId++, aggregated));
        }
        return new RuntimeTable(resultColumns, resultRows);
    }

    private RuntimeTable applyFormatter(RuntimeTable inputTable, OperationSpec spec) {
        List<RuntimeRow> transformedRows = inputTable.getRows().stream().map(row -> applyFormatter(row, spec)).toList();
        return new RuntimeTable(inputTable.getColumns(), transformedRows);
    }

    private RuntimeTable applyDateFormatter(RuntimeTable inputTable, OperationSpec spec) {
        List<RuntimeRow> transformedRows = inputTable.getRows().stream().map(row -> applyDateFormatter(row, spec)).toList();
        return new RuntimeTable(inputTable.getColumns(), transformedRows);
    }

    private void validateRequiredInputColumns(List<String> availableColumns, OperationSpec spec) {
        LinkedHashSet<String> missingColumns = new LinkedHashSet<>();
        if ("sort".equals(spec.getType())) {
            missingColumns.addAll(findMissingColumns(availableColumns, asStringList(spec.getParams().get("by"))));
        } else if ("aggregate".equals(spec.getType())) {
            missingColumns.addAll(findMissingColumns(availableColumns, asStringList(spec.getParams().get("by"))));
            Object rawActions = spec.getParams().get("actions");
            if (rawActions instanceof Map<?, ?> actions) {
                missingColumns.addAll(findMissingColumns(availableColumns, asStringList(actions.get("on"))));
            }
        } else if ("formatter".equals(spec.getType()) || "date_formatter".equals(spec.getType())) {
            missingColumns.addAll(findMissingColumns(availableColumns, asStringList(spec.getParams().get("columns"))));
        }

        if (!missingColumns.isEmpty()) {
            throw new PipelineStepExecutionException(spec, buildMissingColumnsDetail(missingColumns));
        }
    }

    private RuntimeTable mergeColumnResults(RuntimeTable baseTable, List<RuntimeTable> partialResults, List<String> finalColumns) {
        Map<Long, LinkedHashMap<String, Object>> mergedByRowId = new LinkedHashMap<>();
        for (RuntimeRow row : baseTable.getRows()) {
            mergedByRowId.put(row.getRowId(), row.getValues());
        }

        for (RuntimeTable partialResult : partialResults) {
            for (RuntimeRow partialRow : partialResult.getRows()) {
                LinkedHashMap<String, Object> targetValues = mergedByRowId.get(partialRow.getRowId());
                if (targetValues == null) {
                    continue;
                }
                for (String column : partialResult.getColumns()) {
                    targetValues.put(column, partialRow.getValues().get(column));
                }
            }
        }

        List<RuntimeRow> mergedRows = new ArrayList<>();
        for (RuntimeRow baseRow : baseTable.getRows()) {
            mergedRows.add(new RuntimeRow(baseRow.getRowId(), mergedByRowId.get(baseRow.getRowId())));
        }
        return new RuntimeTable(finalColumns, mergedRows);
    }

    private List<String> resolveTouchedColumns(List<OperationSpec> specs, List<String> preferredOrder) {
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        for (OperationSpec spec : specs) {
            if (!COLUMN_NATIVE_TYPES.contains(spec.getType())) {
                continue;
            }
            Object rawColumns = spec.getParams().get("columns");
            if (!(rawColumns instanceof List<?> columnList)) {
                continue;
            }
            for (Object column : columnList) {
                columns.add(String.valueOf(column));
            }
        }
        List<String> ordered = new ArrayList<>();
        for (String column : preferredOrder) {
            if (columns.contains(column)) {
                ordered.add(column);
            }
        }
        // 中文说明：constant 会新增输入表里还不存在的列，列式切分时这些目标列也必须保留，
        // 否则会被误判成“没有可切分列”而退回 serial。
        for (String column : columns) {
            if (!ordered.contains(column)) {
                ordered.add(column);
            }
        }
        return ordered;
    }

    private List<List<String>> chunkColumns(List<String> columns, int maxWorkers) {
        int workerCount = Math.max(1, Math.min(maxWorkers, columns.size()));
        List<List<String>> groups = new ArrayList<>();
        for (int index = 0; index < workerCount; index++) {
            groups.add(new ArrayList<>());
        }
        for (int index = 0; index < columns.size(); index++) {
            groups.get(index % workerCount).add(columns.get(index));
        }
        return groups.stream().filter(group -> !group.isEmpty()).toList();
    }

    private List<OperationSpec> subsetSpecsForColumns(List<OperationSpec> specs, List<String> allowedColumns) {
        Set<String> allowed = new LinkedHashSet<>(allowedColumns);
        List<OperationSpec> subset = new ArrayList<>();
        for (OperationSpec spec : specs) {
            if (!COLUMN_NATIVE_TYPES.contains(spec.getType())) {
                continue;
            }
            if ("constant".equals(spec.getType())) {
                List<String> selectedColumns = asStringList(spec.getParams().get("columns")).stream()
                    .filter(allowed::contains)
                    .toList();
                if (selectedColumns.isEmpty()) {
                    continue;
                }
                Map<String, Object> params = new LinkedHashMap<>(spec.getParams());
                LinkedHashMap<String, Object> selectedValues = new LinkedHashMap<>();
                asObjectMap(params.get("values")).forEach((column, value) -> {
                    if (selectedColumns.contains(column)) {
                        selectedValues.put(column, value);
                    }
                });
                params.put("columns", selectedColumns);
                params.put("values", selectedValues);
                subset.add(new OperationSpec(spec.getType(), params, spec.getCondition(), spec.getSourceStepIndex(), spec.getExecution()));
                continue;
            }
            if ("value_mapping".equals(spec.getType())) {
                List<String> selectedColumns = asStringList(spec.getParams().get("columns")).stream()
                    .filter(allowed::contains)
                    .toList();
                if (selectedColumns.isEmpty()) {
                    continue;
                }
                Map<String, Object> params = new LinkedHashMap<>(spec.getParams());
                LinkedHashMap<String, Map<String, Object>> selectedMappings = new LinkedHashMap<>();
                asNestedObjectMap(params.get("mappings")).forEach((column, mapping) -> {
                    if (selectedColumns.contains(column)) {
                        selectedMappings.put(column, mapping);
                    }
                });
                params.put("columns", selectedColumns);
                params.put("mappings", selectedMappings);
                params.put(
                    "result_columns",
                    isReplaceMode(params.get("mode"))
                        ? selectedColumns
                        : selectedColumns.stream().map(column -> column + "_mapped").toList()
                );
                subset.add(new OperationSpec(spec.getType(), params, spec.getCondition(), spec.getSourceStepIndex(), spec.getExecution()));
                continue;
            }
            Object rawColumns = spec.getParams().get("columns");
            if (!(rawColumns instanceof List<?> columnList)) {
                continue;
            }
            List<String> selectedColumns = columnList.stream()
                .map(String::valueOf)
                .filter(allowed::contains)
                .toList();
            if (selectedColumns.isEmpty()) {
                continue;
            }
            Map<String, Object> params = new LinkedHashMap<>(spec.getParams());
            params.put("columns", selectedColumns);
            subset.add(new OperationSpec(spec.getType(), params, spec.getCondition(), spec.getSourceStepIndex(), spec.getExecution()));
        }
        return subset;
    }

    private List<RowExpressionEvaluator.CompiledBooleanExpression> compileConditions(
        List<String> expressions,
        List<String> availableColumns
    ) {
        return expressions.stream()
            .map(expression -> rowExpressionEvaluator.compileBooleanExpression(expression, availableColumns))
            .toList();
    }

    private RuntimeRow applyTag(
        RuntimeRow row,
        String tagColumn,
        List<RowExpressionEvaluator.CompiledBooleanExpression> compiledConditions,
        List<String> tags,
        Object defaultTag
    ) {
        Object resolvedTag = defaultTag;
        for (int index = 0; index < compiledConditions.size(); index++) {
            if (rowExpressionEvaluator.evaluateBoolean(row, compiledConditions.get(index))) {
                resolvedTag = tags.get(index);
                break;
            }
        }
        LinkedHashMap<String, Object> values = row.getValues();
        values.put(tagColumn, resolvedTag);
        return new RuntimeRow(row.getRowId(), values);
    }

    private RuntimeRow applyConstant(RuntimeRow row, OperationSpec spec) {
        LinkedHashMap<String, Object> values = row.getValues();
        asObjectMap(spec.getParams().get("values")).forEach(values::put);
        return new RuntimeRow(row.getRowId(), values);
    }

    private RuntimeRow applyValueMapping(RuntimeRow row, OperationSpec spec) {
        LinkedHashMap<String, Object> values = row.getValues();
        LinkedHashMap<String, Map<String, Object>> mappings = asNestedObjectMap(spec.getParams().get("mappings"));
        Object defaultValue = spec.getParams().get("default");
        boolean replaceMode = isReplaceMode(spec.getParams().get("mode"));

        for (String columnName : asStringList(spec.getParams().get("columns"))) {
            if (!values.containsKey(columnName)) {
                continue;
            }
            Object currentValue = values.get(columnName);
            Map<String, Object> mapping = mappings.get(columnName);
            Object mappedValue = resolveMappedValue(mapping, currentValue);
            if (replaceMode) {
                values.put(
                    columnName,
                    mappedValue != null
                        ? mappedValue
                        : (hasExplicitDefault(defaultValue) ? defaultValue : currentValue)
                );
                continue;
            }
            String targetColumn = columnName + "_mapped";
            values.put(targetColumn, mappedValue != null ? mappedValue : (hasExplicitDefault(defaultValue) ? defaultValue : null));
        }

        return new RuntimeRow(row.getRowId(), values);
    }

    private RuntimeRow applyVectorizedColAssign(
        RuntimeRow row,
        String columnName,
        RowExpressionEvaluator.CompiledBooleanExpression compiledCondition,
        RowExpressionEvaluator.CompiledValueExpression compiledValue
    ) {
        LinkedHashMap<String, Object> values = row.getValues();
        Object assignedValue = rowExpressionEvaluator.evaluateBoolean(row, compiledCondition)
            ? rowExpressionEvaluator.evaluateValue(row, compiledValue)
            : null;
        values.put(columnName, assignedValue);
        return new RuntimeRow(row.getRowId(), values);
    }

    private static RuntimeRow renameRow(RuntimeRow row, List<String> currentColumns, Map<String, String> renameMap) {
        LinkedHashMap<String, Object> renamed = new LinkedHashMap<>();
        Map<String, Object> values = row.getValues();
        for (String column : currentColumns) {
            String renamedColumn = renameMap.getOrDefault(column, column);
            renamed.put(renamedColumn, values.get(column));
        }
        return new RuntimeRow(row.getRowId(), renamed);
    }

    private static RuntimeRow projectRow(RuntimeRow row, List<String> projectedColumns) {
        LinkedHashMap<String, Object> projected = new LinkedHashMap<>();
        Map<String, Object> values = row.getValues();
        for (String column : projectedColumns) {
            projected.put(column, values.get(column));
        }
        return new RuntimeRow(row.getRowId(), projected);
    }

    private static List<String> projectedColumns(List<String> currentColumns, OperationSpec spec) {
        List<String> requiredColumns = asStringList(spec.getParams().get("requiredCols"));
        if (requiredColumns.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> projected = new LinkedHashSet<>();
        for (String column : currentColumns) {
            if (requiredColumns.contains(column)) {
                projected.add(column);
            }
        }
        return new ArrayList<>(projected);
    }

    private List<String> renameColumns(List<String> currentColumns, Map<String, String> renameMap) {
        return currentColumns.stream().map(column -> renameMap.getOrDefault(column, column)).toList();
    }

    private List<String> appendColumn(List<String> currentColumns, String columnName) {
        List<String> nextColumns = new ArrayList<>(currentColumns);
        if (!nextColumns.contains(columnName)) {
            nextColumns.add(columnName);
        }
        return nextColumns;
    }

    private List<String> appendColumns(List<String> currentColumns, List<String> columnNames) {
        List<String> nextColumns = new ArrayList<>(currentColumns);
        for (String columnName : columnNames) {
            if (!nextColumns.contains(columnName)) {
                nextColumns.add(columnName);
            }
        }
        return nextColumns;
    }

    // 中文说明：列式并发回拼时需要按逻辑算子顺序重建最终列序，避免新增列因为 worker 分组顺序而漂移。
    private List<String> previewColumnsAfterSpecs(List<String> currentColumns, List<OperationSpec> specs) {
        List<String> previewColumns = new ArrayList<>(currentColumns);
        for (OperationSpec spec : specs) {
            if ("constant".equals(spec.getType())) {
                previewColumns = appendColumns(previewColumns, asStringList(spec.getParams().get("columns")));
                continue;
            }
            if ("value_mapping".equals(spec.getType()) && !isReplaceMode(spec.getParams().get("mode"))) {
                previewColumns = appendColumns(previewColumns, asStringList(spec.getParams().get("result_columns")));
            }
        }
        return previewColumns;
    }

    private static RuntimeRow applyFormatter(RuntimeRow row, OperationSpec spec) {
        LinkedHashMap<String, Object> values = row.getValues();
        String method = String.valueOf(spec.getParams().get("method"));
        String valueExpr = String.valueOf(spec.getParams().getOrDefault("value_expr", ""));
        for (String column : asStringList(spec.getParams().get("columns"))) {
            values.put(column, transformFormatterValue(values.get(column), method, valueExpr));
        }
        return new RuntimeRow(row.getRowId(), values);
    }

    private static RuntimeRow applyDateFormatter(RuntimeRow row, OperationSpec spec) {
        LinkedHashMap<String, Object> values = row.getValues();
        for (String column : asStringList(spec.getParams().get("columns"))) {
            values.put(column, transformDateValue(values.get(column), spec.getParams()));
        }
        return new RuntimeRow(row.getRowId(), values);
    }

    private Object aggregateValues(List<RuntimeRow> groupRows, String column, String method) {
        List<Object> values = groupRows.stream()
            .map(row -> row.getValues().get(column))
            .filter(Objects::nonNull)
            .toList();

        if ("count".equals(method)) {
            return (long) values.size();
        }
        if (values.isEmpty()) {
            if ("sum".equals(method)) {
                return 0L;
            }
            return null;
        }
        if ("sum".equals(method)) {
            if (values.stream().allMatch(Number.class::isInstance)) {
                boolean allIntegral = values.stream().allMatch(value -> value instanceof Byte
                    || value instanceof Short
                    || value instanceof Integer
                    || value instanceof Long);
                double sum = values.stream().mapToDouble(value -> ((Number) value).doubleValue()).sum();
                return allIntegral ? Math.round(sum) : sum;
            }
            return values.stream().map(String::valueOf).reduce("", String::concat);
        }
        if ("mean".equals(method)) {
            return values.stream().mapToDouble(value -> toDouble(value)).average().orElse(Double.NaN);
        }
        if ("min".equals(method)) {
            return values.stream().min((left, right) -> compareValues(left, right, true)).orElse(null);
        }
        if ("max".equals(method)) {
            return values.stream().max((left, right) -> compareValues(left, right, true)).orElse(null);
        }
        if ("std".equals(method)) {
            if (values.size() <= 1) {
                return null;
            }
            double mean = values.stream().mapToDouble(value -> toDouble(value)).average().orElse(Double.NaN);
            double squaredDistance = values.stream()
                .mapToDouble(value -> {
                    double delta = toDouble(value) - mean;
                    return delta * delta;
                })
                .sum();
            return Math.sqrt(squaredDistance / (values.size() - 1));
        }
        throw new IllegalStateException("Unsupported aggregate method: " + method);
    }

    private int compareGroupKeys(List<Object> leftValues, List<Object> rightValues) {
        int count = Math.min(leftValues.size(), rightValues.size());
        for (int index = 0; index < count; index++) {
            int comparison = compareValues(leftValues.get(index), rightValues.get(index), true);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftValues.size(), rightValues.size());
    }

    private static int compareValues(Object left, Object right, boolean ascending) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }

        int comparison;
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            comparison = Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        } else if (left instanceof Comparable<?> && right != null && left.getClass().isAssignableFrom(right.getClass())) {
            @SuppressWarnings("unchecked")
            Comparable<Object> comparable = (Comparable<Object>) left;
            comparison = comparable.compareTo(right);
        } else {
            comparison = String.valueOf(left).compareTo(String.valueOf(right));
        }
        return ascending ? comparison : -comparison;
    }

    private static List<Boolean> normalizeAscending(List<String> sortColumns, Object rawAscending) {
        List<Boolean> ascending = new ArrayList<>();
        if (rawAscending instanceof List<?> rawList) {
            rawList.forEach(value -> ascending.add(Boolean.valueOf(String.valueOf(value))));
        }
        while (ascending.size() < sortColumns.size()) {
            ascending.add(Boolean.TRUE);
        }
        return ascending.isEmpty() && !sortColumns.isEmpty()
            ? sortColumns.stream().map(column -> Boolean.TRUE).toList()
            : ascending;
    }

    private static Object transformFormatterValue(Object value, String method, String valueExpr) {
        if ("ToUpperCase".equals(method)) {
            return value == null ? "" : String.valueOf(value).toUpperCase(Locale.ROOT);
        }
        if ("ToLowerCase".equals(method)) {
            return value == null ? "" : String.valueOf(value).toLowerCase(Locale.ROOT);
        }
        if ("StringPrefix".equals(method)) {
            return value == null ? "" : valueExpr + value;
        }
        if ("DecimalScale".equals(method)) {
            return value == null ? "" : String.format(Locale.US, "%." + Integer.parseInt(valueExpr) + "f", toDouble(value));
        }
        if ("NumberSeparator".equals(method)) {
            return value == null ? "" : String.format(Locale.US, "%,.2f", toDouble(value));
        }
        if ("TakePercentage".equals(method)) {
            return value == null ? "" : toDouble(value) * Double.parseDouble(valueExpr);
        }
        if ("ChangeEncodingToGBK".equals(method)) {
            if (value == null) {
                return "";
            }
            String raw = String.valueOf(value);
            try {
                String gbkStable = new String(raw.getBytes("GBK"), "GBK");
                if (raw.equals(gbkStable)) {
                    return raw;
                }
                return new String(raw.getBytes(Charset.forName(valueExpr)), Charset.forName("GBK"));
            } catch (Exception exception) {
                return raw;
            }
        }
        throw new IllegalStateException("Unsupported formatter method: " + method);
    }

    private static Object transformDateValue(Object value, Map<String, Object> params) {
        String fromFormat = String.valueOf(params.get("from_format"));
        String toFormat = String.valueOf(params.get("to_format"));
        String fromTimezone = String.valueOf(params.getOrDefault("from_timezone", "UTC"));
        String toTimezone = String.valueOf(params.getOrDefault("to_timezone", "UTC"));

        if ("UTC".equals(fromFormat) && "UTC".equals(toFormat)) {
            return value == null ? 0L : Long.valueOf(toLong(value));
        }
        if (!"UTC".equals(fromFormat) && "UTC".equals(toFormat)) {
            if (value == null) {
                return 0L;
            }
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMATS.get(fromFormat));
            LocalDateTime localDateTime = parseLocalDateTime(String.valueOf(toLong(value)), formatter);
            return localDateTime.atZone(ZoneId.of(fromTimezone)).toEpochSecond();
        }
        if ("UTC".equals(fromFormat)) {
            if (value == null) {
                return "";
            }
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMATS.get(toFormat));
            return Instant.ofEpochSecond(toLong(value)).atZone(ZoneId.of(toTimezone)).format(formatter);
        }
        if (value == null) {
            return "";
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMATS.get(fromFormat));
        LocalDateTime localDateTime = parseLocalDateTime(String.valueOf(toLong(value)), formatter);
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern(DATE_FORMATS.get(toFormat));
        return localDateTime.atZone(ZoneId.of(fromTimezone))
            .withZoneSameInstant(ZoneId.of(toTimezone))
            .format(targetFormatter);
    }

    private static LocalDateTime parseLocalDateTime(String rawValue, DateTimeFormatter formatter) {
        TemporalAccessor parsed = formatter.parse(rawValue);
        if (parsed.query(TemporalQueries.localTime()) == null) {
            return LocalDate.from(parsed).atStartOfDay();
        }
        return LocalDateTime.from(parsed);
    }

    private static double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static List<String> findMissingColumns(List<String> availableColumns, List<String> requiredColumns) {
        LinkedHashSet<String> available = new LinkedHashSet<>(availableColumns);
        LinkedHashSet<String> missing = new LinkedHashSet<>();
        for (String column : requiredColumns) {
            if (!available.contains(column)) {
                missing.add(column);
            }
        }
        return new ArrayList<>(missing);
    }

    private static String buildMissingColumnsDetail(List<String> missingColumns) {
        if (missingColumns.size() == 1) {
            return "Column `" + missingColumns.get(0) + "` does not exist.";
        }
        return "Columns " + missingColumns.stream().map(column -> "`" + column + "`").toList() + " do not exist.";
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> asStringMap(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            return Map.of();
        }
        LinkedHashMap<String, String> values = new LinkedHashMap<>();
        rawMap.forEach((key, value) -> values.put(String.valueOf(key), String.valueOf(value)));
        return values;
    }

    private static List<String> asStringList(Object rawValue) {
        if (!(rawValue instanceof List<?> rawList)) {
            return List.of();
        }
        return rawList.stream().map(String::valueOf).toList();
    }

    @SuppressWarnings("unchecked")
    private static LinkedHashMap<String, Object> asObjectMap(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            return new LinkedHashMap<>();
        }
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        rawMap.forEach((key, value) -> values.put(String.valueOf(key), value));
        return values;
    }

    @SuppressWarnings("unchecked")
    private static LinkedHashMap<String, Map<String, Object>> asNestedObjectMap(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            return new LinkedHashMap<>();
        }
        LinkedHashMap<String, Map<String, Object>> values = new LinkedHashMap<>();
        rawMap.forEach((key, value) -> {
            LinkedHashMap<String, Object> nested = new LinkedHashMap<>();
            if (value instanceof Map<?, ?> nestedMap) {
                nestedMap.forEach((nestedKey, nestedValue) -> nested.put(String.valueOf(nestedKey), nestedValue));
            }
            values.put(String.valueOf(key), nested);
        });
        return values;
    }

    private static boolean isReplaceMode(Object rawMode) {
        return "replace".equals(String.valueOf(rawMode == null ? "replace" : rawMode));
    }

    private static boolean hasExplicitDefault(Object defaultValue) {
        return defaultValue != null && (!(defaultValue instanceof String stringValue) || !stringValue.isEmpty());
    }

    private static Object resolveMappedValue(Map<String, Object> mapping, Object currentValue) {
        if (mapping == null) {
            return null;
        }
        return mapping.get(currentValue);
    }

    private record GroupKey(List<Object> values) {
    }
}
