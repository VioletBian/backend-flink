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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dataprocessor.flink.planner.ExecutionConfig;
import com.dataprocessor.flink.planner.OperationSpec;
import com.dataprocessor.flink.planner.StagePlan;
import com.dataprocessor.flink.runtime.RuntimeRow;
import com.dataprocessor.flink.runtime.RuntimeTable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

// 中文说明：这里只实现当前最容易守住语义的 Java native 段，其余 stage 统一回退 Python，避免把备用后端做成“半对齐”。
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

    private final BatchTableEnvironmentFactory batchTableEnvironmentFactory;

    public NativeStageExecutor(BatchTableEnvironmentFactory batchTableEnvironmentFactory) {
        this.batchTableEnvironmentFactory = batchTableEnvironmentFactory;
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

    // 中文说明：行式 stage 真正交给 Flink batch DataStream 执行，确保这部分不是“假并行”。
    private RuntimeTable executeRowStage(RuntimeTable inputTable, StagePlan stagePlan) {
        if (stagePlan.getMaxWorkers() <= 1 || inputTable.getRowCount() < 2) {
            return executeSerialStage(inputTable, stagePlan.getSpecs());
        }

        List<String> currentColumns = new ArrayList<>(inputTable.getColumns());
        StreamExecutionEnvironment env = batchTableEnvironmentFactory.createBatchStreamExecutionEnvironment(stagePlan.getMaxWorkers());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<RuntimeRow> stream = env.fromCollection(inputTable.getRows()).setParallelism(stagePlan.getMaxWorkers());
        for (OperationSpec spec : stagePlan.getSpecs()) {
            if ("rename".equals(spec.getType())) {
                List<String> beforeColumns = new ArrayList<>(currentColumns);
                Map<String, String> renameMap = asStringMap(spec.getParams().get("map"));
                stream = stream.map(row -> renameRow(row, beforeColumns, renameMap)).setParallelism(stagePlan.getMaxWorkers());
                currentColumns = renameColumns(currentColumns, renameMap);
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

    // 中文说明：列式 stage 只对当前已原生支持的逐列算子开放，按列分组并发跑后再按 _row_id 回拼。
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
            return mergeColumnResults(inputTable, partialResults);
        } finally {
            executorService.shutdown();
        }
    }

    private RuntimeTable executeSerialStage(RuntimeTable inputTable, List<OperationSpec> specs) {
        List<String> currentColumns = new ArrayList<>(inputTable.getColumns());
        List<RuntimeRow> currentRows = inputTable.getRows();

        for (OperationSpec spec : specs) {
            if ("rename".equals(spec.getType())) {
                Map<String, String> renameMap = asStringMap(spec.getParams().get("map"));
                List<String> beforeColumns = new ArrayList<>(currentColumns);
                currentRows = currentRows.stream().map(row -> renameRow(row, beforeColumns, renameMap)).toList();
                currentColumns = renameColumns(currentColumns, renameMap);
                continue;
            }
            if ("formatter".equals(spec.getType())) {
                currentRows = currentRows.stream().map(row -> applyFormatter(row, spec)).toList();
                continue;
            }
            if ("date_formatter".equals(spec.getType())) {
                currentRows = currentRows.stream().map(row -> applyDateFormatter(row, spec)).toList();
                continue;
            }
            throw new IllegalStateException("Unsupported native serial stage operator: " + spec.getType());
        }

        return new RuntimeTable(currentColumns, currentRows);
    }

    private RuntimeTable mergeColumnResults(RuntimeTable baseTable, List<RuntimeTable> partialResults) {
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

        List<RuntimeRow> mergedRows = mergedByRowId.entrySet().stream()
            .map(entry -> new RuntimeRow(entry.getKey(), entry.getValue()))
            .toList();
        return new RuntimeTable(baseTable.getColumns(), mergedRows);
    }

    private List<String> resolveTouchedColumns(List<OperationSpec> specs, List<String> preferredOrder) {
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        for (OperationSpec spec : specs) {
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
            if (!spec.getParams().containsKey("columns")) {
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
            Map<String, Object> params = spec.getParams();
            params.put("columns", selectedColumns);
            subset.add(new OperationSpec(spec.getType(), params, spec.getCondition(), spec.getSourceStepIndex(), spec.getExecution()));
        }
        return subset;
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

    private List<String> renameColumns(List<String> currentColumns, Map<String, String> renameMap) {
        return currentColumns.stream().map(column -> renameMap.getOrDefault(column, column)).toList();
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
}
