package com.dataprocessor.flink.runtime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// 中文说明：RuntimeTable 是 Java 侧 /run 的统一中间态，既能驱动原生 stage，也能和 Python fallback 互转 CSV。
public class RuntimeTable {

    private final List<String> columns;
    private final List<RuntimeRow> rows;

    public RuntimeTable(List<String> columns, List<RuntimeRow> rows) {
        this.columns = new ArrayList<>(columns);
        // 中文说明：这里保留调用方给出的当前行顺序；_row_id 只承担稳定索引身份，不能反过来把 sort 后顺序洗掉。
        this.rows = rows.stream().map(RuntimeRow::copy).toList();
    }

    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    public List<RuntimeRow> getRows() {
        return rows.stream().map(RuntimeRow::copy).toList();
    }

    public int getRowCount() {
        return rows.size();
    }

    public int getColumnCount() {
        return columns.size();
    }

    public RuntimeTable projectColumns(List<String> requestedColumns) {
        Set<String> columnSet = new LinkedHashSet<>(requestedColumns);
        List<String> projectedColumns = columns.stream().filter(columnSet::contains).toList();
        List<RuntimeRow> projectedRows = rows.stream()
            .map(row -> {
                LinkedHashMap<String, Object> values = new LinkedHashMap<>();
                Map<String, Object> source = row.getValues();
                for (String column : projectedColumns) {
                    values.put(column, source.get(column));
                }
                return new RuntimeRow(row.getRowId(), values);
            })
            .toList();
        return new RuntimeTable(projectedColumns, projectedRows);
    }

    public RuntimeTable replaceColumns(Map<String, Map<Long, Object>> columnValues, List<String> resultColumns) {
        List<RuntimeRow> mergedRows = new ArrayList<>();
        for (RuntimeRow row : rows) {
            LinkedHashMap<String, Object> values = row.getValues();
            for (String column : resultColumns) {
                Map<Long, Object> rowValues = columnValues.get(column);
                if (rowValues == null) {
                    continue;
                }
                values.put(column, rowValues.get(row.getRowId()));
            }
            mergedRows.add(new RuntimeRow(row.getRowId(), values));
        }

        List<RuntimeRow> normalizedRows = mergedRows.stream()
            .map(row -> {
                LinkedHashMap<String, Object> mergedValues = row.getValues();
                for (String resultColumn : resultColumns) {
                    mergedValues.putIfAbsent(resultColumn, null);
                }
                return new RuntimeRow(row.getRowId(), mergedValues);
            })
            .toList();

        List<String> mergedColumns = new ArrayList<>(columns);
        for (String resultColumn : resultColumns) {
            if (!mergedColumns.contains(resultColumn)) {
                mergedColumns.add(resultColumn);
            }
        }
        return new RuntimeTable(mergedColumns, normalizedRows);
    }
}
