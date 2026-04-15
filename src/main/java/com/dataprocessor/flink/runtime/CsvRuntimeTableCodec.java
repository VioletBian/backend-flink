package com.dataprocessor.flink.runtime;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

// 中文说明：Java 侧需要在原生 stage 和 Python fallback 之间反复交换 CSV，因此单独收口 CSV <-> RuntimeTable。
@Component
public class CsvRuntimeTableCodec {

    private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("^-?\\d+\\.\\d+$");

    public RuntimeTable read(byte[] csvBytes) {
        IllegalArgumentException lastError = null;
        for (Charset charset : List.of(StandardCharsets.UTF_8, Charset.forName("GBK"))) {
            try {
                return read(new String(csvBytes, charset));
            } catch (IllegalArgumentException exception) {
                lastError = exception;
            }
        }
        throw lastError == null ? new IllegalArgumentException("Unable to parse uploaded CSV.") : lastError;
    }

    public RuntimeTable read(String csvText) {
        String normalizedCsvText = csvText != null && csvText.startsWith("\uFEFF") ? csvText.substring(1) : csvText;
        try (Reader reader = new StringReader(normalizedCsvText);
             CSVParser parser = CSVFormat.DEFAULT.builder()
                 .setHeader()
                 .setSkipHeaderRecord(true)
                 .setIgnoreEmptyLines(false)
                 .build()
                 .parse(reader)) {
            List<String> columns = parser.getHeaderNames();
            List<RuntimeRow> rows = new ArrayList<>();
            long rowId = 0L;
            for (CSVRecord record : parser) {
                LinkedHashMap<String, Object> values = new LinkedHashMap<>();
                for (String column : columns) {
                    values.put(column, inferValue(record.get(column)));
                }
                rows.add(new RuntimeRow(rowId++, values));
            }
            return new RuntimeTable(columns, rows);
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to parse uploaded CSV: " + exception.getMessage(), exception);
        }
    }

    public String write(RuntimeTable table) {
        try (StringWriter writer = new StringWriter();
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {
            printer.printRecord(table.getColumns());
            for (RuntimeRow row : table.getRows()) {
                List<String> record = new ArrayList<>();
                Map<String, Object> values = row.getValues();
                for (String column : table.getColumns()) {
                    Object value = values.get(column);
                    record.add(value == null ? "" : String.valueOf(value));
                }
                printer.printRecord(record);
            }
            printer.flush();
            return writer.toString();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to encode runtime table as CSV.", exception);
        }
    }

    public RuntimeTable readResponseMatrix(Object columnsPayload, Object dataPayload) {
        // 中文说明：Java <-> Python fallback 优先走紧凑矩阵契约，避免再把中间表先编码成 CSV 再反序列化回来。
        List<String> columns = toColumnList(columnsPayload);
        if (!(dataPayload instanceof List<?> rawRows)) {
            throw new IllegalArgumentException("Pipeline run response field `data` must be a JSON array.");
        }

        List<RuntimeRow> rows = new ArrayList<>();
        long rowId = 0L;
        for (Object rawRow : rawRows) {
            if (!(rawRow instanceof List<?> cellValues)) {
                throw new IllegalArgumentException("Pipeline run response `data` must be an array of row arrays.");
            }
            if (cellValues.size() != columns.size()) {
                throw new IllegalArgumentException("Pipeline run response row width does not match `columns`.");
            }
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                values.put(columns.get(columnIndex), cellValues.get(columnIndex));
            }
            rows.add(new RuntimeRow(rowId++, values));
        }
        return new RuntimeTable(columns, rows);
    }

    public List<List<Object>> toResponseDataMatrix(RuntimeTable table) {
        List<List<Object>> rows = new ArrayList<>();
        for (RuntimeRow row : table.getRows()) {
            List<Object> payload = new ArrayList<>();
            Map<String, Object> values = row.getValues();
            for (String column : table.getColumns()) {
                payload.add(values.get(column));
            }
            rows.add(payload);
        }
        return rows;
    }

    private List<String> toColumnList(Object columnsPayload) {
        if (!(columnsPayload instanceof List<?> rawColumns)) {
            throw new IllegalArgumentException("Pipeline run response field `columns` must be a JSON array.");
        }

        List<String> columns = new ArrayList<>();
        for (Object rawColumn : rawColumns) {
            columns.add(String.valueOf(rawColumn));
        }
        return columns;
    }

    private Object inferValue(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        if (INTEGER_PATTERN.matcher(raw).matches()) {
            try {
                return Long.valueOf(raw);
            } catch (NumberFormatException ignored) {
                return raw;
            }
        }
        if (DECIMAL_PATTERN.matcher(raw).matches()) {
            try {
                return Double.valueOf(raw);
            } catch (NumberFormatException ignored) {
                return raw;
            }
        }
        return raw;
    }
}
