package com.dataprocessor.flink.runtime;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

// 中文说明：统一用 _row_id 持有稳定行身份，后续无论原生执行还是 fallback 回拼都靠它保证顺序可恢复。
public class RuntimeRow implements Serializable {

    private final long rowId;
    private final LinkedHashMap<String, Object> values;

    public RuntimeRow(long rowId, Map<String, Object> values) {
        this.rowId = rowId;
        this.values = new LinkedHashMap<>(values);
    }

    public long getRowId() {
        return rowId;
    }

    public LinkedHashMap<String, Object> getValues() {
        return new LinkedHashMap<>(values);
    }

    public RuntimeRow copy() {
        return new RuntimeRow(rowId, values);
    }
}
