package com.dataprocessor.flink.runtime;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

// 中文说明：rows 并行路径不再把 RuntimeRow 直接暴露给 Flink 类型系统，
// 而是统一压成 String payload，规避 Java 17 下 GenericType/Kryo 的兼容性问题。
public final class RuntimeRowWireCodec {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.USE_LONG_FOR_INTS, true);

    private RuntimeRowWireCodec() {
    }

    public static String encode(RuntimeRow row) {
        try {
            return OBJECT_MAPPER.writeValueAsString(new RuntimeRowEnvelope(row.getRowId(), row.getValues()));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to encode runtime row payload.", exception);
        }
    }

    public static RuntimeRow decode(String payload) {
        try {
            RuntimeRowEnvelope envelope = OBJECT_MAPPER.readValue(payload, RuntimeRowEnvelope.class);
            return new RuntimeRow(envelope.getRowId(), envelope.getValues());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to decode runtime row payload.", exception);
        }
    }

    public static final class RuntimeRowEnvelope {
        private long rowId;
        private LinkedHashMap<String, Object> values = new LinkedHashMap<>();

        public RuntimeRowEnvelope() {
        }

        public RuntimeRowEnvelope(long rowId, Map<String, Object> values) {
            this.rowId = rowId;
            this.values = new LinkedHashMap<>(values);
        }

        public long getRowId() {
            return rowId;
        }

        public void setRowId(long rowId) {
            this.rowId = rowId;
        }

        public LinkedHashMap<String, Object> getValues() {
            return new LinkedHashMap<>(values);
        }

        public void setValues(LinkedHashMap<String, Object> values) {
            this.values = values == null ? new LinkedHashMap<>() : new LinkedHashMap<>(values);
        }
    }
}
