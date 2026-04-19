package com.dataprocessor.flink.planner;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PipelineContractNormalizerTest {

    private final PipelineContractNormalizer normalizer = new PipelineContractNormalizer(new ObjectMapper());

    @Test
    void normalizesLegacyAliases() {
        String payload = """
            [
              {
                "type": "filter",
                "params": {
                  "by": ["Client Account", "Buy/Sell"],
                  "condition": "`Client Account` > 7000000000"
                }
              },
              {
                "type": "tag",
                "params": {
                  "col_name": "RiskTag",
                  "conditions": ["`Client Account` > 8000000000"],
                  "tags": ["high"]
                }
              }
            ]
            """;

        List<Map<String, Object>> normalized = normalizer.normalizeJson(payload);

        Assertions.assertEquals(
            List.of("Client Account", "Buy/Sell"),
            ((Map<?, ?>) normalized.get(0).get("params")).get("requiredCols")
        );
        Assertions.assertEquals(
            "RiskTag",
            ((Map<?, ?>) normalized.get(1).get("params")).get("tag_col_name")
        );
    }

    @Test
    void rejectsUnsupportedOperator() {
        IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> normalizer.normalizeJson("""
                [{"type": "mystery_operator", "params": {}}]
                """)
        );

        Assertions.assertTrue(exception.getMessage().contains("Unsupported operator"));
    }

    @Test
    void acceptsConstantAndValueMappingOperators() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "constant",
                "params": {
                  "columns": {
                    "Country": "CN"
                  }
                }
              },
              {
                "type": "value_mapping",
                "params": {
                  "mode": "replace",
                  "mappings": {
                    "Client Account": {
                      "7001": "VIP"
                    }
                  }
                }
              }
            ]
            """);

        Assertions.assertEquals("constant", normalized.get(0).get("type"));
        Assertions.assertEquals("value_mapping", normalized.get(1).get("type"));
    }

    @Test
    void normalizesAggregateMethodToLowercaseCanonicalValue() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "aggregate",
                "params": {
                  "by": ["Client Account", "Contract"],
                  "actions": {
                    "method": "Sum",
                    "on": ["Quantity"]
                  }
                }
              }
            ]
            """);

        Assertions.assertEquals(
            "sum",
            ((Map<?, ?>) ((Map<?, ?>) normalized.get(0).get("params")).get("actions")).get("method")
        );
    }

    @Test
    void normalizesAggregateStringFieldsToCanonicalLists() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "aggregate",
                "params": {
                  "by": "Client Account, Contract",
                  "actions": {
                    "method": "sum",
                    "on": "Quantity",
                    "rename": ""
                  }
                }
              }
            ]
            """);

        Map<?, ?> params = (Map<?, ?>) normalized.get(0).get("params");
        Map<?, ?> actions = (Map<?, ?>) params.get("actions");
        Assertions.assertEquals(List.of("Client Account", "Contract"), params.get("by"));
        Assertions.assertEquals(List.of("Quantity"), actions.get("on"));
        Assertions.assertEquals(List.of(), actions.get("rename"));
    }
}
