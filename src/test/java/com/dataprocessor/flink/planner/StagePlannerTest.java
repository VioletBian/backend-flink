package com.dataprocessor.flink.planner;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StagePlannerTest {

    private final PipelineContractNormalizer normalizer = new PipelineContractNormalizer(new ObjectMapper());
    private final StagePlanner planner = new StagePlanner(new OperatorCapabilityRegistry());

    @Test
    void groupsRowLocalOperatorsAndKeepsAggregateAsBarrier() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "filter",
                "params": {
                  "requiredCols": ["Client Account", "Price"],
                  "condition": "`Client Account` > 7000000000"
                }
              },
              {
                "type": "formatter",
                "params": {
                  "method": "DecimalScale",
                  "columns": ["Price"],
                  "value_expr": "2"
                }
              },
              {
                "type": "aggregate",
                "params": {
                  "by": ["Client Account"],
                  "actions": {
                    "method": "sum",
                    "on": ["Price"]
                  }
                }
              }
            ]
            """);

        PipelinePlan plan = planner.plan(normalized);

        Assertions.assertEquals(2, plan.getStages().size());
        Assertions.assertEquals("row-and-column-splittable", plan.getStages().get(0).getOptimizationHint());
        Assertions.assertEquals("two-phase-aggregate", plan.getStages().get(1).getOptimizationHint());
    }

    @Test
    void sendsLambdaStyleOperationsToPythonFallback() {
        List<Map<String, Object>> normalized = normalizer.normalizeJson("""
            [
              {
                "type": "col_assign",
                "params": {
                  "method": "lambda",
                  "col_name": "TradeAmount",
                  "value_expr": "lambda x: x['Quantity'] * x['Price']"
                }
              }
            ]
            """);

        PipelinePlan plan = planner.plan(normalized);

        Assertions.assertEquals("PYTHON_FALLBACK", plan.getOperators().get(0).getExecutionMode());
        Assertions.assertEquals("python-reference-barrier", plan.getStages().get(0).getOptimizationHint());
    }
}
