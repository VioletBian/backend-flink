package com.dataprocessor.flink.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.planner.ExecutionConfig;
import com.dataprocessor.flink.planner.OperationSpec;
import com.dataprocessor.flink.planner.StagePlan;
import com.dataprocessor.flink.runtime.RowExpressionEvaluator;
import com.dataprocessor.flink.runtime.RuntimeRow;
import com.dataprocessor.flink.runtime.RuntimeTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NativeStageExecutorTest {

    @Test
    void executesSimpleFilterWithoutPythonFallback() {
        NativeStageExecutor executor = createExecutor();

        OperationSpec filterSpec = new OperationSpec(
            "filter",
            Map.of("requiredCols", List.of("Client Account", "Price")),
            "`Price` > 10",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        RuntimeTable resultTable = executor.execute(buildSampleTable(), buildStagePlan(List.of(filterSpec), List.of("filter")));

        Assertions.assertEquals(List.of("Client Account", "Price"), resultTable.getColumns());
        Assertions.assertEquals(1, resultTable.getRows().size());
        Assertions.assertEquals("7001", resultTable.getRows().get(0).getValues().get("Client Account"));
    }

    @Test
    void executesTagWithFirstMatchSemantics() {
        NativeStageExecutor executor = createExecutor();

        OperationSpec tagSpec = new OperationSpec(
            "tag",
            Map.of(
                "conditions", List.of("`Price` >= 10", "`Client Account` == '7002'"),
                "tag_col_name", "Alert",
                "tags", List.of("HIGH", "SECOND"),
                "default_tag", "OTHER"
            ),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        RuntimeTable resultTable = executor.execute(buildSampleTable(), buildStagePlan(List.of(tagSpec), List.of("tag")));

        Assertions.assertEquals(List.of("Client Account", "Price", "Alert"), resultTable.getColumns());
        Assertions.assertEquals("HIGH", resultTable.getRows().get(0).getValues().get("Alert"));
        Assertions.assertEquals("SECOND", resultTable.getRows().get(1).getValues().get("Alert"));
    }

    @Test
    void executesTagWithSingleAmpersandCondition() {
        NativeStageExecutor executor = createExecutor();

        RuntimeTable resultTable = executor.execute(
            buildSampleTable(),
            buildStagePlan(
                List.of(new OperationSpec(
                    "tag",
                    Map.of(
                        "conditions", List.of("Price >= 10 & `Client Account` == '7001'", "Price >= 9"),
                        "tag_col_name", "Alert",
                        "tags", List.of("HIGH", "WATCH"),
                        "default_tag", "OTHER"
                    ),
                    "index > -1",
                    0,
                    new ExecutionConfig(ExecutionConfig.SERIAL, null)
                )),
                List.of("tag")
            )
        );

        Assertions.assertEquals(List.of("Client Account", "Price", "Alert"), resultTable.getColumns());
        Assertions.assertEquals("HIGH", resultTable.getRows().get(0).getValues().get("Alert"));
        Assertions.assertEquals("WATCH", resultTable.getRows().get(1).getValues().get("Alert"));
    }

    @Test
    void executesVectorizedColAssignAndNullsUnmatchedRows() {
        NativeStageExecutor executor = createExecutor();

        OperationSpec assignSpec = new OperationSpec(
            "col_assign",
            Map.of(
                "method", "vectorized",
                "col_name", "Scaled",
                "value_expr", "`Price` * 2"
            ),
            "`Client Account` == '7001'",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        RuntimeTable resultTable = executor.execute(buildSampleTable(), buildStagePlan(List.of(assignSpec), List.of("col_assign")));

        Assertions.assertEquals(List.of("Client Account", "Price", "Scaled"), resultTable.getColumns());
        Assertions.assertEquals(
            22.0,
            ((Number) resultTable.getRows().get(0).getValues().get("Scaled")).doubleValue()
        );
        Assertions.assertNull(resultTable.getRows().get(1).getValues().get("Scaled"));
    }

    @Test
    void executesConstantOnColumnStageAndAppendsColumnsInSpecOrder() {
        NativeStageExecutor executor = createExecutor();
        OperationSpec constantSpec = new OperationSpec(
            "constant",
            Map.of(
                "values", new LinkedHashMap<>(Map.of("Desk", "SH", "Region", "APAC")),
                "columns", List.of("Desk", "Region")
            ),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.COLUMNS, 2)
        );

        RuntimeTable resultTable = executor.execute(
            buildSampleTable(),
            new StagePlan(
                0,
                ExecutionConfig.COLUMNS,
                List.of(0),
                List.of("constant"),
                List.of(constantSpec),
                2,
                true,
                null
            )
        );

        Assertions.assertEquals(List.of("Client Account", "Price", "Desk", "Region"), resultTable.getColumns());
        Assertions.assertEquals("SH", resultTable.getRows().get(0).getValues().get("Desk"));
        Assertions.assertEquals("APAC", resultTable.getRows().get(1).getValues().get("Region"));
    }

    @Test
    void executesValueMappingInBothReplaceAndMapModes() {
        NativeStageExecutor executor = createExecutor();
        RuntimeTable source = new RuntimeTable(
            List.of("Client Account", "Desk"),
            List.of(
                new RuntimeRow(0L, new LinkedHashMap<>(Map.of("Client Account", "7001", "Desk", "SH"))),
                new RuntimeRow(1L, new LinkedHashMap<>(Map.of("Client Account", "7002", "Desk", "LDN")))
            )
        );

        OperationSpec replaceSpec = new OperationSpec(
            "value_mapping",
            Map.of(
                "mode", "replace",
                "mappings", new LinkedHashMap<>(Map.of(
                    "Desk", new LinkedHashMap<>(Map.of("SH", "Shanghai"))
                )),
                "default", "",
                "columns", List.of("Desk"),
                "result_columns", List.of("Desk")
            ),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );
        OperationSpec mapSpec = new OperationSpec(
            "value_mapping",
            Map.of(
                "mode", "map",
                "mappings", new LinkedHashMap<>(Map.of(
                    "Client Account", new LinkedHashMap<>(Map.of("7001", "VIP")),
                    "Desk", new LinkedHashMap<>(Map.of("SH", "CN"))
                )),
                "default", "NORMAL",
                "columns", List.of("Client Account", "Desk"),
                "result_columns", List.of("Client Account_mapped", "Desk_mapped")
            ),
            "index > -1",
            1,
            new ExecutionConfig(ExecutionConfig.COLUMNS, 2)
        );

        RuntimeTable replaced = executor.execute(source, buildStagePlan(List.of(replaceSpec), List.of("value_mapping")));
        RuntimeTable mapped = executor.execute(
            source,
            new StagePlan(
                0,
                ExecutionConfig.COLUMNS,
                List.of(1),
                List.of("value_mapping"),
                List.of(mapSpec),
                2,
                true,
                null
            )
        );

        Assertions.assertEquals("Shanghai", replaced.getRows().get(0).getValues().get("Desk"));
        Assertions.assertEquals("LDN", replaced.getRows().get(1).getValues().get("Desk"));
        Assertions.assertEquals(List.of("Client Account", "Desk", "Client Account_mapped", "Desk_mapped"), mapped.getColumns());
        Assertions.assertEquals("VIP", mapped.getRows().get(0).getValues().get("Client Account_mapped"));
        Assertions.assertEquals("NORMAL", mapped.getRows().get(1).getValues().get("Client Account_mapped"));
        Assertions.assertEquals("CN", mapped.getRows().get(0).getValues().get("Desk_mapped"));
        Assertions.assertEquals("NORMAL", mapped.getRows().get(1).getValues().get("Desk_mapped"));
    }

    @Test
    void executesStableSortWithoutResettingIndexIdentity() {
        NativeStageExecutor executor = createExecutor();
        RuntimeTable unsortedTable = new RuntimeTable(
            List.of("Client Account", "Price"),
            List.of(
                new RuntimeRow(10L, new LinkedHashMap<>(Map.of("Client Account", "C", "Price", 5L))),
                new RuntimeRow(20L, new LinkedHashMap<>(Map.of("Client Account", "A", "Price", 5L))),
                new RuntimeRow(30L, new LinkedHashMap<>(Map.of("Client Account", "B", "Price", 3L)))
            )
        );
        OperationSpec sortSpec = new OperationSpec(
            "sort",
            Map.of("by", List.of("Price"), "ascending", List.of(true)),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        RuntimeTable resultTable = executor.execute(unsortedTable, buildStagePlan(List.of(sortSpec), List.of("sort")));

        Assertions.assertEquals(List.of(30L, 10L, 20L), resultTable.getRows().stream().map(RuntimeRow::getRowId).toList());
    }

    @Test
    void raisesStepScopedErrorWhenSortColumnIsMissing() {
        NativeStageExecutor executor = createExecutor();
        OperationSpec sortSpec = new OperationSpec(
            "sort",
            Map.of("by", List.of("Missing Column"), "ascending", List.of(true)),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> executor.execute(buildSampleTable(), buildStagePlan(List.of(sortSpec), List.of("sort")))
        );

        Assertions.assertEquals("Step 1 (sort) failed: Column `Missing Column` does not exist.", exception.getMessage());
    }

    @Test
    void executesRowsStageWithoutNonParallelSourceFailure() {
        NativeStageExecutor executor = new NativeStageExecutor(new BatchTableEnvironmentFactory(), new RowExpressionEvaluator());
        OperationSpec formatterSpec = new OperationSpec(
            "formatter",
            Map.of(
                "method", "StringPrefix",
                "columns", List.of("Client Account"),
                "value_expr", "acct_"
            ),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.ROWS, 2)
        );

        RuntimeTable resultTable = executor.execute(
            buildSampleTable(),
            new StagePlan(
                0,
                ExecutionConfig.ROWS,
                List.of(0),
                List.of("formatter"),
                List.of(formatterSpec),
                2,
                true,
                null
            )
        );

        Assertions.assertEquals("acct_7001", resultTable.getRows().get(0).getValues().get("Client Account"));
        Assertions.assertEquals("acct_7002", resultTable.getRows().get(1).getValues().get("Client Account"));
    }

    @Test
    void executesRowsFilterStageWithoutSerializableClosureFailure() {
        NativeStageExecutor executor = new NativeStageExecutor(new BatchTableEnvironmentFactory(), new RowExpressionEvaluator());
        OperationSpec filterSpec = new OperationSpec(
            "filter",
            Map.of("requiredCols", List.of("Client Account", "Price")),
            "`Price` > 10",
            0,
            new ExecutionConfig(ExecutionConfig.ROWS, 2)
        );

        RuntimeTable resultTable = executor.execute(
            buildSampleTable(),
            new StagePlan(
                0,
                ExecutionConfig.ROWS,
                List.of(0),
                List.of("filter"),
                List.of(filterSpec),
                2,
                true,
                null
            )
        );

        Assertions.assertEquals(List.of("Client Account", "Price"), resultTable.getColumns());
        Assertions.assertEquals(1, resultTable.getRows().size());
        Assertions.assertEquals("7001", resultTable.getRows().get(0).getValues().get("Client Account"));
    }

    @Test
    void executesAggregateWithRenameAndResetsIndex() {
        NativeStageExecutor executor = createExecutor();
        RuntimeTable aggregateSource = new RuntimeTable(
            List.of("Client Account", "Price"),
            List.of(
                new RuntimeRow(10L, new LinkedHashMap<>(Map.of("Client Account", "A", "Price", 10L))),
                new RuntimeRow(20L, new LinkedHashMap<>(Map.of("Client Account", "A", "Price", 15L))),
                new RuntimeRow(30L, new LinkedHashMap<>(Map.of("Client Account", "B", "Price", 7L)))
            )
        );
        OperationSpec aggregateSpec = new OperationSpec(
            "aggregate",
            Map.of(
                "by", List.of("Client Account"),
                "actions", Map.of("method", "sum", "on", List.of("Price"), "rename", List.of("Total Price"))
            ),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        RuntimeTable resultTable = executor.execute(aggregateSource, buildStagePlan(List.of(aggregateSpec), List.of("aggregate")));

        Assertions.assertEquals(List.of("Client Account", "Total Price"), resultTable.getColumns());
        Assertions.assertEquals(List.of(0L, 1L), resultTable.getRows().stream().map(RuntimeRow::getRowId).toList());
        Assertions.assertEquals(25L, resultTable.getRows().get(0).getValues().get("Total Price"));
        Assertions.assertEquals(7L, resultTable.getRows().get(1).getValues().get("Total Price"));
    }

    @Test
    void executesAggregateCountOnGroupedQuantityColumn() {
        NativeStageExecutor executor = createExecutor();
        RuntimeTable aggregateSource = new RuntimeTable(
            List.of("Client Account", "Contract", "Quantity"),
            List.of(
                new RuntimeRow(10L, new LinkedHashMap<>(Map.of("Client Account", "A", "Contract", "C1", "Quantity", 10L))),
                new RuntimeRow(20L, new LinkedHashMap<>(Map.of("Client Account", "A", "Contract", "C1", "Quantity", 20L))),
                new RuntimeRow(30L, new LinkedHashMap<>(Map.of("Client Account", "A", "Contract", "C2", "Quantity", 30L))),
                new RuntimeRow(40L, new LinkedHashMap<>(Map.of("Client Account", "B", "Contract", "C1", "Quantity", 40L)))
            )
        );
        OperationSpec aggregateSpec = new OperationSpec(
            "aggregate",
            Map.of(
                "by", List.of("Client Account", "Contract"),
                "actions", Map.of("method", "count", "on", List.of("Quantity"))
            ),
            "index > -1",
            0,
            new ExecutionConfig(ExecutionConfig.SERIAL, null)
        );

        RuntimeTable resultTable = executor.execute(aggregateSource, buildStagePlan(List.of(aggregateSpec), List.of("aggregate")));

        Assertions.assertEquals(List.of("Client Account", "Contract", "Quantity"), resultTable.getColumns());
        Assertions.assertEquals(List.of(0L, 1L, 2L), resultTable.getRows().stream().map(RuntimeRow::getRowId).toList());
        Assertions.assertEquals(2L, resultTable.getRows().get(0).getValues().get("Quantity"));
        Assertions.assertEquals(1L, resultTable.getRows().get(1).getValues().get("Quantity"));
        Assertions.assertEquals(1L, resultTable.getRows().get(2).getValues().get("Quantity"));
    }

    @Test
    void rowsStrategyMatchesSerialForLongRowLocalPipeline() {
        NativeStageExecutor executor = createExecutor();
        RuntimeTable source = buildLongRowsTable(5000);
        List<OperationSpec> specs = List.of(
            new OperationSpec(
                "filter",
                Map.of(
                    "requiredCols",
                    List.of(
                        "account_id",
                        "qty",
                        "price",
                        "fee",
                        "metric_0",
                        "metric_1",
                        "metric_2",
                        "metric_3",
                        "trade_date_local",
                        "settle_date_local"
                    )
                ),
                "metric_0 >= 0",
                0,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            ),
            new OperationSpec(
                "tag",
                Map.of(
                    "tag_col_name", "risk_bucket",
                    "conditions", List.of("metric_1 % 2 == 0", "metric_2 % 5 == 0"),
                    "tags", List.of("even", "penta"),
                    "default_tag", "base"
                ),
                "index > -1",
                1,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            ),
            new OperationSpec(
                "col_assign",
                Map.of("method", "vectorized", "col_name", "notional", "value_expr", "qty * price + fee"),
                "index > -1",
                2,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            ),
            new OperationSpec(
                "col_assign",
                Map.of("method", "vectorized", "col_name", "risk_score", "value_expr", "notional - metric_3 * 13"),
                "index > -1",
                3,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            ),
            new OperationSpec(
                "date_formatter",
                Map.of(
                    "columns", List.of("trade_date_local", "settle_date_local"),
                    "from_format", "yyyyMMdd",
                    "to_format", "UTC",
                    "from_timezone", "Asia/Shanghai",
                    "to_timezone", "UTC"
                ),
                "index > -1",
                4,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            ),
            new OperationSpec(
                "formatter",
                Map.of("method", "DecimalScale", "columns", List.of("notional", "risk_score"), "value_expr", 2),
                "index > -1",
                5,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            ),
            new OperationSpec(
                "formatter",
                Map.of("method", "StringPrefix", "columns", List.of("risk_bucket"), "value_expr", "bucket_"),
                "index > -1",
                6,
                new ExecutionConfig(ExecutionConfig.SERIAL, null)
            )
        );

        List<Integer> logicalStepIndexes = List.of(0, 1, 2, 3, 4, 5, 6);
        List<String> operationTypes = List.of("filter", "tag", "col_assign", "col_assign", "date_formatter", "formatter", "formatter");

        RuntimeTable serialResult = executor.execute(
            source,
            new StagePlan(0, ExecutionConfig.SERIAL, logicalStepIndexes, operationTypes, specs, 1, true, null)
        );
        RuntimeTable rowsResult = executor.execute(
            source,
            new StagePlan(0, ExecutionConfig.ROWS, logicalStepIndexes, operationTypes, specs, 4, true, null)
        );

        List<RuntimeRow> serialRows = serialResult.getRows();
        List<RuntimeRow> parallelRows = rowsResult.getRows();
        Assertions.assertEquals(serialResult.getColumns(), rowsResult.getColumns());
        Assertions.assertEquals(serialRows.size(), parallelRows.size());
        for (int index = 0; index < serialRows.size(); index++) {
            Assertions.assertEquals(serialRows.get(index).getRowId(), parallelRows.get(index).getRowId());
            Assertions.assertEquals(serialRows.get(index).getValues(), parallelRows.get(index).getValues());
        }
    }

    private NativeStageExecutor createExecutor() {
        return new NativeStageExecutor(
            Mockito.mock(BatchTableEnvironmentFactory.class),
            new RowExpressionEvaluator()
        );
    }

    private RuntimeTable buildSampleTable() {
        return new RuntimeTable(
            List.of("Client Account", "Price"),
            List.of(
                new RuntimeRow(0L, new LinkedHashMap<>(Map.of("Client Account", "7001", "Price", 11L))),
                new RuntimeRow(1L, new LinkedHashMap<>(Map.of("Client Account", "7002", "Price", 9L)))
            )
        );
    }

    private RuntimeTable buildLongRowsTable(int rowCount) {
        int[] tradeDates = {20250106, 20250113, 20250120, 20250127, 20250210, 20250217};
        int[] settleDates = {20250107, 20250114, 20250121, 20250128, 20250211, 20250218};
        List<RuntimeRow> rows = new ArrayList<>();
        for (int index = 0; index < rowCount; index++) {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            values.put("account_id", 9_000_000_000L + (index % 50_000));
            values.put("qty", (long) ((index % 97) + 1));
            values.put("price", ((index * 13L) % 10_000) / 37.0 + 100.0);
            values.put("fee", (long) ((index % 17) + 1));
            values.put("metric_0", (long) (index % 97));
            values.put("metric_1", (long) ((index * 7L) % 10_003));
            values.put("metric_2", (long) ((index * 11L) % 9_973));
            values.put("metric_3", (long) ((index * 17L) % 8_191));
            values.put("trade_date_local", (long) tradeDates[index % tradeDates.length]);
            values.put("settle_date_local", (long) settleDates[index % settleDates.length]);
            rows.add(new RuntimeRow(index, values));
        }
        return new RuntimeTable(
            List.of(
                "account_id",
                "qty",
                "price",
                "fee",
                "metric_0",
                "metric_1",
                "metric_2",
                "metric_3",
                "trade_date_local",
                "settle_date_local"
            ),
            rows
        );
    }

    private StagePlan buildStagePlan(List<OperationSpec> specs, List<String> operationTypes) {
        return new StagePlan(
            0,
            ExecutionConfig.SERIAL,
            List.of(0),
            operationTypes,
            specs,
            1,
            true,
            null
        );
    }
}
