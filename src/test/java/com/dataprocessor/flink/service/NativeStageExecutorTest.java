package com.dataprocessor.flink.service;

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
