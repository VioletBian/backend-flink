package com.dataprocessor.flink.service;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.springframework.stereotype.Component;

@Component
public class BatchTableEnvironmentFactory {

    // 中文说明：保留 Table API 入口，后续如果要把更多算子下推到 SQL/Table 层，直接复用这里。
    public TableEnvironment createBatchTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        return TableEnvironment.create(settings);
    }

    // 中文说明：当前最小 native 实现以 DataStream batch mode 为主，用它承接行式并行 stage。
    public StreamExecutionEnvironment createBatchStreamExecutionEnvironment(int parallelism) {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment.setParallelism(Math.max(1, parallelism));
        return environment;
    }
}
