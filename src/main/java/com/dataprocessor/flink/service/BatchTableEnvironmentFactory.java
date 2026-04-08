package com.dataprocessor.flink.service;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.springframework.stereotype.Component;

@Component
public class BatchTableEnvironmentFactory {

    // 预留 Flink Table API batch-mode 入口，后续 native stage 真正落地时直接复用这里的环境创建逻辑。
    public TableEnvironment createBatchTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        return TableEnvironment.create(settings);
    }
}
