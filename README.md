# backend-flink

`backend-flink` 是当前 pipeline `/run` 的 Java/Flink 备用执行后端。

## 当前范围

- 对外重点对齐 `/dp/pipeline/run`
- `/run` 接口参数与 Python backend 一致：
  - `file`
  - `pipeline_json`
  - `enableParallel`
  - 可选 query `debug=true`
- 运行时会在 Java 侧临时 enrich step 顶层 `execution`，执行结束即丢弃，不污染保存态 pipeline JSON
- 当前已实现 Java runner / planner / segment execute 链路
- 当前原生执行已经覆盖一批“可静态证明安全”的 native 子集：
  - `filter`（简单条件表达式）
  - `rename`
  - `tag`（简单条件表达式，first-match）
  - `constant`（支持 `rows + columns`）
  - `value_mapping`（支持 `replace/map`，支持 `rows + columns`）
  - `col_assign`（仅 `method=vectorized` 且表达式可静态解析）
  - `sort`
  - `aggregate`（`mean/count/sum/max/min/std`）
  - `formatter`
  - `date_formatter`
- 其他算子或不安全 stage 会自动回退到 Python reference runner
  - `col_apply`
  - `col_assign.lambda`
  - 复杂 `filter` / `tag` / `col_assign.vectorized` 表达式
  - `series_transform`（当前仍走 Python）

## 运行默认值

- 端口：`8004`
- Python bridge command：`python3 backend/scripts/reference_runner_cli.py`
- 开发态 CORS 默认放开 `localhost/127.0.0.1` 的各端口，可在 `backend.flink.cors.allowed-origin-patterns` 调整

## 构建

```bash
./gradlew test
./gradlew bootRun
```

## 说明

- 当前以 Gradle wrapper 作为标准构建入口，已对齐到 `/Users/fortunebian/Downloads/repos/backend-flink` 的 wrapper 体系。
- 工程目标 Java 版本为 `21`。
- 当前 Gradle wrapper 为 `8.7`，运行 `./gradlew` 也需要本机 `JAVA_HOME` 指向 JDK 17+；如果要和工程目标保持一致，建议直接切到 JDK 21。
- 更完整的实现细节、并行规划逻辑、当前原生支持边界和 fallback 算子说明见 [优化说明.md](/Users/fortunebian/Desktop/data-processor/backend-flink/优化说明.md)。
