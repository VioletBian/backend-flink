# backend-flink

`backend-flink` is the Java-side parity backend for the shared JSON operator pipeline.

## Current scope

- Exposes the same REST routes as the Python backend: `/dp/pipeline/save`, `/dp/pipeline/get`, `/dp/pipeline/versions`, `/dp/pipeline/run`.
- Normalizes legacy JSON field aliases into the canonical DSL before saving or running.
- Builds a stage plan with row-split / column-split eligibility and native-vs-fallback markers.
- Uses a local Python reference runner as the execution bridge today so result semantics stay aligned while the Flink-native path is expanded.

## Runtime defaults

- Port: `8004`
- Mongo collection: `pipelines`
- Python bridge command: `python3 ../backend/scripts/reference_runner_cli.py`

## Build

```bash
mvn spring-boot:run
```

## Notes

- The Maven project targets Java 17.
- The local Codex environment used for this change only has Java 8 installed, so the project structure and tests were added but not compiled locally here.
- Detailed implementation notes, current gaps, and operator-level parity caveats are documented in `优化说明.md`.
