# vegafusion-runtime

Execution engine using Apache DataFusion for transform evaluation.

## Key Modules
- `transform/`: Async transform implementations
- `expression/compiler/`: Expression → DataFusion Expr compilation
- `task_graph/`: Runtime execution with caching
- `datafusion/`: Custom UDFs, UDAFs, context setup

## Patterns

### Async Transforms
All transforms implement `TransformTrait` with `#[async_trait]`:

```rust
#[async_trait]
impl TransformTrait for FilterType {
    async fn eval(&self, dataframe: DataFrame, config: &CompilationConfig)
        -> Result<(DataFrame, Vec<TaskValue>)>
}
```

### Custom Functions
Vega-specific functions (isfinite, percentile, etc.) implemented as UDFs/UDAFs in `datafusion/`.

## Testing
Tests compare against VegaJS reference:
- Test specs in `tests/specs/`
- VegaJS runtime in `tests/util/vegajs_runtime/`
- Use `#[rstest]` for parameterized tests
- Float tolerance: 1e-6

Run: `pixi run test-rs-runtime`

## Feature Flags

Default: `fs`, `multi-thread`, `rustls-tls`, `s3`, `http`, `parquet`, `proto`, `tonic`

Critical combinations:
- WASM: Use `http-wasm`, NOT `http`
- Server: Needs `proto` + `tonic` (default)
- Python: Needs `py` feature

## Pitfalls
- Async recursion can hit stack limits with deep task graphs
- Column escaping must match vegafusion-core exactly
- DataFrame schema mutations need careful handling
- Timezone: UTC vs local datetime conversions are complex
