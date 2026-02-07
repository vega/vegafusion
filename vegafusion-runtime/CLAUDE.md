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

## Arrow String Type Handling

Arrow has three string types that must be treated equivalently:
- `DataType::Utf8` / `ScalarValue::Utf8` - Standard UTF-8 strings
- `DataType::LargeUtf8` / `ScalarValue::LargeUtf8` - UTF-8 with 64-bit offsets
- `DataType::Utf8View` / `ScalarValue::Utf8View` - View-based strings (used by Polars)

**When checking DataType:**
```rust
use vegafusion_common::datatypes::is_string_datatype;
if is_string_datatype(&dtype) { ... }
```

**When pattern matching ScalarValue:**
```rust
match value {
    ScalarValue::Utf8(Some(s))
    | ScalarValue::LargeUtf8(Some(s))
    | ScalarValue::Utf8View(Some(s)) => { ... }
    _ => { ... }
}
```

**When downcasting ArrayRef:**
```rust
match array.data_type() {
    DataType::Utf8 => array.as_any().downcast_ref::<StringArray>(),
    DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>(),
    DataType::Utf8View => array.as_any().downcast_ref::<StringViewArray>(),
    _ => ...
}
```

Polars uses Utf8View by default. Failing to handle all three types will cause runtime errors with Polars DataFrames.

## Pitfalls
- Async recursion can hit stack limits with deep task graphs
- Column escaping must match vegafusion-core exactly
- DataFrame schema mutations need careful handling
- Timezone: UTC vs local datetime conversions are complex
- **String types**: Always handle Utf8, LargeUtf8, and Utf8View equivalently (see above)
