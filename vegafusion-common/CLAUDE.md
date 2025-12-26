# vegafusion-common

Shared utilities and data structures used across all VegaFusion crates.

## Key Modules
- `error.rs`: VegaFusionError with ErrorContext chain
- `data/table.rs`: VegaFusionTable wrapping Arrow RecordBatch
- `column.rs`: DataFusion column helpers (flat_col, unescaped_col)

## Error Handling Pattern

```rust
use vegafusion_common::error::ResultWithContext;
result.with_context(|| "Description of what was attempted")?
```

Use lazy closures (`|| "message"`) to avoid string allocation on success path.

The `ErrorContext` accumulates a stack of context strings for debugging.

## Feature Flags
- `py`: PyO3 support for Python bindings
- `json`: JSON serialization via serde_json
- `proto`: Protocol buffer support
- `base64`, `object_store`, `url`: Storage backends

## Pitfalls
- Column escaping must be consistent with vegafusion-core
- Error `duplicate()` converts to string, losing original type info
