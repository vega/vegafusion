# vegafusion-python

Python bindings via PyO3 and maturin.

## Structure
Single `lib.rs` with PyChartState class and supporting functions.

## Build
```bash
pixi run dev-py  # Builds with release profile
```
Must rebuild after any Rust changes.

## Patterns

### GIL Management
CRITICAL: Always release GIL for blocking operations:

```rust
py.allow_threads(|| {
    tokio_runtime.block_on(async_operation())
})
```

Forgetting `py.allow_threads()` blocks all Python threads.

### Tokio Runtime
Single static runtime shared across calls. Uses custom stack size for deep recursion.

### Data Marshalling
- Python → Rust: `depythonize` (serde)
- Rust → Python: `pythonize` (serde)
- Arrow: `pyo3-arrow` crate

## Pitfalls
- PyErr conversions may lose Rust error context
- "Symbol not found": Stale build, run `pixi run dev-py`
- PyArrow version must match pyo3-arrow expectations
