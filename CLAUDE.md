# CLAUDE.md

VegaFusion optimizes Vega visualizations by pre-computing data transforms using Apache DataFusion. This enables scalable charting with the Vega ecosystem.

## Architecture

Crate dependency chain:
- vegafusion-common → vegafusion-core → vegafusion-runtime → (python, wasm, server)

DataFusion was chosen for columnar computation and query optimization capabilities.

## Development Setup

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh  # Rust
curl -fsSL https://pixi.sh/install.sh | bash                     # Pixi (manages all else)
```

Key commands: `pixi run test-rs-runtime`, `pixi run dev-py`, `pixi run build-wasm`

## Critical Constraints

IMPORTANT: Feature flag conflicts
- `http` and `http-wasm` are mutually exclusive (use `http-wasm` for WASM builds)
- `tonic` requires `proto` feature

IMPORTANT: Proto changes require Python rebuild
- Proto files in `vegafusion-core/src/proto/` auto-generate during build
- After proto changes: `pixi run dev-py`

IMPORTANT: Error handling
- Use `.with_context(|| "message")` pattern (see vegafusion-common for details)

## Testing

Tests compare VegaFusion output against VegaJS reference implementation.
- Float tolerance: 1e-6
- Test specs: `vegafusion-runtime/tests/specs/`
- VegaJS runtime: `vegafusion-runtime/tests/util/vegajs_runtime/`

## Common Debugging

Proto compilation issues:
- Check stderr for protoc errors
- Verify `protobuf-src` feature if system protoc unavailable

Python "symbol not found":
- Stale build - run `pixi run dev-py`

VegaJS test crashes:
- Check `npm install` completed in `tests/util/vegajs_runtime/`
