# vegafusion-wasm

WebAssembly bindings for browser usage.

## Architecture
Implements `VegaFusionRuntimeTrait` by delegating to JavaScript functions.
Uses async channels and `JsFuture` for promise-based communication.

## Build
```bash
pixi run install-wasm-toolchain  # One-time
pixi run build-wasm
```
Uses `release-small` profile for size optimization.

## Patterns
- `wasm_bindgen_futures::spawn_local` for background tasks
- Proto serialization via prost for message encoding

## Pitfalls
- Single-threaded execution model
- JavaScript async errors can panic WASM runtime
- Message size limits for proto encoding
- MUST use `http-wasm` feature, NOT `http`
