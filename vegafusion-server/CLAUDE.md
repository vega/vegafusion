# vegafusion-server

gRPC server binary for remote VegaFusion execution.

## Structure
Minimal Rust code in `main.rs`. Standard Tonic/gRPC setup.

## Build
```bash
pixi run build-rs-server                                        # Development
cargo build -p vegafusion-server --profile release-opt          # Production
```

## CLI
Uses clap for argument parsing. See `--help` for options.

## Feature Flags
- Requires `proto` + `tonic` (default)
- `protobuf-src`: Embedded protobuf compiler (use if system protoc unavailable)

## Pitfalls
- Proto code generation dependencies can be tricky
- Watch for port conflicts in integration tests
