# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

VegaFusion is a Rust-based library that provides Python, JavaScript, and Rust APIs for analyzing and scaling Vega visualizations. The project optimizes Vega specifications by pre-computing data transformations and pushing computations to more efficient backends like Apache DataFusion.

## Architecture

The project is structured as a Rust workspace with multiple crates:

- **vegafusion-common**: Shared data structures and utilities (table handling, errors, columns)
- **vegafusion-core**: Core logic for parsing, planning, and transforming Vega specifications  
- **vegafusion-runtime**: Runtime implementation with DataFusion backend for executing transforms
- **vegafusion-server**: gRPC server for remote execution
- **vegafusion-python**: Python bindings using PyO3 and maturin
- **vegafusion-wasm**: WebAssembly bindings for browser usage

### Key Components

- **Spec parsing**: Vega/Vega-Lite specification parsing and validation
- **Planning**: Dependency graph analysis and optimization planning
- **Transform compilation**: Transforms Vega data transforms into DataFusion logical plans
- **Chart state management**: Manages interactive chart state and updates
- **Pre-transformation**: Optimizes specs by executing transforms ahead of time

## Development Environment

The development environment uses:
- **Rust**: Must be installed separately via [rustup.rs](https://rustup.rs/)
- **Pixi**: Manages all other dependencies including Python, Node.js, and system packages

### Setup Commands

```bash
# Install Rust (one-time setup)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install Pixi (one-time setup)
curl -fsSL https://pixi.sh/install.sh | bash

# Start MinIO server (for S3 testing)
pixi run start-minio
```

## Common Development Tasks

### Rust Development

```bash
# Format all Rust code
pixi run fmt-rs

# Check formatting without changes
pixi run check-rs-fmt

# Check for warnings
pixi run check-rs-warnings

# Run clippy linter
pixi run check-rs-clippy

# Test individual crates
pixi run test-rs-core
pixi run test-rs-runtime
pixi run test-rs-server

# Test all Rust crates
pixi run test-rs

# Build optimized server binary
pixi run build-rs-server

# Run the server
pixi run run-rs-server
```

### Python Development

```bash
# Install Python package in development mode
pixi run dev-py

# Test Python package
pixi run test-py

# Test Python package in headless mode (CI)
pixi run test-py-headless

# Format Python code
pixi run fmt-py

# Lint and format Python code
pixi run lint-fix-py

# Check linting without changes
pixi run lint-check-py

# Type check Python code
pixi run type-check-py

# Build Python wheel
pixi run build-py
```

### Testing

The project uses a VegaJS runtime for integration testing that compares outputs between VegaFusion and the reference Vega implementation. The VegaJS runtime requires Node.js dependencies:

```bash
# VegaJS runtime is built automatically when running test-rs-runtime
pixi run test-rs-runtime
```

### Documentation

```bash
# Build documentation
pixi run docs-build

# Serve documentation locally
pixi run docs-serve

# Clean and rebuild docs
pixi run docs-rebuild

# Publish documentation
pixi run docs-publish
```

### WebAssembly

```bash
# Install WASM toolchain (requires Rust installed)
pixi run install-wasm-toolchain

# Build WASM package
pixi run build-wasm

# Pack WASM for npm
pixi run pack-wasm
```

## Code Organization

### Rust Module Structure

- **vegafusion-core/src/planning/**: Query planning and optimization
- **vegafusion-core/src/expression/**: Expression parsing and evaluation  
- **vegafusion-core/src/transform/**: Vega transform implementations
- **vegafusion-runtime/src/transform/**: DataFusion-based transform execution
- **vegafusion-runtime/src/expression/compiler/**: Expression compilation to DataFusion
- **vegafusion-python/src/lib.rs**: Python API implementation with PyO3

### Test Organization

- **vegafusion-runtime/tests/**: Integration tests with spec files
- **vegafusion-runtime/tests/util/vegajs_runtime/**: VegaJS comparison utilities
- **vegafusion-python/tests/**: Python-specific tests
- **vegafusion-runtime/tests/specs/**: Test specification files (Vega/Vega-Lite)

## Code Style

- **Rust**: Uses `cargo fmt` with default settings, `clippy` for linting
- **Python**: Uses `ruff` for formatting and linting, `mypy` for type checking
- Line length: 88 characters for Python
- Target Python version: 3.9+

## Build System

- **Rust**: Standard Cargo workspace with custom build profiles
- **Python**: Uses maturin for building PyO3 extensions
- **Dependencies**: Managed through pixi.toml with conda-forge packages
- **Profiles**: Multiple release profiles (release, release-opt, release-small)

## Testing Strategy

Tests compare VegaFusion output against reference Vega implementations:
- Image comparison tests for visual output
- JSON comparison for data transforms
- Interactive behavior testing for chart state
- Performance benchmarks for large datasets

The project includes extensive mock data and test specifications covering the Vega/Vega-Lite feature surface.