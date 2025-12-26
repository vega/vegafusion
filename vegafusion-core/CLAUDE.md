# vegafusion-core

Core logic for parsing Vega specs, expression evaluation, and query planning.

## Key Modules
- `spec/`: Vega/Vega-Lite spec parsing with ChartVisitor
- `expression/`: Expression parser (lexer.rs, parser.rs) with visitor pattern
- `transform/`: Transform trait definitions
- `planning/`: Query optimization (projection pushdown, dependency graphs)
- `proto/`: Protobuf definitions and generation

## Patterns

### Visitor Pattern for ASTs
- `ExpressionVisitor` / `MutExpressionVisitor` for expressions
- `ChartVisitor` for spec traversal
- Used for column analysis, variable tracking

### Transform Pairing
Each transform has:
- Spec definition in `spec/transform/`
- Runtime trait in `transform/`

## Proto Workflow

Proto files in `src/proto/`:
- expression.proto, transforms.proto, tasks.proto
- errors.proto, pretransform.proto, services.proto

Generation happens automatically via build.rs.

When modifying proto:
1. Edit .proto file
2. Run `cargo build -p vegafusion-core`
3. Rebuild dependent crates (especially Python: `pixi run dev-py`)

## Feature Flags
- `tonic_support`: Enables gRPC service generation (needed for server)

## Pitfalls
- Expression parsing is permissive; evaluation is strict about types
- Datetime handling tracks local vs UTC via TzConfig
- Scope arrays represent nested facet/group hierarchy
