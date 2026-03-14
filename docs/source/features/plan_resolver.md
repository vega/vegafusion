# Plan Resolver

PlanResolver lets you connect custom data sources to VegaFusion. Use it when data lives in an external system (Spark, Snowflake, DuckDB, a custom API) and you want to push computation there instead of pulling it all into memory. For data you already have in Python as DataFrames or Arrow tables, [inline datasets](./inline_datasets.md) are simpler.

:::{note}
`resolve_table`, `resolve_plan_proto` (bytes variant), and `unparse_to_sql` with bytes require no additional dependencies beyond `vegafusion`.

`scan_url`, `resolve_plan` (deserialized `LogicalPlanNode`), `external_table_scan_node`, and `inline_table_scan_node` require the protobuf package:

```
pip install vegafusion[plan-resolver]
```
:::

## Python

Override one of these methods on `PlanResolver` (simplest first):

- `resolve_table`: return data for each external table independently. The default `resolve_plan` walks the plan and calls this for every external table.
- `resolve_plan` / `resolve_plan_proto`: receive the entire logical plan. Overriding this supersedes `resolve_table` since the runtime calls `resolve_plan` directly; `resolve_table` is only reached via the default implementation.

### resolve_table

```python
class TableResolver(PlanResolver):
    def __init__(self, table):
        self._table = table

    def resolve_table(self, name, scheme, schema, metadata=None,
                      projected_columns=None):
        return self._table

source = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})
ext = ExternalDataset(scheme="custom", schema=source.schema, data=source)
resolver = TableResolver(source)

rt = vf.VegaFusionRuntime(plan_resolver=resolver)
datasets, _ = rt.pre_transform_datasets(
    spec, datasets=["filtered"],
    inline_datasets={"source": ext}, dataset_format="pyarrow",
)
```

VegaFusion calls `resolve_table` to get the data, then applies Vega transforms (filter, aggregate, etc.) via DataFusion. No protobuf dependency is needed.

See [plan_resolver_basic.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/plan_resolver_basic.py) for a complete example.

### capabilities + scan_url

For custom URL schemes in Vega specs (e.g. `"url": "mydata://database/sales"`), override `capabilities()` and `scan_url()`:

```python
class SalesResolver(PlanResolver):
    def capabilities(self):
        return {"supported_schemes": ["mydata"]}

    def scan_url(self, parsed_url):
        if parsed_url["scheme"] == "mydata":
            schema = pa.schema([("product", pa.utf8()), ("revenue", pa.int64())])
            return external_table_scan_node(
                table_name="sales_data", schema=schema, scheme="mydata",
            )
        return None  # pass to next resolver

    def resolve_table(self, name, scheme, schema, metadata=None,
                      projected_columns=None):
        return pa.table({"product": ["Widget", "Gadget"], "revenue": [1200, 3400]})
```

`capabilities()` tells the planner that `mydata://` URLs are supported. `scan_url()` creates an `ExternalTableProvider` plan node, and `resolve_table()` provides the data at execution time.

See [plan_resolver_url_scanning.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/plan_resolver_url_scanning.py) for a complete example.

### resolve_plan + unparse_to_sql

Override `resolve_plan_proto` to receive the serialized logical plan. Use `unparse_to_sql()` to convert it to SQL:

```python
class SqlResolver(PlanResolver):
    def resolve_plan_proto(self, plan_bytes, datasets):
        sql = unparse_to_sql(plan_bytes, dialect="postgres")
        # Execute SQL against your database and return the result
        return execute_query(sql)
```

`resolve_plan_proto` receives protobuf bytes that can be passed directly to `unparse_to_sql()` without deserialization. To inspect or modify the plan tree, use `resolve_plan()` instead (it receives a deserialized `LogicalPlanNode`).

Supported SQL dialects: `"default"`, `"postgres"`, `"mysql"`, `"sqlite"`, `"duckdb"`, `"bigquery"`.

See [plan_resolver_sql.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/plan_resolver_sql.py) for a complete example.

`PlanResolver` cannot be used with `grpc_connect()` (resolvers run in-process). Set `thread_safe = False` for backends with thread-affine connections (e.g. DuckDB). Set `skip_when_no_external_tables = False` to receive all plans (e.g. for logging). The `capabilities()` dict also accepts `supports_arrow_tables: True` to let the runtime eagerly materialize plans into Arrow tables.

### API Reference

```{eval-rst}
.. autoclass:: vegafusion.PlanResolver
   :members:

.. autoclass:: vegafusion.ExternalDataset
   :members:

.. autofunction:: vegafusion.plan_resolver.external_table_scan_node

.. autofunction:: vegafusion.plan_resolver.unparse_to_sql

.. autofunction:: vegafusion.plan_resolver.inline_table_scan_node
```

## Rust

The `PlanResolver` trait in `vegafusion-runtime` provides the same two-phase architecture (scan_url at planning time, resolve_table/resolve_plan at execution time).

See [custom_resolver.rs](https://github.com/vega/vegafusion/tree/main/examples/rust-examples/examples/custom_resolver.rs) for a working example, and the [vegafusion-runtime docs on docs.rs](https://docs.rs/vegafusion-runtime/) for the full API.
