# Plan Resolver

PlanResolver lets you connect custom data sources to VegaFusion. Use it when data lives in an external system (Spark, Snowflake, DuckDB, a custom API) and you want to push computation there instead of pulling it all into memory. For data you already have in Python as DataFrames or Arrow tables, [inline datasets](./inline_datasets.md) are simpler.

:::{note}
`resolve_table`, `resolve_plan_proto` (bytes variant), and `unparse_to_sql` with bytes require no additional dependencies beyond `vegafusion`.

`external_table_scan_node`, `inline_table_scan_node`, and `resolve_plan` (deserialized `LogicalPlanNode` variant) require the protobuf package:

```
pip install vegafusion[plan-resolver]
```
:::

## Python

Override one of these methods on `PlanResolver` (simplest first):

- `resolve_table`: return an Arrow table for a single external data source. VegaFusion handles the rest — it applies Vega transforms (filter, aggregate, etc.) via DataFusion after your resolver provides the data.
- `resolve_plan` / `resolve_plan_proto`: evaluate an entire logical plan, or the parts your backend supports. Use this to transpile the plan to SQL and execute it remotely, or to push supported operations to your query engine while letting DataFusion handle the rest.

### scan_url + resolve_table

For custom URL schemes in Vega specs (e.g. `"url": "mydb://warehouse/sales"`), override `scan_url()` and `resolve_table()`:

```python
import vegafusion as vf
from vegafusion import PlanResolver
from vegafusion.plan_resolver import external_table_scan_node

class MyResolver(PlanResolver):
    def scan_url(self, parsed_url):
        if parsed_url["scheme"] != "mydb":
            return None  # pass to next resolver

        # Look up the table schema from your data source.
        # This is called at planning time, so avoid loading data here.
        schema = get_table_schema(parsed_url["path"])

        return external_table_scan_node(
            table_name=parsed_url["url"],
            schema=schema,
            scheme="mydb",
            metadata={"path": parsed_url["path"]},
        )

    def resolve_table(self, name, scheme, schema, metadata=None,
                      projected_columns=None, filters=None):
        # Called at execution time — load the actual data.
        # projected_columns lists only the columns DataFusion needs,
        # so you can avoid reading unnecessary columns.
        return load_table(metadata["path"], columns=projected_columns)
```

`scan_url()` is called at planning time — it inspects the URL and returns an `ExternalTableProvider` plan node with the table's schema. `resolve_table()` is called at execution time to provide the actual data.

Use `base_url` on the runtime to set a base path for relative URLs in Vega specs:

```python
resolver = MyResolver()
rt = vf.VegaFusionRuntime(
    plan_resolver=resolver,
    base_url="mydb://warehouse/",
)

# Vega spec with "url": "sales" resolves to "mydb://warehouse/sales"
```

See [plan_resolver_url_scanning.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/plan_resolver_url_scanning.py) for a complete example.

### resolve_table only

If data comes from `ExternalDataset` inline datasets (not URLs), you only need `resolve_table`:

```python
import vegafusion as vf
from vegafusion import ExternalDataset, PlanResolver

class MyResolver(PlanResolver):
    def resolve_table(self, name, scheme, schema, metadata=None,
                      projected_columns=None, filters=None):
        # Look up data by name from your data source
        df = my_database.query(name, columns=projected_columns)
        return df.to_arrow()

ext = ExternalDataset(scheme="mydb", schema=table.schema, data=table)
rt = vf.VegaFusionRuntime(plan_resolver=MyResolver())
datasets, _ = rt.pre_transform_datasets(
    spec, datasets=["result"],
    inline_datasets={"source": ext}, dataset_format="pyarrow",
)
```

No protobuf dependency is needed for this pattern.

### resolve_plan + unparse_to_sql

Override `resolve_plan_proto` to receive the full logical plan and transpile it to SQL for remote execution:

```python
from vegafusion import PlanResolver
from vegafusion.plan_resolver import unparse_to_sql

class SqlResolver(PlanResolver):
    def __init__(self, connection):
        self._conn = connection

    def resolve_plan_proto(self, plan_bytes, datasets):
        # Convert the DataFusion logical plan to a SQL string
        sql = unparse_to_sql(plan_bytes, dialect="default")

        # Execute the SQL against your database
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return cursor.fetch_arrow_all()
```

`resolve_plan_proto` receives protobuf bytes that can be passed directly to `unparse_to_sql()` without deserialization. To inspect or modify the plan tree, use `resolve_plan()` instead (it receives a deserialized `LogicalPlanNode`).

Supported SQL dialects: `"default"`, `"postgres"`, `"mysql"`, `"sqlite"`, `"duckdb"`, `"bigquery"`.

See [plan_resolver_sql.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/plan_resolver_sql.py) for a complete example.

### Configuration

`PlanResolver` cannot be used with `grpc_connect()` (resolvers run in-process). Class-level attributes control resolver behavior:

- `thread_safe` (default `True`) — set to `False` for backends with thread-affine connections (e.g. DuckDB)
- `skip_when_no_external_tables` (default `True`) — set to `False` to receive all plans, not just those with external tables (e.g. for logging)
- `supports_arrow_tables` (default `False`) — set to `True` to let the runtime eagerly materialize plans into Arrow tables

### API Reference

```{eval-rst}
.. autoclass:: vegafusion.PlanResolver
   :members:

.. autoclass:: vegafusion.ExternalDataset
   :members:

.. autofunction:: vegafusion.plan_resolver.external_table_scan_node

.. autofunction:: vegafusion.plan_resolver.unparse_to_sql

.. autofunction:: vegafusion.plan_resolver.unparse_expr_to_sql

.. autofunction:: vegafusion.plan_resolver.inline_table_scan_node
```

## Rust

The `PlanResolver` trait in `vegafusion-runtime` provides the same two-phase architecture (scan_url at planning time, resolve_table/resolve_plan at execution time). See the [vegafusion-runtime docs on docs.rs](https://docs.rs/vegafusion-runtime/) for the full API.
