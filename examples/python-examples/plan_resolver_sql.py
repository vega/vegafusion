# Demonstrates SQL transpilation using resolve_plan_proto() + unparse_to_sql().
# The resolver receives a serialized logical plan, converts it to SQL, and returns
# a result table. In a real application you would execute the SQL against a database.

import json
from typing import Any

import pyarrow as pa

import vegafusion as vf
from vegafusion import ExternalDataset, PlanResolver
from vegafusion.plan_resolver import unparse_to_sql


def main() -> None:
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})
    ext = ExternalDataset(scheme="table", schema=source_table.schema, data=source_table)

    resolver = SqlTranspileResolver(source_table=source_table)
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    spec = get_spec()
    datasets, warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert warnings == []
    result = datasets[0]
    assert result.column("x").to_pylist() == [5, 10]
    assert result.column("y").to_pylist() == ["b", "c"]
    assert resolver.captured_sql is not None
    assert "SELECT" in resolver.captured_sql

    print("Captured SQL (postgres dialect):")
    print(resolver.captured_sql)
    print()
    print("Result table:")
    print(result)


class SqlTranspileResolver(PlanResolver):
    """Converts the logical plan to Postgres-dialect SQL."""

    def __init__(self, source_table: pa.Table) -> None:
        self.source_table = source_table
        self.captured_sql: str | None = None

    def resolve_plan_proto(
        self, plan_bytes: bytes, datasets: dict[str, Any]
    ) -> pa.Table:
        sql = unparse_to_sql(plan_bytes, dialect="postgres")
        self.captured_sql = sql
        # In a real scenario you would execute `sql` against a database.
        return pa.table({"x": [5, 10], "y": ["b", "c"]})


def get_spec() -> dict[str, Any]:
    return json.loads("""
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "source",
      "url": "table://source"
    },
    {
      "name": "filtered",
      "source": "source",
      "transform": [
        {
          "type": "filter",
          "expr": "datum.x > 3"
        }
      ]
    }
  ]
}
    """)


if __name__ == "__main__":
    main()
