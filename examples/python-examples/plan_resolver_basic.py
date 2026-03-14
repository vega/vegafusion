# Demonstrates the simplest PlanResolver pattern: override resolve_table to provide
# data for an ExternalDataset. No protobuf dependency needed.

import json
from typing import Any

import pyarrow as pa

import vegafusion as vf
from vegafusion import ExternalDataset, PlanResolver


def main() -> None:
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})
    ext = ExternalDataset(
        scheme="custom", schema=source_table.schema, data=source_table
    )
    resolver = TableResolver(source_table)
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    spec = make_spec()
    datasets, warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert len(datasets) == 1
    result = datasets[0]
    assert result.num_rows == 2
    assert result.column("x").to_pylist() == [5, 10]
    assert result.column("y").to_pylist() == ["b", "c"]

    print("Result after filter (x > 3):")
    print(result.to_pandas().to_string(index=False))


class TableResolver(PlanResolver):
    """Returns a fixed table for any resolve_table call."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def resolve_table(
        self,
        name: str,
        scheme: str,
        schema: Any,
        metadata: dict[str, Any] | None = None,
        projected_columns: list[str] | None = None,
    ) -> pa.Table:
        return self._table


def make_spec() -> dict[str, Any]:
    return json.loads(
        """
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
    """
    )


if __name__ == "__main__":
    main()
