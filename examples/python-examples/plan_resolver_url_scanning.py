# Requires: pip install vegafusion[plan-resolver]
"""
Demonstrates the URL scanning pattern for custom URL schemes:
capabilities() + scan_url() + resolve_table()

VegaFusion's PlanResolver lets you register custom URL schemes so that
data references like "mydata://database/sales" in a Vega spec are resolved
by your own Python code rather than fetched over HTTP.
"""

import json
from typing import Any

import pyarrow as pa
import vegafusion as vf
from vegafusion import PlanResolver
from vegafusion.plan_resolver import external_table_scan_node


def main():
    resolver = SalesDataResolver()
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    spec = make_spec()
    datasets, warnings = rt.pre_transform_datasets(
        spec, datasets=["sales"], dataset_format="pyarrow"
    )

    assert warnings == []
    assert len(datasets) == 1

    table = datasets[0]
    assert table.column("product").to_pylist() == ["Widget", "Gadget", "Gizmo"]
    assert table.column("revenue").to_pylist() == [1200, 3400, 560]
    print("Result table:")
    print(table.to_pandas().to_string(index=False))
    print("\nAll assertions passed.")


class SalesDataResolver(PlanResolver):
    """Resolves URLs with the 'mydata' scheme using in-memory data."""

    def capabilities(self) -> dict[str, list[str]]:
        return {"supported_schemes": ["mydata"]}

    def scan_url(self, parsed_url: dict[str, Any]) -> Any:
        if parsed_url["scheme"] == "mydata":
            schema = pa.schema([("product", pa.utf8()), ("revenue", pa.int64())])
            return external_table_scan_node(
                table_name="sales_data",
                schema=schema,
                scheme="mydata",
            )
        return None

    def resolve_table(
        self,
        name: str,
        scheme: str,
        schema: Any,
        metadata: dict[str, Any] | None = None,
        projected_columns: list[str] | None = None,
    ) -> pa.Table:
        return pa.table(
            {
                "product": ["Widget", "Gadget", "Gizmo"],
                "revenue": [1200, 3400, 560],
            }
        )


def make_spec() -> dict[str, Any]:
    spec_str = """
{
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "data": [
    {
      "name": "sales",
      "url": "mydata://database/sales"
    }
  ]
}
    """
    return json.loads(spec_str)


if __name__ == "__main__":
    main()
