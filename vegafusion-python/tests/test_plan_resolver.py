from __future__ import annotations

import os
import tempfile
from typing import Any

import pyarrow as pa
import pyarrow.csv as pcsv

import vegafusion as vf
from vegafusion import ExternalDataset, PlanResolver
import pytest
from inline_snapshot import snapshot

from vegafusion.plan_resolver import (
    ResolvedPlan,
    inline_table_scan_node,
    unparse_to_sql,
)


def setup_module(module: Any) -> None:
    vf.set_local_tz("UTC")


class PassthroughResolver(PlanResolver):
    """A simple resolver that returns a fixed result table (full execution)."""

    def __init__(self, result_table: pa.Table) -> None:
        self._result_table = result_table
        self.last_plan_bytes: bytes | None = None
        self.last_datasets: dict[str, Any] | None = None

    def resolve_plan_proto(
        self, plan_bytes: bytes, datasets: dict[str, Any]
    ) -> pa.Table:
        self.last_plan_bytes = plan_bytes
        self.last_datasets = datasets
        return self._result_table


class DeserializingResolver(PlanResolver):
    """A resolver that exercises the two-way dispatch
    (proto bytes -> LogicalPlanNode -> resolve_plan()).
    """

    def __init__(self, result_table: pa.Table) -> None:
        self._result_table = result_table
        self.last_plan: Any = None
        self.last_datasets: dict[str, Any] | None = None

    def resolve_plan(self, logical_plan: Any, datasets: dict[str, Any]) -> pa.Table:
        self.last_plan = logical_plan
        self.last_datasets = datasets
        return self._result_table


def _simple_spec(dataset_name: str = "source") -> dict[str, Any]:
    """A minimal Vega spec that references an inline dataset."""
    return {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {
                "name": dataset_name,
                "url": f"table://{dataset_name}",
            },
            {
                "name": "filtered",
                "source": dataset_name,
                "transform": [
                    {
                        "type": "filter",
                        "expr": "datum.x > 3",
                    }
                ],
            },
        ],
    }


def test_passthrough_resolver() -> None:
    """ExternalDataset + PassthroughResolver round-trips through Rust."""
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})
    expected_result = pa.table({"x": [5, 10], "y": ["b", "c"]})

    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)
    resolver = PassthroughResolver(result_table=expected_result)

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    datasets, warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert len(datasets) == 1
    assert datasets[0].equals(expected_result)
    assert resolver.last_plan_bytes is not None
    assert len(resolver.last_plan_bytes) > 0
    # Verify datasets dict was passed with a reconstructed ExternalDataset
    assert resolver.last_datasets is not None
    assert "source" in resolver.last_datasets
    ds = resolver.last_datasets["source"]
    assert isinstance(ds, ExternalDataset)
    assert ds.data is source_table  # data recovered via registry


def test_deserializing_resolver() -> None:
    """Two-way dispatch: proto bytes -> LogicalPlanNode -> resolve_plan()."""
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})
    expected_result = pa.table({"x": [5, 10], "y": ["b", "c"]})

    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)
    resolver = DeserializingResolver(result_table=expected_result)

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    datasets, _warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert len(datasets) == 1
    assert datasets[0].equals(expected_result)
    assert resolver.last_plan is not None
    assert hasattr(resolver.last_plan, "SerializeToString")
    # Verify datasets dict was passed through the two-way dispatch
    assert resolver.last_datasets is not None
    assert "source" in resolver.last_datasets
    ds = resolver.last_datasets["source"]
    assert isinstance(ds, ExternalDataset)
    assert ds.data is source_table  # data recovered via registry


def test_external_dataset_registry() -> None:
    """ExternalDataset with data registers data in weakref registry."""
    table = pa.table({"a": [1, 2, 3]})
    ext = ExternalDataset(
        "test", schema=table.schema, data=table, metadata={"engine": "test"}
    )

    assert ext.kind == "test"
    assert "_vf_kind" not in ext.metadata  # kind is separate from metadata
    assert "_vf_ref_id" in ext.metadata
    ref_id = ext.metadata["_vf_ref_id"]
    assert ExternalDataset.resolve_data(ref_id) is table
    assert ext.data is table
    assert ext.metadata["engine"] == "test"


def test_external_dataset_schema_only() -> None:
    """ExternalDataset without data does not register."""
    schema = pa.schema([("x", pa.int64())])
    ext = ExternalDataset("test", schema=schema)

    assert "_vf_ref_id" not in ext.metadata
    assert ext.data is None


def test_plan_resolver_on_runtime_property() -> None:
    """Setting plan_resolver property triggers runtime reset."""
    result = pa.table({"x": [1]})
    resolver = PassthroughResolver(result_table=result)

    rt = vf.VegaFusionRuntime()
    assert rt.plan_resolver is None

    rt.plan_resolver = resolver
    assert rt.plan_resolver is resolver
    assert rt._runtime is None


def test_datafusion_resolver_aggregate() -> None:
    """End-to-end: datafusion-python resolver with aggregate transforms on CSV."""
    import datafusion

    # Write test CSV
    csv_path = os.path.join(tempfile.gettempdir(), "vf_resolver_test.csv")
    table = pa.table(
        {
            "category": ["A", "A", "B", "B", "C"],
            "amount": [10, 20, 30, 40, 50],
        }
    )
    pcsv.write_csv(table, csv_path)

    spec = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {
                "name": "source",
                "url": csv_path,
                "format": {"type": "csv"},
            },
            {
                "name": "aggregated",
                "source": "source",
                "transform": [
                    {
                        "type": "aggregate",
                        "groupby": ["category"],
                        "ops": ["sum"],
                        "fields": ["amount"],
                        "as": ["total"],
                    },
                    {
                        "type": "collect",
                        "sort": {"field": ["category"]},
                    },
                ],
            },
        ],
    }

    class DataFusionResolver(PlanResolver):
        def resolve_plan_proto(
            self, plan_bytes: bytes, datasets: dict[str, Any]
        ) -> pa.Table:  # noqa: ANN401
            ctx = datafusion.SessionContext()
            plan = datafusion.LogicalPlan.from_proto(ctx, plan_bytes)
            df = ctx.create_dataframe_from_logical_plan(plan)
            return df.to_arrow_table()

    resolver = DataFusionResolver()
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    datasets, _warnings = rt.pre_transform_datasets(
        spec, datasets=["aggregated"], dataset_format="pyarrow"
    )

    result = datasets[0]
    assert result.num_rows == 3
    assert result.column("category").to_pylist() == ["A", "B", "C"]
    assert result.column("total").to_pylist() == [30.0, 70.0, 50.0]


def test_datafusion_resolver_filter_formula() -> None:
    """End-to-end: datafusion-python resolver with filter + formula transforms."""
    import datafusion

    csv_path = os.path.join(tempfile.gettempdir(), "vf_resolver_test2.csv")
    table = pa.table(
        {
            "name": ["Alice", "Bob", "Charlie", "Diana"],
            "score": [85, 42, 91, 67],
        }
    )
    pcsv.write_csv(table, csv_path)

    spec = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {
                "name": "students",
                "url": csv_path,
                "format": {"type": "csv"},
            },
            {
                "name": "passing",
                "source": "students",
                "transform": [
                    {"type": "filter", "expr": "datum.score >= 60"},
                    {
                        "type": "formula",
                        "as": "grade",
                        "expr": "datum.score >= 90 ? 'A' : 'B'",
                    },
                    {
                        "type": "collect",
                        "sort": {"field": ["name"]},
                    },
                ],
            },
        ],
    }

    class DataFusionResolver(PlanResolver):
        def resolve_plan_proto(
            self, plan_bytes: bytes, datasets: dict[str, Any]
        ) -> pa.Table:  # noqa: ANN401
            ctx = datafusion.SessionContext()
            plan = datafusion.LogicalPlan.from_proto(ctx, plan_bytes)
            df = ctx.create_dataframe_from_logical_plan(plan)
            return df.to_arrow_table()

    resolver = DataFusionResolver()
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    datasets, _warnings = rt.pre_transform_datasets(
        spec, datasets=["passing"], dataset_format="pyarrow"
    )

    result = datasets[0]
    assert result.num_rows == 3
    assert result.column("name").to_pylist() == ["Alice", "Charlie", "Diana"]
    assert result.column("grade").to_pylist() == ["B", "A", "B"]


def test_resolve_table_resolver() -> None:
    """resolve_table override: provides per-table data, plan rewriting handled by base class."""
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})

    class TableResolver(PlanResolver):
        def __init__(self) -> None:
            self.resolve_calls: list[dict[str, Any]] = []

        def resolve_table(
            self,
            name: str,
            schema: Any,
            metadata: dict[str, Any],
            projected_columns: list[str] | None = None,
        ) -> pa.Table:
            self.resolve_calls.append(
                {
                    "name": name,
                    "metadata": metadata,
                    "projected_columns": projected_columns,
                }
            )
            return source_table

    resolver = TableResolver()
    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    datasets, _warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert len(datasets) == 1
    # The filter transform (datum.x > 3) should be applied by DataFusion
    # after the resolver provides the data
    result = datasets[0]
    assert result.num_rows == 2
    assert result.column("x").to_pylist() == [5, 10]

    # Verify resolve_table was called
    assert len(resolver.resolve_calls) > 0
    assert resolver.resolve_calls[0]["name"] == "source"


def test_resolve_plan_returns_resolved_plan() -> None:
    """resolve_plan override returns ResolvedPlan with manual inline_table_scan_node."""
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})

    class ManualResolver(PlanResolver):
        """Overrides resolve_plan to manually walk the plan and build ResolvedPlan."""

        def __init__(self) -> None:
            self.was_called = False

        def resolve_plan(
            self, logical_plan: Any, datasets: dict[str, Any]
        ) -> ResolvedPlan:
            self.was_called = True
            # Use the default tree-walking implementation but verify we can
            # return ResolvedPlan directly
            sidecar: dict[str, Any] = {}
            # Walk the plan and replace external tables manually
            for name, ext in datasets.items():
                replacement = inline_table_scan_node(
                    name=name,
                    schema=ext.schema,
                )
                # Find and replace the custom_scan node
                self._replace_custom_scan(logical_plan, name, replacement)
                sidecar[name] = source_table
            return ResolvedPlan(plan=logical_plan, datasets=sidecar)

        def _replace_custom_scan(
            self, node: Any, target_name: str, replacement: Any
        ) -> None:
            """Recursively find and replace a custom_scan node by table name."""
            variant = node.WhichOneof("LogicalPlanType")
            if variant is None:
                return
            inner = getattr(node, variant)
            if variant == "custom_scan":
                table_ref = inner.table_name
                which = table_ref.WhichOneof("table_reference_enum")
                if which == "bare" and table_ref.bare.table == target_name:
                    node.CopyFrom(replacement)
                return
            # Recurse
            from vegafusion.plan_resolver import _get_child_nodes

            for child in _get_child_nodes(variant, inner):
                self._replace_custom_scan(child, target_name, replacement)

    resolver = ManualResolver()
    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    datasets, _warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert resolver.was_called
    assert len(datasets) == 1
    result = datasets[0]
    # Filter (datum.x > 3) applied by DataFusion after resolver provides data
    assert result.num_rows == 2
    assert result.column("x").to_pylist() == [5, 10]


def test_multiple_external_tables() -> None:
    """Plan with multiple external tables resolved independently."""
    table_a = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
    table_b = pa.table({"id": [2, 3, 4], "val": [200, 300, 400]})

    class MultiTableResolver(PlanResolver):
        def __init__(self) -> None:
            self.resolved_names: list[str] = []

        def resolve_table(
            self,
            name: str,
            schema: Any,
            metadata: dict[str, Any],
            projected_columns: list[str] | None = None,
        ) -> pa.Table:
            self.resolved_names.append(name)
            if name == "source_a":
                return table_a
            elif name == "source_b":
                return table_b
            raise ValueError(f"Unknown table: {name}")

    spec = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {"name": "source_a", "url": "table://source_a"},
            {"name": "source_b", "url": "table://source_b"},
            {
                "name": "filtered_a",
                "source": "source_a",
                "transform": [{"type": "filter", "expr": "datum.val > 15"}],
            },
            {
                "name": "filtered_b",
                "source": "source_b",
                "transform": [{"type": "filter", "expr": "datum.val > 250"}],
            },
        ],
    }

    ext_a = ExternalDataset("test", schema=table_a.schema, data=table_a)
    ext_b = ExternalDataset("test", schema=table_b.schema, data=table_b)
    resolver = MultiTableResolver()

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    datasets, _warnings = rt.pre_transform_datasets(
        spec,
        datasets=["filtered_a", "filtered_b"],
        inline_datasets={"source_a": ext_a, "source_b": ext_b},
        dataset_format="pyarrow",
    )

    assert len(datasets) == 2
    # filtered_a: val > 15 → rows with val 20, 30
    assert datasets[0].num_rows == 2
    assert datasets[0].column("val").to_pylist() == [20, 30]
    # filtered_b: val > 250 → rows with val 300, 400
    assert datasets[1].num_rows == 2
    assert datasets[1].column("val").to_pylist() == [300, 400]
    # Both tables were resolved
    assert "source_a" in resolver.resolved_names
    assert "source_b" in resolver.resolved_names


def test_resolve_table_error_propagates() -> None:
    """Error in resolve_table propagates as a runtime error."""
    source_table = pa.table({"x": [1, 2, 3]})

    class FailingResolver(PlanResolver):
        def resolve_table(
            self,
            name: str,
            schema: Any,
            metadata: dict[str, Any],
            projected_columns: list[str] | None = None,
        ) -> pa.Table:
            raise ValueError("Simulated resolver failure")

    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)
    resolver = FailingResolver()
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    with pytest.raises(Exception, match="Simulated resolver failure"):
        rt.pre_transform_datasets(
            spec,
            datasets=["filtered"],
            inline_datasets={"source": ext},
            dataset_format="pyarrow",
        )


def test_no_external_tables_passthrough() -> None:
    """Plans without external tables skip the resolver and execute directly."""

    class NoOpResolver(PlanResolver):
        pass

    # Use a CSV-based spec so DataFusion can execute without external tables
    csv_path = os.path.join(tempfile.gettempdir(), "vf_noop_test.csv")
    table = pa.table({"x": [1, 5, 10]})
    pcsv.write_csv(table, csv_path)

    spec = {
        "$schema": "https://vega.github.io/schema/vega/v5.json",
        "data": [
            {
                "name": "source",
                "url": csv_path,
                "format": {"type": "csv"},
            },
            {
                "name": "filtered",
                "source": "source",
                "transform": [{"type": "filter", "expr": "datum.x > 3"}],
            },
        ],
    }

    resolver = NoOpResolver()
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    datasets, _warnings = rt.pre_transform_datasets(
        spec, datasets=["filtered"], dataset_format="pyarrow"
    )

    assert len(datasets) == 1
    result = datasets[0]
    assert result.num_rows == 2
    assert result.column("x").to_pylist() == ["5", "10"]


def test_unparse_plan_to_sql_from_resolver() -> None:
    """unparse_to_sql converts a plan from resolve_plan_proto into SQL."""
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})

    class SqlCapturingResolver(PlanResolver):
        def __init__(self) -> None:
            self.captured_sql: dict[str, str] = {}

        def resolve_plan_proto(
            self, plan_bytes: bytes, datasets: dict[str, Any]
        ) -> pa.Table:
            for dialect in ["default", "postgres", "mysql", "sqlite", "duckdb"]:
                self.captured_sql[dialect] = unparse_to_sql(plan_bytes, dialect=dialect)
            # Return a dummy result
            return source_table

    resolver = SqlCapturingResolver()
    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    # Verify SQL output with inline snapshots for each dialect
    assert resolver.captured_sql["default"] == snapshot(
        'SELECT x, y FROM (SELECT _vf_order AS _vf_order, "source".x AS x, "source".y AS y FROM (SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS _vf_order, "source".x, "source".y FROM "source")) WHERE CASE WHEN (x > 3.0) IS NULL THEN false ELSE (x > 3.0) END ORDER BY _vf_order ASC NULLS LAST'
    )
    assert resolver.captured_sql["postgres"] == snapshot(
        'SELECT "x", "y" FROM (SELECT "_vf_order" AS "_vf_order", "source"."x" AS "x", "source"."y" AS "y" FROM (SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "_vf_order", "source"."x", "source"."y" FROM "source") AS "derived_projection") AS "derived_projection" WHERE CASE WHEN ("x" > 3.0) IS NULL THEN false ELSE ("x" > 3.0) END ORDER BY "_vf_order" ASC NULLS LAST'
    )
    assert resolver.captured_sql["mysql"] == snapshot(
        "SELECT `x`, `y` FROM (SELECT `_vf_order` AS `_vf_order`, `source`.`x` AS `x`, `source`.`y` AS `y` FROM (SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `_vf_order`, `source`.`x`, `source`.`y` FROM `source`) AS `derived_projection`) AS `derived_projection` WHERE CASE WHEN (`x` > 3.0) IS NULL THEN false ELSE (`x` > 3.0) END ORDER BY `_vf_order` ASC"
    )
    assert resolver.captured_sql["sqlite"] == snapshot(
        "SELECT `x`, `y` FROM (SELECT `_vf_order` AS `_vf_order`, `source`.`x` AS `x`, `source`.`y` AS `y` FROM (SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `_vf_order`, `source`.`x`, `source`.`y` FROM `source`)) WHERE CASE WHEN (`x` > 3.0) IS NULL THEN false ELSE (`x` > 3.0) END ORDER BY `_vf_order` ASC NULLS LAST"
    )
    assert resolver.captured_sql["duckdb"] == snapshot(
        'SELECT "x", "y" FROM (SELECT "_vf_order" AS "_vf_order", "source"."x" AS "x", "source"."y" AS "y" FROM (SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "_vf_order", "source"."x", "source"."y" FROM "source")) WHERE CASE WHEN ("x" > 3.0) IS NULL THEN false ELSE ("x" > 3.0) END ORDER BY "_vf_order" ASC NULLS LAST'
    )


def test_unparse_plan_to_sql_from_proto_message() -> None:
    """unparse_to_sql accepts a deserialized LogicalPlanNode."""
    source_table = pa.table({"x": [1, 5, 10], "y": ["a", "b", "c"]})

    class ProtoCapturingResolver(PlanResolver):
        def __init__(self) -> None:
            self.sql_from_bytes: str | None = None
            self.sql_from_proto: str | None = None

        def resolve_plan(self, logical_plan: Any, datasets: dict[str, Any]) -> pa.Table:
            # Test with deserialized protobuf message
            self.sql_from_proto = unparse_to_sql(logical_plan, dialect="postgres")
            # Also test via bytes for comparison
            self.sql_from_bytes = unparse_to_sql(
                logical_plan.SerializeToString(), dialect="postgres"
            )
            return source_table

    resolver = ProtoCapturingResolver()
    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)

    rt = vf.VegaFusionRuntime(plan_resolver=resolver)
    spec = _simple_spec()

    rt.pre_transform_datasets(
        spec,
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert resolver.sql_from_proto is not None
    assert resolver.sql_from_bytes is not None
    assert resolver.sql_from_proto == resolver.sql_from_bytes
    # Verify the SQL references the external table name
    assert resolver.sql_from_proto == snapshot(
        'SELECT "x", "y" FROM (SELECT "_vf_order" AS "_vf_order", "source"."x" AS "x", "source"."y" AS "y" FROM (SELECT row_number() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "_vf_order", "source"."x", "source"."y" FROM "source") AS "derived_projection") AS "derived_projection" WHERE CASE WHEN ("x" > 3.0) IS NULL THEN false ELSE ("x" > 3.0) END ORDER BY "_vf_order" ASC NULLS LAST'
    )


def test_external_dataset_without_resolver_raises() -> None:
    """ExternalDataset without a plan resolver raises ValueError with helpful message."""
    source_table = pa.table({"x": [1, 2, 3]})
    ext = ExternalDataset("spark", schema=source_table.schema, data=source_table)

    rt = vf.VegaFusionRuntime()  # No resolver
    spec = _simple_spec()

    with pytest.raises(ValueError, match="require a plan resolver") as exc_info:
        rt.pre_transform_datasets(
            spec,
            datasets=["filtered"],
            inline_datasets={"source": ext},
            dataset_format="pyarrow",
        )
    # Verify kind appears in the error message
    assert "spark" in str(exc_info.value)


def test_unparse_invalid_dialect() -> None:
    """unparse_to_sql raises ValueError for unknown dialect."""
    source_table = pa.table({"x": [1, 5, 10]})

    class DialectTestResolver(PlanResolver):
        def __init__(self) -> None:
            self.error: Exception | None = None

        def resolve_plan_proto(
            self, plan_bytes: bytes, datasets: dict[str, Any]
        ) -> pa.Table:
            try:
                unparse_to_sql(plan_bytes, dialect="spark")
            except ValueError as e:
                self.error = e
            return source_table

    resolver = DialectTestResolver()
    ext = ExternalDataset("test", schema=source_table.schema, data=source_table)
    rt = vf.VegaFusionRuntime(plan_resolver=resolver)

    rt.pre_transform_datasets(
        _simple_spec(),
        datasets=["filtered"],
        inline_datasets={"source": ext},
        dataset_format="pyarrow",
    )

    assert resolver.error is not None
    assert "Unknown dialect" in str(resolver.error)
