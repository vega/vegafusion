from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Union

from arro3.core import Schema, Table

if TYPE_CHECKING:
    from vegafusion.dataset import ExternalDataset
    from vegafusion.proto.datafusion_pb2 import LogicalPlanNode  # type: ignore[attr-defined]


@dataclass
class ResolvedPlan:
    """A rewritten plan with optional sidecar Arrow data.

    Returned from resolve_plan/resolve_plan_proto when the resolver
    rewrites the plan but leaves execution to DataFusion.
    """

    plan: LogicalPlanNode | bytes
    datasets: dict[str, Table] = field(default_factory=dict)
    """Sidecar dict mapping table names to Arrow-compatible tables.
    Used to resolve InlineTableProvider markers in the plan."""


# ResolutionResult is the return type for resolve_plan / resolve_plan_proto.
# Return an Arrow-compatible table for full execution, or a ResolvedPlan for
# plan rewriting.
ResolutionResult = Union[Table, ResolvedPlan]


class PlanResolver:
    """Base class for plan resolvers.

    Override one of these (checked in priority order):

    1. ``resolve_table`` — simple per-table data resolution (always returns Plan)
    2. ``resolve_plan_proto`` / ``resolve_plan`` — full control over resolution

    For ``resolve_plan``, override either the ``_proto`` variant (raw bytes) or
    the non-``_proto`` variant (deserialized ``LogicalPlanNode``). The ``_proto``
    variant's default implementation deserializes and delegates to
    ``resolve_plan``.
    """

    def resolve_table(
        self,
        name: str,
        schema: Schema,
        metadata: dict[str, Any],
        projected_columns: list[str] | None = None,
    ) -> Table:
        """Provide data for an external table reference.

        Called once per ExternalTableProvider node in the plan.

        Args:
            name: Table name from the plan.
            schema: Full schema of the external table.
            metadata: JSON metadata dict from ExternalTableProvider.
            projected_columns: Column names DataFusion actually needs.
                None if no projection (all columns needed).

        Returns:
            An Arrow-compatible table (arro3, PyArrow, etc.).
        """
        raise NotImplementedError

    def resolve_plan_proto(
        self,
        plan_bytes: bytes,
        datasets: dict[str, ExternalDataset],
    ) -> ResolutionResult:
        """Resolve a plan given raw protobuf bytes.

        The default implementation deserializes into a
        LogicalPlanNode and calls resolve_plan().
        """
        from vegafusion.proto.datafusion_pb2 import (  # type: ignore[attr-defined]
            LogicalPlanNode,
        )

        plan = LogicalPlanNode()
        plan.ParseFromString(plan_bytes)
        result = self.resolve_plan(plan, datasets)
        if isinstance(result, ResolvedPlan):
            return ResolvedPlan(
                plan=result.plan.SerializeToString(),
                datasets=result.datasets,
            )
        return result  # Arrow table passthrough

    def resolve_plan(
        self,
        logical_plan: LogicalPlanNode,
        datasets: dict[str, ExternalDataset],
    ) -> ResolutionResult:
        """Resolve a plan given a deserialized LogicalPlanNode.

        The default implementation walks the plan tree looking for
        CustomTableScanNode nodes that correspond to ExternalTableProvider
        entries. For each, it calls resolve_table() and replaces the node
        with an inline_table_scan_node.
        """
        sidecar: dict[str, Table] = {}
        self._resolve_external_tables(logical_plan, datasets, sidecar)
        return ResolvedPlan(plan=logical_plan, datasets=sidecar)

    def _resolve_external_tables(
        self,
        node: LogicalPlanNode,
        datasets: dict[str, ExternalDataset],
        sidecar: dict[str, Table],
    ) -> None:
        """Walk protobuf plan tree, replacing ExternalTableProvider scans."""
        variant = node.WhichOneof("LogicalPlanType")
        if variant is None:
            return

        inner = getattr(node, variant)

        if variant == "custom_scan":
            table_name = _extract_table_name(inner.table_name)
            if table_name in datasets:
                dataset = datasets[table_name]

                # Extract projected columns
                projected_columns = None
                if inner.HasField("projection"):
                    projected_columns = list(inner.projection.columns)

                # Extract metadata from custom_table_data
                metadata: dict[str, Any] = {}
                if inner.custom_table_data:
                    try:
                        envelope = json.loads(inner.custom_table_data)
                        if isinstance(envelope, dict):
                            metadata = envelope.get("metadata", {}) or {}
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass

                # Call resolve_table
                table_data = self.resolve_table(
                    name=table_name,
                    schema=dataset.schema,
                    metadata=metadata,
                    projected_columns=projected_columns,
                )

                # Replace node in-place with inline table scan
                replacement = inline_table_scan_node(
                    name=table_name,
                    schema=dataset.schema,
                )
                node.CopyFrom(replacement)

                # Store the resolved data in sidecar
                sidecar[table_name] = table_data
                return

        # Recurse into children
        for child in _get_child_nodes(variant, inner):
            self._resolve_external_tables(child, datasets, sidecar)


def _extract_table_name(table_ref: Any) -> str:
    """Extract table name string from an OwnedTableReference."""
    which = table_ref.WhichOneof("table_reference_enum")
    if which == "bare":
        return table_ref.bare.table
    elif which == "partial":
        return table_ref.partial.table
    elif which == "full":
        return table_ref.full.table
    else:
        raise ValueError(f"Unknown table reference variant: {which}")


def _get_child_nodes(variant: str, inner: Any) -> list[LogicalPlanNode]:
    """Return child LogicalPlanNode references for a plan node variant."""
    _SINGLE_INPUT = {
        "projection",
        "selection",
        "limit",
        "aggregate",
        "sort",
        "repartition",
        "window",
        "analyze",
        "explain",
        "distinct",
        "subquery_alias",
        "create_view",
        "prepare",
        "distinct_on",
        "copy_to",
        "view_scan",
    }

    _TWO_CHILD = {"join", "cross_join"}
    _MULTI_INPUT = {"union", "extension"}

    if variant in _SINGLE_INPUT:
        if inner.HasField("input"):
            return [inner.input]
        return []
    elif variant in _TWO_CHILD:
        children = []
        if inner.HasField("left"):
            children.append(inner.left)
        if inner.HasField("right"):
            children.append(inner.right)
        return children
    elif variant in _MULTI_INPUT:
        return list(inner.inputs)
    else:
        return []


def inline_table_scan_node(
    name: str,
    schema: Schema,
) -> LogicalPlanNode:
    """Build a LogicalPlanNode for an inline table scan.

    Use this in resolve_plan implementations to replace a subtree
    that the resolver has already executed with a leaf node that
    references sidecar Arrow data by name.

    Args:
        name: Key that will match an entry in ResolvedPlan.datasets.
        schema: Arrow schema of the sidecar table (arro3.core.Schema).

    Returns:
        A deserialized LogicalPlanNode protobuf message.
    """
    from vegafusion._vegafusion import inline_table_scan_node as _native
    from vegafusion.proto.datafusion_pb2 import (  # type: ignore[attr-defined]
        LogicalPlanNode,
    )

    node = LogicalPlanNode()
    node.ParseFromString(_native(name, schema))
    return node
