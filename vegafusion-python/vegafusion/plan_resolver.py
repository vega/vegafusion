from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Union

from arro3.core import Schema, Table

logger = logging.getLogger(__name__)

_PROTOBUF_INSTALL_HINT = (
    "The 'protobuf' package is required for plan-level resolvers "
    "(resolve_plan / resolve_plan_proto) and related utilities "
    "(inline_table_scan_node, unparse_to_sql). "
    "Install it with: pip install vegafusion[plan-resolver]"
)

if TYPE_CHECKING:
    from vegafusion.dataset import ExternalDataset
    from vegafusion.proto.datafusion_pb2 import (
        LogicalPlanNode,  # type: ignore[attr-defined]
    )


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

    1. ``resolve_table`` — provide data for each external table independently
    2. ``resolve_plan_proto`` / ``resolve_plan`` — full control over resolution

    For ``resolve_plan``, override either the ``_proto`` variant (raw bytes) or
    the non-``_proto`` variant (deserialized ``LogicalPlanNode``). The ``_proto``
    variant's default implementation deserializes and delegates to
    ``resolve_plan``.

    .. warning::

        Implementations that override ``resolve_plan`` or ``resolve_plan_proto``
        are coupled to the DataFusion protobuf schema used by the current
        VegaFusion version. Resolver code may require updates when upgrading
        VegaFusion if the underlying DataFusion version changes.
    """

    skip_when_no_external_tables: bool = True
    """When True (default), the resolver is skipped for plans that contain no
    ExternalTableProvider nodes. Set to False to receive all plans, e.g. for
    logging or custom query rewriting."""

    thread_safe: bool = True
    """Whether the resolver can be called from any thread. When False, the
    VegaFusionRuntime uses a single-threaded tokio runtime so that resolver
    callbacks run on the main thread. Set to False for backends with
    thread-affine connections (e.g. DuckDB in-memory databases)."""

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
        try:
            from vegafusion.proto.datafusion_pb2 import (
                LogicalPlanNode,  # type: ignore[attr-defined]
            )
        except ImportError as e:
            raise ImportError(_PROTOBUF_INSTALL_HINT) from e

        plan = LogicalPlanNode()
        plan.ParseFromString(plan_bytes)
        result = self.resolve_plan(plan, datasets)
        if isinstance(result, ResolvedPlan):
            plan_obj = result.plan
            if not isinstance(plan_obj, bytes):
                plan_obj = plan_obj.SerializeToString()  # type: ignore[union-attr]
            return ResolvedPlan(
                plan=plan_obj,
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

                projected_columns = None
                if inner.HasField("projection"):
                    projected_columns = list(inner.projection.columns)

                metadata: dict[str, Any] = {}
                if inner.custom_table_data:
                    try:
                        envelope = json.loads(inner.custom_table_data)
                        if isinstance(envelope, dict):
                            metadata = envelope.get("metadata", {}) or {}
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        logger.warning(
                            "Failed to decode metadata for table '%s'",
                            table_name,
                        )

                table_data = self.resolve_table(
                    name=table_name,
                    schema=dataset.schema,
                    metadata=metadata,
                    projected_columns=projected_columns,
                )

                replacement = inline_table_scan_node(
                    name=table_name,
                    schema=dataset.schema,
                )
                node.CopyFrom(replacement)
                sidecar[table_name] = table_data
                return

        for child in _get_child_nodes(variant, inner):
            self._resolve_external_tables(child, datasets, sidecar)


def _extract_table_name(table_ref: Any) -> str:  # noqa: ANN401
    """Extract table name string from an OwnedTableReference."""
    which = table_ref.WhichOneof("table_reference_enum")
    if which == "bare":
        return str(table_ref.bare.table)
    elif which == "partial":
        return str(table_ref.partial.table)
    elif which == "full":
        return str(table_ref.full.table)
    else:
        raise ValueError(f"Unknown table reference variant: {which}")


def _build_child_fields() -> dict[str, list[tuple[str, bool]]]:
    """Derive plan node child fields from the LogicalPlanNode protobuf descriptor.

    Returns a dict mapping variant name to a list of (field_name, is_repeated)
    tuples for each LogicalPlanNode-typed child field. Leaf variants are omitted.
    """
    try:
        from google.protobuf.descriptor import FieldDescriptor

        from vegafusion.proto.datafusion_pb2 import (
            LogicalPlanNode,  # type: ignore[attr-defined]
        )
    except ImportError as e:
        raise ImportError(_PROTOBUF_INSTALL_HINT) from e

    desc = LogicalPlanNode.DESCRIPTOR
    oneof = desc.oneofs_by_name["LogicalPlanType"]

    result: dict[str, list[tuple[str, bool]]] = {}
    for oneof_field in oneof.fields:
        children = [
            (f.name, f.label == FieldDescriptor.LABEL_REPEATED)
            for f in oneof_field.message_type.fields
            if f.message_type and f.message_type.name == "LogicalPlanNode"
        ]
        if children:
            result[oneof_field.name] = children
    return result


_CHILD_FIELDS: dict[str, list[tuple[str, bool]]] | None = None


def _get_child_fields() -> dict[str, list[tuple[str, bool]]]:
    global _CHILD_FIELDS
    if _CHILD_FIELDS is None:
        _CHILD_FIELDS = _build_child_fields()
    return _CHILD_FIELDS


def _get_child_nodes(variant: str, inner: Any) -> list[LogicalPlanNode]:  # noqa: ANN401
    """Return child LogicalPlanNode references for a plan node variant."""
    fields = _get_child_fields().get(variant)
    if fields is None:
        return []
    children: list[LogicalPlanNode] = []
    for field_name, is_repeated in fields:
        if is_repeated:
            children.extend(getattr(inner, field_name))
        elif inner.HasField(field_name):
            children.append(getattr(inner, field_name))
    return children


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

    try:
        from vegafusion.proto.datafusion_pb2 import (
            LogicalPlanNode,  # type: ignore[attr-defined]
        )
    except ImportError as e:
        raise ImportError(_PROTOBUF_INSTALL_HINT) from e

    node = LogicalPlanNode()
    node.ParseFromString(_native(name, schema))
    return node


def unparse_to_sql(
    plan: bytes | LogicalPlanNode,
    dialect: str = "default",
) -> str:
    """Convert a protobuf LogicalPlan to a SQL string.

    Uses DataFusion's Unparser to convert a serialized LogicalPlan into
    a SQL string in the specified dialect.

    Args:
        plan: Serialized protobuf bytes or a deserialized LogicalPlanNode.
        dialect: SQL dialect. One of ``"default"``, ``"postgres"``,
            ``"mysql"``, ``"sqlite"``, ``"duckdb"``, ``"bigquery"``.

    Returns:
        The SQL string.
    """
    from vegafusion._vegafusion import unparse_plan_to_sql as _native

    if not isinstance(plan, bytes):
        plan = plan.SerializeToString()
    return str(_native(plan, dialect))
