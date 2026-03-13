from __future__ import annotations

import uuid
import weakref
from typing import Any, ClassVar

from arro3.core import Schema


class _DataRef:
    """Weak-referenceable wrapper holding an opaque data object."""

    __slots__ = ("data", "__weakref__")

    def __init__(self, data: Any) -> None:  # noqa: ANN401
        self.data = data


class ExternalDataset:
    """External dataset with scheme, schema, metadata, and optional data ref.

    The ``scheme`` parameter is an optional short identifier for the data
    source type (e.g. ``"spark"``, ``"snowflake"``, ``"duckdb"``).  It is
    propagated through protobuf separately from metadata so that error
    messages can name the source when no resolver is registered.

    When ``data`` is provided, it is registered in a class-level
    :class:`weakref.WeakValueDictionary` keyed by a UUID.  The UUID is
    embedded in the metadata under ``_vf_ref_id`` so that the Rust bridge
    can recover the live Python data object after the plan has round-tripped
    through protobuf serialization.

    The ``ExternalDataset`` itself is *not* stored in the registry — only
    the data.  On the return path from Rust, a fresh ``ExternalDataset``
    is reconstructed from the protobuf schema + metadata plus the recovered
    data (if still alive).
    """

    _registry: ClassVar[weakref.WeakValueDictionary[str, _DataRef]] = (
        weakref.WeakValueDictionary()
    )

    def __init__(
        self,
        scheme: str | None = None,
        schema: Any = None,  # noqa: ANN401
        metadata: dict[str, Any] | None = None,
        data: Any = None,  # noqa: ANN401
        source: str | None = None,
    ) -> None:
        self._schema: Schema = (
            Schema.from_arrow(schema) if not isinstance(schema, Schema) else schema
        )
        self._scheme = scheme
        self._source = source
        self._metadata: dict[str, Any] = dict(metadata) if metadata else {}
        self._data: Any = data
        self._data_ref: _DataRef | None = None

        if data is not None and "_vf_ref_id" not in self._metadata:
            ref_id = str(uuid.uuid4())
            self._data_ref = _DataRef(data)
            ExternalDataset._registry[ref_id] = self._data_ref
            self._metadata["_vf_ref_id"] = ref_id

    @classmethod
    def resolve_data(cls, ref_id: str) -> Any | None:  # noqa: ANN401
        """Look up a data object by its ``_vf_ref_id``.

        Returns the raw data object, or ``None`` if the owning
        ``ExternalDataset`` has been garbage-collected.
        """
        data_ref = cls._registry.get(ref_id)
        return data_ref.data if data_ref is not None else None

    @property
    def scheme(self) -> str | None:
        """Optional short identifier for the data source type (e.g. ``"spark"``)."""
        return self._scheme

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def data(self) -> Any:  # noqa: ANN401
        return self._data
