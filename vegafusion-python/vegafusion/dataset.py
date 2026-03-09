from __future__ import annotations

import uuid
import weakref
from typing import Any, ClassVar

from arro3.core import Schema


class ExternalDataset:
    """An external dataset with schema, JSON metadata, and optional object ref.

    When ``data`` is provided, the instance is registered in a class-level
    :class:`weakref.WeakValueDictionary` keyed by a UUID.  The UUID is embedded
    in the metadata under ``_vf_ref_id`` so that a :class:`PlanResolver` can
    resolve the live Python object after the plan has round-tripped through Rust.
    """

    _registry: ClassVar[weakref.WeakValueDictionary[str, ExternalDataset]] = (
        weakref.WeakValueDictionary()
    )

    def __init__(
        self,
        schema: Any,  # noqa: ANN401
        metadata: dict[str, Any] | None = None,
        data: Any = None,  # noqa: ANN401
    ) -> None:
        self._schema: Schema = (
            Schema.from_arrow(schema) if not isinstance(schema, Schema) else schema
        )
        self._metadata: dict[str, Any] = dict(metadata) if metadata else {}
        self._data: Any = data

        if data is not None:
            self._ref_id = str(uuid.uuid4())
            ExternalDataset._registry[self._ref_id] = self
            self._metadata["_vf_ref_id"] = self._ref_id

    @classmethod
    def resolve_ref(cls, ref_id: str) -> ExternalDataset | None:
        """Look up an ExternalDataset by its ``_vf_ref_id``."""
        return cls._registry.get(ref_id)

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def data(self) -> Any:  # noqa: ANN401
        return self._data
