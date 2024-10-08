from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from .datasource import Datasource

if TYPE_CHECKING:
    import pyarrow as pa


class PyArrowDatasource(Datasource):
    def __init__(self, dataframe: pa.Table) -> None:
        self._table = dataframe

    def schema(self) -> pa.Schema:
        return self._table.schema

    def fetch(self, columns: Iterable[str]) -> pa.Table:
        import pyarrow as pa

        return pa.Table.from_arrays(
            [self._table[c] for c in columns], names=list(columns)
        )
