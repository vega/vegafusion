from typing import Iterable
import pyarrow as pa
from .datasource import Datasource

class PyArrowDatasource(Datasource):
    def __init__(self, dataframe: pa.Table):
        self._table = dataframe

    def schema(self) -> pa.Schema:
        return self._table.schema

    def fetch(self, columns: Iterable[str]) -> pa.Table:
        return pa.Table.from_arrays(
            [self._table[c] for c in columns],
            names=list(columns)
        )
