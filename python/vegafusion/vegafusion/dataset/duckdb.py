import logging
import pyarrow as pa
from duckdb import DuckDBPyRelation
from .sql import SqlDataset
from ..connection.duckdb import duckdb_relation_to_schema
from typing import Any


class DuckDbDataset(SqlDataset):
    @classmethod
    def dialect(cls) -> str:
        return "duckdb"

    def __init__(self, relation: DuckDBPyRelation, fallback: bool = True, verbose: bool = False):
        self._relation = relation
        self._fallback = fallback
        self._verbose = verbose
        self._table_name = "tbl"
        self._table_schema = duckdb_relation_to_schema(self._relation)
        self.logger = logging.getLogger("DuckDbDataset")

    def table_name(self) -> str:
        return self._table_name

    def table_schema(self) -> pa.Schema:
        return self._table_schema

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        self.logger.info(f"DuckDB Query:\n{query}\n")
        if self._verbose:
            print(f"DuckDB Query:\n{query}\n")

        return self._relation.query(self._table_name, query).to_arrow_table()

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True, **kwargs
    ) -> Any:
        return self._relation.to_arrow_table().__dataframe__(
            nan_as_null=nan_as_null, allow_copy=allow_copy
        )

    def fallback(self) -> bool:
        return self._fallback
