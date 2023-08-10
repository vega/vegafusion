import logging
import pyarrow as pa
from duckdb import DuckDBPyRelation
from .sql import SqlDataset
from ..connection.duckdb import duckdb_relation_to_schema


class DuckDbDataset(SqlDataset):
    def dialect(self) -> str:
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

    def fallback(self) -> bool:
        return self._fallback
