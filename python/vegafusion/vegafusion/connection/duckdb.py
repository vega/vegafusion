from . import SqlConnection, CsvReadOptions

from typing import Dict, Union

import duckdb
import pyarrow as pa
import pandas as pd


def duckdb_type_name_to_pyarrow_type(duckdb_type: str) -> pa.DataType:
    if duckdb_type in ('VARCHAR', 'JSON'):
        return pa.string()
    elif duckdb_type == "UBIGINT":
        return pa.uint64()
    elif duckdb_type == "DOUBLE":
        return pa.float64()
    else:
        raise ValueError(f"Unexpected type string {duckdb_type}")


def duckdb_relation_to_schema(rel: duckdb.DuckDBPyRelation) -> pa.Schema:
    return pa.schema({
        col: duckdb_type_name_to_pyarrow_type(type_name)
        for col, type_name in zip(rel.columns, rel.dtypes)
    })


class DuckDbConnection(SqlConnection):
    def __init__(self, inline_datasets: Dict[str, Union[pd.DataFrame, pa.Table]]):
        self._table_schemas = {}
        self.conn = duckdb.connect()
        for name, tbl in inline_datasets.items():
            self.conn.register(name, tbl)
            if isinstance(tbl, pd.DataFrame):
                self._table_schemas[name] = pa.Schema.from_pandas(tbl)
            elif isinstance(tbl, pa.Table):
                self._table_schemas[name] = tbl.schema
            else:
                raise ValueError(f"Unexpected Table type: {type(tbl)}")

    @classmethod
    def dialect(cls) -> str:
        return "duckdb"

    def tables(self) -> Dict[str, pa.Schema]:
        return dict(**self._table_schemas)

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        # print(query)
        return self.conn.query(query).to_arrow_table(8096)

    def register_arrow(self, name: str, table: pa.Table):
        self.conn.register(name, table)
        self._table_schemas[name] = table.schema

    def register_json(self, name: str, path: str):
        relation = self.conn.read_json(path)
        relation.to_view(name)
        self._table_schemas[name] = duckdb_relation_to_schema(relation)

    def register_csv(self, name: str, path: str, options: CsvReadOptions):
        # TODO: handle schema from options
        relation = self.conn.read_csv(
            path,
            header=options.has_header,
            delimiter=options.delimeter,
        )
        relation.to_view(name)
        self._table_schemas[name] = duckdb_relation_to_schema(relation)

    def register_parquet(self, name: str, path: str):
        relation = self.conn.read_parquet(path)
        relation.to_view(name)
        self._table_schemas[name] = duckdb_relation_to_schema(relation)
