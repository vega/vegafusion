from . import SqlConnection, CsvReadOptions

from typing import Dict, Union
from distutils.version import LooseVersion

import duckdb
import pyarrow as pa
import pandas as pd
import logging


def duckdb_type_name_to_pyarrow_type(duckdb_type: str) -> pa.DataType:
    if duckdb_type in ("VARCHAR", "JSON", "CHAR"):
        return pa.string()
    elif duckdb_type in ("REAL", "FLOAT4", "FLOAT"):
        return pa.float32()
    elif duckdb_type in ("DOUBLE", "FLOAT8"):
        return pa.float64()
    elif duckdb_type in ("TINYINT", "INT1"):
        return pa.int8()
    elif duckdb_type in ("SMALLINT", "INT2", "SHORT"):
        return pa.int16()
    elif duckdb_type in ("INTEGER", "INT4", "INT", "SIGNED"):
        return pa.int32()
    elif duckdb_type in ("BIGINT", "INT8", "LONG"):
        return pa.int64()
    elif duckdb_type == "UTINYINT":
        return pa.uint8()
    elif duckdb_type == "USMALLINT":
        return pa.uint16()
    elif duckdb_type == "UINTEGER":
        return pa.uint32()
    elif duckdb_type == "UBIGINT":
        return pa.uint64()
    elif duckdb_type == "DOUBLE":
        return pa.float64()
    elif duckdb_type == "BOOLEAN":
        return pa.bool_()
    elif duckdb_type == "DATE":
        return pa.date32()
    elif duckdb_type == "TIMESTAMP":
        return pa.timestamp("ms")
    else:
        raise ValueError(f"Unexpected DuckDB type: {duckdb_type}")


def duckdb_relation_to_schema(rel: duckdb.DuckDBPyRelation) -> pa.Schema:
    return pa.schema({
        col: duckdb_type_name_to_pyarrow_type(type_name)
        for col, type_name in zip(rel.columns, rel.dtypes)
    })


class DuckDbConnection(SqlConnection):
    def __init__(self, connection: duckdb.DuckDBPyConnection = None, fallback: bool = True):
        # Validate duckdb version
        if LooseVersion(duckdb.__version__) < LooseVersion("0.7.0"):
            raise ImportError(
                f"The VegaFusion DuckDB connection requires at least DuckDB version 0.7.0\n"
                f"Found version {duckdb.__version__}"
            )

        self._fallback = fallback
        self._temp_tables = set()

        if connection is None:
            connection = duckdb.connect()
        self.conn = connection

        self.logger = logging.getLogger("DuckDbConnection")

        # Load config/extensions
        self.conn.install_extension("httpfs")
        self.conn.load_extension("httpfs")
        self.conn.install_extension("icu")
        self.conn.load_extension("icu")

        # Use a less round number for pandas_analyze_sample (default is 1000)
        self.conn.execute("SET GLOBAL pandas_analyze_sample=1007")

        self._table_schemas = dict()
        # Call self.tables to warm the cache of table schemas
        self.tables()

    @classmethod
    def dialect(cls) -> str:
        return "duckdb"

    def fallback(self) -> bool:
        return self._fallback

    def tables(self) -> Dict[str, pa.Schema]:
        result = {}
        table_names = self.conn.query(
            "select table_name from information_schema.tables"
        ).to_df()["table_name"].tolist()

        for table_name in table_names:
            if table_name in self._table_schemas:
                result[table_name] = self._table_schemas[table_name]
            else:
                rel = self.conn.query(f'select * from "{table_name}" limit 1')
                schema = duckdb_relation_to_schema(rel)
                self._table_schemas[table_name] = schema
                result[table_name] = schema

        return result

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        self.logger.info(f"Query:\n{query}\n")
        return self.conn.query(query).to_arrow_table(8096)

    def _update_temp_names(self, name: str, temporary: bool):
        if temporary:
            self._temp_tables.add(name)
        elif name in self._temp_tables:
            self._temp_tables.remove(name)

    def register_pandas(self, name: str, df: pd.DataFrame, temporary: bool = False):
        # Add _vf_order column to avoid the more expensive operation of computing it with a
        # ROW_NUMBER function in duckdb
        from ..transformer import to_arrow_table
        df = df.copy(deep=False)
        df["_vf_order"] = range(0, len(df))
        self.conn.register(name, df)
        self._update_temp_names(name, temporary)

    def register_arrow(self, name: str, table: pa.Table, temporary: bool = False):
        self.conn.register(name, table)
        self._update_temp_names(name, temporary)

        # We have the exact schema, save it in _table_schemas, so it doesn't need to
        # be constructed later
        self._table_schemas[name] = table.schema

    def register_json(self, name: str, path: str, temporary: bool = False):
        relation = self.conn.read_json(path)
        relation.to_view(name)
        self._update_temp_names(name, temporary)

    def register_csv(self, name: str, path: str, options: CsvReadOptions, temporary: bool = False):
        # TODO: handle schema from options
        relation = self.conn.read_csv(
            path,
            header=options.has_header,
            delimiter=options.delimeter,
        )
        relation.to_view(name)
        self._update_temp_names(name, temporary)

    def register_parquet(self, name: str, path: str, temporary: bool = False):
        relation = self.conn.read_parquet(path)
        relation.to_view(name)
        self._update_temp_names(name, temporary)

    def unregister(self, name: str):
        self.conn.unregister(name)
        if name in self._temp_tables:
            self._temp_tables.remove(name)
        self._table_schemas.pop(name, None)

    def unregister_temporary_tables(self):
        for name in list(self._temp_tables):
            self.conn.unregister(name)
            self._temp_tables.remove(name)
