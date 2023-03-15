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
    def __init__(self, inline_datasets: Dict[str, Union[pd.DataFrame, pa.Table]] = None, fallback: bool = True):
        # Validate duckdb version
        if LooseVersion(duckdb.__version__) < LooseVersion("0.7.0"):
            raise ImportError(
                f"The VegaFusion DuckDB connection requires at least DuckDB version 0.7.0\n"
                f"Found version {duckdb.__version__}"
            )

        self._fallback = fallback
        self._table_schemas = {}
        self._temp_table_schemas = {}
        self.conn = duckdb.connect()

        self.logger = logging.getLogger("DuckDbConnection")

        # Load config/extensions
        self.conn.install_extension("httpfs")
        self.conn.load_extension("httpfs")
        self.conn.install_extension("icu")
        self.conn.load_extension("icu")
        self.conn.execute("SET GLOBAL pandas_analyze_sample=100000")

        # Register inline_datasets
        for name, tbl in (inline_datasets or {}).items():
            if isinstance(tbl, pd.DataFrame):
                self.register_pandas(name, tbl)
            elif isinstance(tbl, pa.Table):
                self.register_arrow(name, tbl)
            else:
                raise ValueError(f"Unexpected Table type: {type(tbl)}")

    @classmethod
    def dialect(cls) -> str:
        return "duckdb"

    def fallback(self) -> bool:
        return self._fallback

    def tables(self) -> Dict[str, pa.Schema]:
        return dict(**self._table_schemas, **self._temp_table_schemas)

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        self.logger.info(f"Query:\n{query}\n")
        return self.conn.query(query).to_arrow_table(8096)

    def register_pandas(self, name: str, df: pd.DataFrame, temporary: bool = False):
        # Add _vf_order column to avoid the more expensive operation of computing it with a
        # ROW_NUMBER function in duckdb
        from ..transformer import to_arrow_table
        df = df.copy(deep=False)
        df["_vf_order"] = range(0, len(df))
        self.conn.register(name, df)
        schema = to_arrow_table(df.head(100)).schema
        if temporary:
            self._temp_table_schemas[name] = schema
        else:
            self._table_schemas[name] = schema

    def register_arrow(self, name: str, table: pa.Table, temporary: bool = False):
        self.conn.register(name, table)
        if temporary:
            self._temp_table_schemas[name] = table.schema
        else:
            self._table_schemas[name] = table.schema

    def register_json(self, name: str, path: str, temporary: bool = False):
        relation = self.conn.read_json(path)
        relation.to_view(name)
        schema = duckdb_relation_to_schema(relation)
        if temporary:
            self._temp_table_schemas[name] = schema
        else:
            self._table_schemas[name] = schema

    def register_csv(self, name: str, path: str, options: CsvReadOptions, temporary: bool = False):
        # TODO: handle schema from options
        relation = self.conn.read_csv(
            path,
            header=options.has_header,
            delimiter=options.delimeter,
        )
        relation.to_view(name)
        schema = duckdb_relation_to_schema(relation)
        if temporary:
            self._temp_table_schemas[name] = schema
        else:
            self._table_schemas[name] = schema

    def register_parquet(self, name: str, path: str, temporary: bool = False):
        relation = self.conn.read_parquet(path)
        relation.to_view(name)
        schema = duckdb_relation_to_schema(relation)
        if temporary:
            self._temp_table_schemas[name] = schema
        else:
            self._table_schemas[name] = schema

    def unregister(self, name: str):
        if name in self._table_schemas:
            self.conn.unregister(name)
            del self._table_schemas[name]
        elif name in self._temp_table_schemas:
            self.conn.unregister(name)
            del self._temp_table_schemas[name]
        else:
            ValueError(f"No registered table named {name}")

    def unregister_temporary_tables(self):
        for name in list(self._temp_table_schemas):
            self.conn.unregister(name)
            del self._temp_table_schemas[name]
