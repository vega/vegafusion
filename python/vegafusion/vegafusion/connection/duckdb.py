import warnings

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
    def __init__(self, connection: duckdb.DuckDBPyConnection = None, fallback: bool = True, verbose: bool = False):
        # Validate duckdb version
        if LooseVersion(duckdb.__version__) < LooseVersion("0.7.0"):
            raise ImportError(
                f"The VegaFusion DuckDB connection requires at least DuckDB version 0.7.0\n"
                f"Found version {duckdb.__version__}"
            )

        self._fallback = fallback
        self._verbose = verbose
        self._temp_tables = set()

        if connection is None:
            connection = duckdb.connect()

            # Install and load the httpfs extension only if we are creating the duckdb connection
            # here. If a connection was passed in, don't assume it has internet access and the
            # ability to install extensions
            try:
                connection.install_extension("httpfs")
                connection.load_extension("httpfs")
            except (IOError, duckdb.IOException, duckdb.InvalidInputException) as e:
                warnings.warn(f"Failed to install and load the DuckDB httpfs extension:\n{e}")

        # The icu extension is pre-bundled in Python, so no need to install it
        connection.load_extension("icu")

        self.conn = connection
        self.logger = logging.getLogger("DuckDbConnection")

        # Use a less round number for pandas_analyze_sample (default is 1000)
        self.conn.execute("SET GLOBAL pandas_analyze_sample=1007")

        self._registered_table_schemas = dict()
        # Call self.tables to warm the cache of table schemas
        self.tables()

    @classmethod
    def dialect(cls) -> str:
        return "duckdb"

    def fallback(self) -> bool:
        return self._fallback

    def _schema_for_table(self, table_name: str):
        rel = self.conn.query(f'select * from "{table_name}" limit 1')
        return duckdb_relation_to_schema(rel)

    def tables(self) -> Dict[str, pa.Schema]:
        result = {}
        table_names = self.conn.query(
            "select table_name from information_schema.tables"
        ).to_df()["table_name"].tolist()

        for table_name in table_names:
            if table_name in self._registered_table_schemas:
                # Registered tables are expected to only change when self.register_* is called,
                # so use the cached version
                result[table_name] = self._registered_table_schemas[table_name]
            else:
                # Dynamically look up schema for tables that are registered with duckdb but now with
                # the self.register_* methods
                result[table_name] = self._schema_for_table(table_name)

        return result

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        self.logger.info(f"Query:\n{query}\n")
        if self._verbose:
            print(f"DuckDB Query:\n{query}\n")
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
        self._registered_table_schemas[name] = self._schema_for_table(name)

    def register_arrow(self, name: str, table: pa.Table, temporary: bool = False):
        self.conn.register(name, table)
        self._update_temp_names(name, temporary)
        self._registered_table_schemas[name] = table.schema

    def register_json(self, name: str, path: str, temporary: bool = False):
        relation = self.conn.read_json(path)
        relation.to_view(name)
        self._update_temp_names(name, temporary)
        self._registered_table_schemas[name] = self._schema_for_table(name)

    def register_csv(self, name: str, path: str, options: CsvReadOptions, temporary: bool = False):
        relation = self.conn.read_csv(
            path,
            header=options.has_header,
            delimiter=options.delimeter,
            all_varchar=True,
        )
        if options.schema is not None:
            replaces = []
            for col_name in options.schema.names:
                field = options.schema.field_by_name(col_name)
                quoted_col_name = quote_column(col_name)
                dtype = field.type
                if dtype == pa.string():
                    # Everything is loaded as string
                    pass
                elif dtype in (pa.float16(), pa.float32(), pa.float64()):
                    replaces.append(f"{quoted_col_name}::double as {quoted_col_name}")
                elif dtype in (pa.int8(), pa.int16(), pa.int32(), pa.int64()):
                    replaces.append(f"{quoted_col_name}::int as {quoted_col_name}")

            if replaces:
                query = f"SELECT * REPLACE({','.join(replaces)}) from _tbl"
                relation = relation.query("_tbl", query)

        relation.to_view(name)
        self._update_temp_names(name, temporary)
        self._registered_table_schemas[name] = self._schema_for_table(name)

    def register_parquet(self, name: str, path: str, temporary: bool = False):
        relation = self.conn.read_parquet(path)
        relation.to_view(name)
        self._update_temp_names(name, temporary)
        self._registered_table_schemas[name] = self._schema_for_table(name)

    def unregister(self, name: str):
        self.conn.unregister(name)
        if name in self._temp_tables:
            self._temp_tables.remove(name)
        self._registered_table_schemas.pop(name, None)

    def unregister_temporary_tables(self):
        for name in list(self._temp_tables):
            self.conn.unregister(name)
            self._temp_tables.remove(name)


def quote_column(name: str):
    return '"' + name.replace('"', '""') + '"'
