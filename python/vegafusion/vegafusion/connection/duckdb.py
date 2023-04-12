import re
import warnings

from . import SqlConnection, CsvReadOptions

from typing import Dict, Optional
from distutils.version import LooseVersion

import duckdb
import pyarrow as pa
import pandas as pd
import logging
import uuid

# Table suffix name to use for raw registered table
RAW_PREFIX = "_vf_raw_"


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
    elif duckdb_type.startswith("DECIMAL"):
        matches = re.findall(r"\d+", duckdb_type)
        precision = int(matches[0])
        scale = int(matches[1])
        return pa.decimal128(precision, scale)
    elif duckdb_type == "UTINYINT":
        return pa.uint8()
    elif duckdb_type == "USMALLINT":
        return pa.uint16()
    elif duckdb_type == "UINTEGER":
        return pa.uint32()
    elif duckdb_type == "UBIGINT":
        return pa.uint64()
    elif duckdb_type == "BOOLEAN":
        return pa.bool_()
    elif duckdb_type == "DATE":
        return pa.date32()
    elif duckdb_type in ("TIMESTAMP", "TIMESTAMP_MS"):
        return pa.timestamp("ms")
    elif duckdb_type == "TIMESTAMP_NS":
        return pa.timestamp("ns")
    elif duckdb_type == "TIMESTAMP WITH TIME ZONE":
        return pa.timestamp("ms", tz="UTC")
    else:
        raise ValueError(f"Unexpected DuckDB type: {duckdb_type}")


def duckdb_relation_to_schema(rel: duckdb.DuckDBPyRelation) -> pa.Schema:
    schema_fields = {}
    for col, type_name in zip(rel.columns, rel.dtypes):
        try:
            type_ = duckdb_type_name_to_pyarrow_type(type_name)
            schema_fields[col] = type_
        except ValueError:
            # Skip columns with unrecognized types
            pass
    return pa.schema(schema_fields)


def pyarrow_type_to_duckdb_type_name(field_type: pa.Schema) -> Optional[str]:
    if field_type in (pa.utf8(), pa.large_utf8()):
        return "VARCHAR"
    elif field_type in (pa.float16(), pa.float32()):
        return "FLOAT"
    elif field_type == pa.float64():
        return "DOUBLE"
    elif field_type == pa.int8():
        return "TINYINT"
    elif field_type == pa.int16():
        return "SMALLINT"
    elif field_type == pa.int32():
        return "INTEGER"
    elif field_type == pa.int64():
        return "BIGINT"
    elif field_type == pa.uint8():
        return "UTINYINT"
    elif field_type == pa.uint16():
        return "USMALLINT"
    elif field_type == pa.uint32():
        return "UINTEGER"
    elif field_type == pa.uint64():
        return "UBIGINT"
    elif field_type == pa.bool_():
        return "BOOLEAN"
    elif field_type == pa.date32():
        return "DATE"
    else:
        return None


def pyarrow_schema_to_select_replace(schema: pa.Schema, table_name: str) -> str:
    """
    Build `SELECT * REPLACE(...) from table_name` query that casts columns
    to match the provided pyarrow schema.

    This is needed because sometimes the resulting DuckDB column types won't exactly
    match those that DataFusion expects (e.g. DuckDB returning a DECIMAL(5) instead of
    and int64). Types that are not covered by `pyarrow_type_to_duckdb_type_name` above
    are passed through as-is.
    """
    replaces = []
    for field_index in range(len(schema)):
        field = schema.field(field_index)
        field_name = field.name
        field_type = field.type

        quoted_column = quote_column(field_name)
        duckdb_type = pyarrow_type_to_duckdb_type_name(field_type)
        if duckdb_type:
            replaces.append(f"{quoted_column}::{duckdb_type} as {quoted_column}")

    if replaces:
        replace_csv = ", ".join(replaces)
        return f"SELECT * REPLACE({replace_csv}) FROM {table_name}"
    else:
        return f"SELECT * FROM {table_name}"


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

            # Use a less round number for pandas_analyze_sample (default is 1000)
            connection.execute("SET GLOBAL pandas_analyze_sample=1007")

        # The icu extension is pre-bundled in Python, so no need to install it
        connection.load_extension("icu")

        self.conn = connection
        self.logger = logging.getLogger("DuckDbConnection")

        self._registered_table_schemas = dict()
        # Call self.tables to warm the cache of table schemas
        self.tables()

    @classmethod
    def dialect(cls) -> str:
        return "duckdb"

    def fallback(self) -> bool:
        return self._fallback

    def _replace_query_for_table(self, table_name: str):
        """
        Build a `SELECT * REPLACE(...) FROM table_name` query for a table
        that converts unsupported column types to varchar columns
        """
        rel = self.conn.view(table_name)
        replaces = []
        for col, type_name in zip(rel.columns, rel.dtypes):
            quoted_col_name = quote_column(col)
            try:
                duckdb_type_name_to_pyarrow_type(type_name)
                # Skip columns with supported types
            except ValueError:
                # Convert unsupported types to strings (except struct)
                if not type_name.startswith("STRUCT"):
                    replaces.append(f"{quoted_col_name}::varchar as {quoted_col_name}")

        if replaces:
            replace_csv = ", ".join(replaces)
            return f"SELECT * REPLACE({replace_csv}) FROM {table_name}"
        else:
            return f"SELECT * FROM {table_name}"

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
            elif not table_name.startswith(RAW_PREFIX):
                # Dynamically look up schema for tables that are registered with duckdb but not with
                # the self.register_* methods. Skip raw tables
                result[table_name] = self._schema_for_table(table_name)

        return result

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        self.logger.info(f"Query:\n{query}\n")
        if self._verbose:
            print(f"DuckDB Query:\n{query}\n")

        # Save query result to temporary view
        tmp_table = "_duckdb_tmp_tbl" + str(uuid.uuid4()).replace("-", "_")
        self.conn.query(query).to_view(tmp_table)

        try:
            # Run replacement query to cast types to match schema
            replace_query = pyarrow_schema_to_select_replace(schema, tmp_table)
            result = self.conn.query(replace_query).to_arrow_table(8096)
        finally:
            # Unregister temporary view
            self.conn.unregister(tmp_table)

        return result

    def _update_temp_names(self, name: str, temporary: bool):
        if temporary:
            self._temp_tables.add(name)
        elif name in self._temp_tables:
            self._temp_tables.remove(name)

    def register_pandas(self, name: str, df: pd.DataFrame, temporary: bool = False):
        # Add _vf_order column to avoid the more expensive operation of computing it with a
        # ROW_NUMBER function in duckdb
        df = df.copy(deep=False)
        df["_vf_order"] = range(0, len(df))

        # Register raw DataFrame under name with prefix
        raw_name = RAW_PREFIX + name
        self.conn.register(raw_name, df)

        # then convert unsupported columns and register result as name
        replace_query = self._replace_query_for_table(raw_name)
        self.conn.query(replace_query).to_view(name)

        self._update_temp_names(name, temporary)
        self._registered_table_schemas[name] = self._schema_for_table(name)

    def register_arrow(self, name: str, table: pa.Table, temporary: bool = False):
        # Register raw table under name with prefix
        raw_name = RAW_PREFIX + name
        self.conn.register(raw_name, table)

        # then convert unsupported columns and register result as name
        replace_query = self._replace_query_for_table(raw_name)
        self.conn.query(replace_query).to_view(name)

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
        # Register raw table under name with prefix
        raw_name = RAW_PREFIX + name
        self.conn.read_parquet(path).to_view(raw_name)

        # then convert unsupported columns and register result as name
        replace_query = self._replace_query_for_table(raw_name)
        self.conn.query(replace_query).to_view(name)

        self._update_temp_names(name, temporary)
        self._registered_table_schemas[name] = self._schema_for_table(name)

    def unregister(self, name: str):
        for view_name in [name, RAW_PREFIX + name]:
            self.conn.unregister(view_name)
            if view_name in self._temp_tables:
                self._temp_tables.remove(view_name)
            self._registered_table_schemas.pop(view_name, None)

    def unregister_temporary_tables(self):
        for name in list(self._temp_tables):
            self.conn.unregister(name)
            self._temp_tables.remove(name)


def quote_column(name: str):
    return '"' + name.replace('"', '""') + '"'
