import logging
import pyarrow as pa
from .sql import SqlDataset
from snowflake.snowpark import Table as SnowparkTable
from snowflake.snowpark.types import DataType as SnowparkDataType
from typing import Dict

from ..transformer import to_arrow_table

SNOWPARK_TO_PYARROW_TYPES: Dict[SnowparkDataType, pa.DataType] = {}


def get_snowpark_to_pyarrow_types():
    if not SNOWPARK_TO_PYARROW_TYPES:
        import snowflake.snowpark.types as sp_types

        SNOWPARK_TO_PYARROW_TYPES.update(
            {
                sp_types.LongType: pa.int64(),
                sp_types.BinaryType: pa.binary(),
                sp_types.BooleanType: pa.bool_(),
                sp_types.ByteType: pa.int8(),
                sp_types.StringType: pa.string(),
                sp_types.DateType: pa.date32(),
                sp_types.DoubleType: pa.float64(),
                sp_types.FloatType: pa.float32(),
                sp_types.IntegerType: pa.int32(),
                sp_types.ShortType: pa.int16(),
                sp_types.TimestampType: pa.timestamp("ms"),
            }
        )
    return SNOWPARK_TO_PYARROW_TYPES


def snowflake_field_to_pyarrow_type(provided_type: SnowparkDataType) -> pa.DataType:
    """
    Converts Snowflake types to PyArrow equivalent types, raising a ValueError if they aren't comparable.
    See https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
    """
    from snowflake.snowpark.types import DecimalType as SnowparkDecimalType

    type_map = get_snowpark_to_pyarrow_types()
    if provided_type.__class__ in type_map:
        return type_map[provided_type.__class__]

    if isinstance(provided_type, SnowparkDecimalType):
        return pa.decimal128(provided_type.precision, provided_type.scale)
    else:
        raise ValueError(f"Unsupported Snowpark type: {provided_type}")


def snowpark_table_to_pyarrow_schema(table: SnowparkTable) -> pa.Schema:
    schema_fields = {}
    for name, field in zip(table.schema.names, table.schema.fields):
        normalised_name = name.strip('"')
        schema_fields[normalised_name] = snowflake_field_to_pyarrow_type(field.datatype)
    return pa.schema(schema_fields)


class SnowparkDataset(SqlDataset):
    def dialect(self) -> str:
        return "snowflake"

    def __init__(
        self, table: SnowparkTable, fallback: bool = True, verbose: bool = False
    ):
        if not isinstance(table, SnowparkTable):
            raise ValueError(
                f"SnowparkDataset accepts a snowpark Table. Received: {type(table)}"
            )
        self._table = table
        self._session = table._session

        self._fallback = fallback
        self._verbose = verbose
        self._table_name = table.table_name
        self._table_schema = snowpark_table_to_pyarrow_schema(self._table)

        self.logger = logging.getLogger("SnowparkDataset")

    def table_name(self) -> str:
        return self._table_name

    def table_schema(self) -> pa.Schema:
        return self._table_schema

    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        self.logger.info(f"Snowflake Query:\n{query}\n")
        if self._verbose:
            print(f"Snowflake Query:\n{query}\n")

        sp_df = self._session.sql(query)
        batches = []
        for pd_batch in sp_df.to_pandas_batches():
            pa_tbl = to_arrow_table(pd_batch).cast(schema, safe=False)
            batches.extend(pa_tbl.to_batches())

        return pa.Table.from_batches(batches, schema)

    def fallback(self) -> bool:
        return self._fallback
