from typing import Iterable
import re
import pyarrow as pa
from pyarrow.interchange import from_dataframe

from typing import Any, Dict
from ._dfi_types import DtypeKind, DataFrame as DfiDataFrame
from .datasource import Datasource

# Taken from private pyarrow utilities
_PYARROW_DTYPES: Dict[DtypeKind, Dict[int, Any]] = {
    DtypeKind.INT: {8: pa.int8(),
                    16: pa.int16(),
                    32: pa.int32(),
                    64: pa.int64()},
    DtypeKind.UINT: {8: pa.uint8(),
                     16: pa.uint16(),
                     32: pa.uint32(),
                     64: pa.uint64()},
    DtypeKind.FLOAT: {16: pa.float16(),
                      32: pa.float32(),
                      64: pa.float64()},
    DtypeKind.BOOL: {1: pa.bool_(),
                     8: pa.uint8()},
    DtypeKind.STRING: {8: pa.string()},
}


def parse_datetime_format_str(format_str):
    """Parse datetime `format_str` to interpret the `data`."""

    # timestamp 'ts{unit}:tz'
    timestamp_meta = re.match(r"ts([smun]):(.*)", format_str)
    if timestamp_meta:
        unit, tz = timestamp_meta.group(1), timestamp_meta.group(2)
        if unit != "s":
            # the format string describes only a first letter of the unit, so
            # add one extra letter to convert the unit to numpy-style:
            # 'm' -> 'ms', 'u' -> 'us', 'n' -> 'ns'
            unit += "s"

        return unit, tz

    raise NotImplementedError(f"DateTime kind is not supported: {format_str}")


def map_date_type(data_type):
    """Map column date type to pyarrow date type. """
    kind, bit_width, f_string, _ = data_type

    if kind == DtypeKind.DATETIME:
        unit, tz = parse_datetime_format_str(f_string)
        return pa.timestamp(unit, tz=tz)
    else:
        pa_dtype = _PYARROW_DTYPES.get(kind, {}).get(bit_width, None)

        # Error if dtype is not supported
        if pa_dtype:
            return pa_dtype
        else:
            raise NotImplementedError(
                f"Conversion for {data_type} is not yet supported.")


class DfiDatasource(Datasource):
    def __init__(self, dataframe: DfiDataFrame):
        if hasattr(dataframe, "__dataframe__"):
            dataframe = dataframe.__dataframe__()
        fields = []
        for col in dataframe.column_names():
            field = dataframe.get_column_by_name(col)
            pa_type = map_date_type(field.dtype)
            fields.append(pa.field(col, pa_type))

        self._dataframe = dataframe
        self._schema = pa.schema(fields)

    def schema(self) -> pa.Schema:
        return self._schema

    def fetch(self, columns: Iterable[str]) -> pa.Table:
        columns = list(columns)
        projected_schema = pa.schema([f for f in self._schema if f.name in columns])
        table = from_dataframe(self._dataframe.select_columns_by_name(columns))
        return table.cast(projected_schema, safe=False)
