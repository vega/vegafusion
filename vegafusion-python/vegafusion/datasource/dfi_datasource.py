from typing import Iterable, TYPE_CHECKING
import re

from ._dfi_types import DtypeKind, DataFrame as DfiDataFrame
from .datasource import Datasource

if TYPE_CHECKING:
    import pyarrow as pa

def get_pyarrow_dtype(kind, bit_width):
    import pyarrow as pa
    if kind == DtypeKind.INT:
        if bit_width == 8:
            return pa.int8()
        elif bit_width == 16:
            return pa.int16()
        elif bit_width == 32:
            return pa.int32()
        elif bit_width == 64:
            return pa.int64()

    elif kind == DtypeKind.UINT:
        if bit_width == 8:
            return pa.uint8()
        elif bit_width == 16:
            return pa.uint16()
        elif bit_width == 32:
            return pa.uint32()
        elif bit_width == 64:
            return pa.uint64()

    elif kind == DtypeKind.FLOAT:
        if bit_width == 16:
            return pa.float16()
        elif bit_width == 32:
            return pa.float32()
        elif bit_width == 64:
            return pa.float64()

    elif kind == DtypeKind.BOOL:
        if bit_width == 1:
            return pa.bool_()
        elif bit_width == 8:
            return pa.uint8()
    elif kind == DtypeKind.STRING:
        return pa.string()

    return None


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
    import pyarrow as pa
    kind, bit_width, f_string, _ = data_type

    if kind == DtypeKind.DATETIME:
        unit, tz = parse_datetime_format_str(f_string)
        return pa.timestamp(unit, tz=tz)
    else:
        pa_dtype = get_pyarrow_dtype(kind, bit_width)

        # Error if dtype is not supported
        if pa_dtype:
            return pa_dtype
        else:
            raise NotImplementedError(
                f"Conversion for {data_type} is not yet supported.")


class DfiDatasource(Datasource):
    def __init__(self, dataframe: DfiDataFrame):
        import pyarrow as pa
        if hasattr(dataframe, "__dataframe__"):
            dataframe = dataframe.__dataframe__()
        fields = []
        for col in dataframe.column_names():
            field = dataframe.get_column_by_name(col)
            pa_type = map_date_type(field.dtype)
            fields.append(pa.field(col, pa_type))

        self._dataframe = dataframe
        self._schema = pa.schema(fields)

    def schema(self) -> "pa.Schema":
        return self._schema

    def fetch(self, columns: Iterable[str]) -> "pa.Table":
        import pyarrow as pa
        from pyarrow.interchange import from_dataframe
        columns = list(columns)
        projected_schema = pa.schema([f for f in self._schema if f.name in columns])
        table = from_dataframe(self._dataframe.select_columns_by_name(columns))
        return table.cast(projected_schema, safe=False)
