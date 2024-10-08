from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.interchange as pi


DATASET_PREFIXES: tuple[str, ...] = ("vegafusion+dataset://", "table://")
BATCH_SIZE: int = 8096

DataFrameLike = Any


def to_arrow_table(data: DataFrameLike) -> pa.Table:
    """
    Helper to convert a pandas DataFrame to a PyArrow Table.

    Args:
        data: pandas DataFrame.

    Returns:
        pyarrow.Table: The converted PyArrow Table.
    """
    import pyarrow as pa

    pd = sys.modules.get("pandas")

    # Reset named index(ex) into a column
    if getattr(data.index, "name", None) is not None:
        data = data.reset_index()

    # Use pyarrow to infer schema from DataFrame
    for col, pd_type in data.dtypes.items():
        try:
            candidate_schema = pa.Schema.from_pandas(data[[col]])

            # Get inferred pyarrow type
            pa_type = candidate_schema.field(col).type

            # Convert Decimal columns to float
            if isinstance(pa_type, (pa.Decimal128Type, pa.Decimal256Type)):
                data = data.assign(**{col: data[col].astype("float64")})

        except pa.ArrowTypeError:
            if pd_type.kind == "O":
                # Try converting object columns to strings to handle cases where a
                # column has a mix of numbers and strings
                data = data.assign(**{col: data[col].astype(str)})

        # Expand categoricals (not yet supported in VegaFusion)
        if pd is not None and isinstance(pd_type, pd.CategoricalDtype):
            cat = data[col].cat
            data = data.assign(**{col: cat.categories[cat.codes]})

        # Copy un-aligned columns to align them
        # (arrow-rs seems to have trouble with un-aligned arrays)
        values = getattr(data[col], "values", None)
        if values is not None:
            flags = getattr(values, "flags", None)
            if flags is not None:
                try:
                    aligned = flags["ALIGNED"]
                    if not aligned:
                        data = data.assign(**{col: data[col].copy()})
                except IndexError:
                    pass

    # Convert DataFrame to table. Keep index only if named
    preserve_index = bool([name for name in getattr(data.index, "names", []) if name])
    table = pa.Table.from_pandas(data, preserve_index=preserve_index)

    # Split into batches of desired size
    batches = table.to_batches(BATCH_SIZE)
    table = pa.Table.from_batches(batches, table.schema)

    return table


def to_arrow_ipc_bytes(data: DataFrameLike, stream: bool = False) -> bytes:
    """
    Helper to convert a DataFrame to the Arrow IPC binary format.

    Args:
        data: Pandas DataFrame, pyarrow Table, or object that supports
            the DataFrame Interchange Protocol.
        stream: If True, write IPC Stream format. If False (default), write ipc
            file format.

    Returns:
        bytes: The Arrow IPC binary format data.

    Raises:
        ValueError: If the input data type is unsupported.
    """
    pa = sys.modules.get("pyarrow", None)
    pd = sys.modules.get("pandas", None)
    if pd is not None and isinstance(data, pd.DataFrame):
        table = to_arrow_table(data)
    elif pa is not None and isinstance(data, pa.Table):
        table = data
    elif hasattr(data, "__dataframe__"):
        pi = import_pyarrow_interchange()
        table = pi.from_dataframe(data)
    else:
        raise ValueError(f"Unsupported input to to_arrow_ipc_bytes: {type(data)}")
    return arrow_table_to_ipc_bytes(table, stream=stream)


def arrow_table_to_ipc_bytes(table: pa.Table, stream: bool = False) -> bytes:
    import pyarrow as pa

    # Next we write the Arrow table as a feather file (The Arrow IPC format on disk).
    # Write it in memory first so we can hash the contents before touching disk.
    bytes_buffer = io.BytesIO()

    if stream:
        with pa.ipc.new_stream(bytes_buffer, table.schema) as f:
            f.write_table(table, max_chunksize=BATCH_SIZE)
    else:
        with pa.ipc.new_file(bytes_buffer, table.schema) as f:
            f.write_table(table, max_chunksize=BATCH_SIZE)

    return bytes_buffer.getvalue()


def to_feather(data: DataFrameLike, file: Union[str, io.IOBase]) -> None:
    """
    Helper to convert a Pandas DataFrame to a feather file that is optimized for
    use as a VegaFusion data source.

    Args:
        data: Pandas DataFrame, pyarrow Table, or object that supports
            the DataFrame Interchange Protocol.
        file: File path string or writable file-like object.
    """
    file_bytes = to_arrow_ipc_bytes(data, stream=False)

    # Either write to new file at path, or to writable file-like object
    if hasattr(file, "write"):
        file.write(file_bytes)
    else:
        with open(file, "wb") as f:
            f.write(file_bytes)


def import_pyarrow_interchange() -> pi:
    """
    Import pyarrow.interchange module.

    Returns:
        pyarrow.interchange: The pyarrow.interchange module.

    Raises:
        ImportError: If pyarrow version is less than 11.0.0.
    """
    try:
        import pyarrow.interchange as pi

        return pi
    except ImportError as e:
        import pyarrow as pa

        raise ImportError(
            "Use of the DataFrame Interchange Protocol requires at least "
            f"version 11.0.0 of pyarrow\nFound version {pa.__version__}"
        ) from e
