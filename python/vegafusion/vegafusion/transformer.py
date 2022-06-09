# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.

import datetime
import io
import os
import pathlib
from hashlib import sha1
from tempfile import NamedTemporaryFile

import altair as alt
import pandas as pd


def to_arrow_table(data):
    """
    Helper to convert a Pandas DataFrame to a PyArrow Table

    :param data: Pandas DataFrame
    :return: pyarrow.Table
    """
    import pyarrow as pa

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
        if isinstance(pd_type, pd.CategoricalDtype):
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

    # Convert DataFrame to table
    table = pa.Table.from_pandas(data)

    return table


def to_arrow_ipc_bytes(data, stream=False):
    """
    Helper to convert a Pandas DataFrame to the Arrow IPC binary format

    :param data: Pandas DataFrame
    :param stream: If True, write IPC Stream format. If False (default), write ipc file format.
    :return: bytes
    """
    table = to_arrow_table(data)
    return arrow_table_to_ipc_bytes(table, stream=stream)


def arrow_table_to_ipc_bytes(table, stream=False):
    import pyarrow as pa

    # Next we write the Arrow table as a feather file (The Arrow IPC format on disk).
    # Write it in memory first so we can hash the contents before touching disk.
    bytes_buffer = io.BytesIO()

    max_chunksize=8096

    if stream:
        with pa.ipc.new_stream(bytes_buffer, table.schema) as f:
            f.write_table(table, max_chunksize=max_chunksize)
    else:
        with pa.ipc.new_file(bytes_buffer, table.schema) as f:
            f.write_table(table, max_chunksize=max_chunksize)

    return bytes_buffer.getvalue()


def to_feather(data, file):
    """
    Helper to convert a Pandas DataFrame to a feather file that is optimized for
    use as a VegaFusion data source

    :param data: Pandas DataFrame
    :param file: File path string or writable file-like object
    :return: None
    """
    file_bytes = to_arrow_ipc_bytes(data, stream=False)

    # Either write to new file at path, or to writable file-like object
    if hasattr(file, "write"):
        file.write(file_bytes)
    else:
        with open(file, "wb") as f:
            f.write(file_bytes)


def feather_transformer(data, data_dir="_vegafusion_data"):
    from vegafusion import runtime
    if "vegafusion" not in alt.renderers.active or not isinstance(data, pd.DataFrame):
        # Use default transformer if a vegafusion renderer is not active
        return alt.default_data_transformer(data)
    else:
        if runtime.using_grpc:
            raise ValueError(
                "DataFrame data sources are not yet supported by the gRPC runtime"
            )

        bytes_buffer = io.BytesIO()
        to_feather(data, bytes_buffer)
        file_bytes = bytes_buffer.getvalue()

        # Hash bytes to generate unique file name
        hasher = sha1()
        hasher.update(file_bytes)
        hashstr = hasher.hexdigest()
        fname = f"vegafusion-{hashstr}.feather"

        # Check if file already exists
        tmp_dir = pathlib.Path(data_dir) / "tmp"
        os.makedirs(tmp_dir, exist_ok=True)
        path = pathlib.Path(data_dir) / fname
        if not path.is_file():
            # Write to temporary file then move (os.replace) to final destination. This is more resistant
            # to race conditions
            with NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_file:
                tmp_file.write(file_bytes)
                tmp_name = tmp_file.name

            os.replace(tmp_name, path)

        return {"url": path.as_posix()}


alt.data_transformers.register("vegafusion-feather", feather_transformer)
