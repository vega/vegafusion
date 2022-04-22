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


def to_arrow_ipc_bytes(data, stream=False):
    """
    Helper to convert a Pandas DataFrame to the Arrow IPC binary format

    :param data: Pandas DataFrame
    :param stream: If True, write IPC Stream format. If Flase (defualt), write ipc file format.
    :return: bytes
    """
    import pyarrow as pa

    # Reset named index(ex) into a column
    if data.index.name is not None:
        data = data.reset_index()

    # Expand categoricals (not yet supported in VegaFusion)
    for col, dtype in data.dtypes.items():
        if isinstance(dtype, pd.CategoricalDtype):
            cat = data[col].cat
            data[col] = cat.categories[cat.codes]

    # Serialize DataFrame to bytes in the arrow file format
    try:
        table = pa.Table.from_pandas(data)
    except pa.ArrowTypeError as e:
        # Try converting object columns to strings to handle cases where a
        # column has a mix of numbers and strings
        mapping = dict()
        for col, dtype in data.dtypes.items():
            if dtype.kind == "O":
                mapping[col] = data[col].astype(str)
        data = data.assign(**mapping)
        # Try again, allowing exception to propagate
        table = pa.Table.from_pandas(data)

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
