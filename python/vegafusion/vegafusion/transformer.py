# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.

import io
import os
import pathlib
from hashlib import sha1
from tempfile import NamedTemporaryFile
import uuid
import altair as alt
import pandas as pd
import pyarrow as pa
from weakref import WeakValueDictionary

DATASET_PREFIXES = ("vegafusion+dataset://", "table://")

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

    # Convert DataFrame to table. Keep index only if named
    preserve_index = bool([name for name in getattr(data.index, "names", []) if name])
    table = pa.Table.from_pandas(data, preserve_index=preserve_index)

    return table


def to_arrow_ipc_bytes(data, stream=False):
    """
    Helper to convert a DataFrame to the Arrow IPC binary format

    :param data: Pandas DataFrame, pyarrow Table, or object that supports
        the DataFrame Interchange Protocol
    :param stream: If True, write IPC Stream format. If False (default), write ipc file format.
    :return: bytes
    """
    if isinstance(data, pd.DataFrame):
        table = to_arrow_table(data)
    elif isinstance(data, pa.Table):
        table = data
    elif hasattr(data, "__dataframe__"):
        pi = import_pyarrow_interchange()
        table = pi.from_dataframe(data)
    else:
        raise ValueError(f"Unsupported input to to_arrow_ipc_bytes: {type(data)}")
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

    :param data: Pandas DataFrame, pyarrow Table, or object that supports
        the DataFrame Interchange Protocol
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

    if "vegafusion" not in alt.renderers.active:
        # Use default transformer if a vegafusion renderer is not active
        return alt.default_data_transformer(data)
    elif has_geo_interface(data):
        # Use default transformer for geo interface objects
        # (e.g. a geopandas GeoDataFrame)
        return alt.default_data_transformer(data)
    elif is_dataframe_like(data):
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
    else:
        # Use default transformer if we don't recognize data
        return alt.default_data_transformer(data)


def get_inline_dataset_names(vega_spec):
    """
    Get set of the inline datasets names in the provided spec

    :param vega_spec: Vega spec
    :return: set of inline dataset names
    """
    table_names = set()
    for data in vega_spec.get("data", []):
        url = data.get("url", "")
        for prefix in DATASET_PREFIXES:
            if url.startswith(prefix):
                name = url[len(prefix):]
                table_names.add(name)

    for mark in vega_spec.get("marks", []):
        table_names.update(get_inline_dataset_names(mark))

    return table_names


__inline_tables = WeakValueDictionary()


def get_inline_dataset_table(table_name):
    return __inline_tables.pop(table_name)


def get_inline_datasets_for_spec(vega_spec):
    table_names = get_inline_dataset_names(vega_spec)
    datasets = {}
    for table_name in table_names:
        try:
            datasets[table_name] = get_inline_dataset_table(table_name)
        except KeyError:
            # named dataset that was provided by the user
            pass
    return datasets


def inline_data_transformer(data):
    if has_geo_interface(data):
        # Use default transformer for geo interface objects
        # # (e.g. a geopandas GeoDataFrame)
        return alt.default_data_transformer(data)
    elif is_dataframe_like(data):
        table_name = f"table_{uuid.uuid4()}".replace("-", "_")
        __inline_tables[table_name] = data
        return {"url": DATASET_PREFIXES[0] + table_name}
    else:
        # Use default transformer if we don't recognize data
        return alt.default_data_transformer(data)


def is_dataframe_like(data):
    return isinstance(data, (pd.DataFrame, pa.Table)) or hasattr(data, "__dataframe__")


def has_geo_interface(data):
    return hasattr(data, "__geo_interface__")


def import_pyarrow_interchange():
    try:
        import pyarrow.interchange as pi
        return pi
    except ImportError:
        raise ImportError(
            "Use of the DataFrame Interchange Protocol requires at least version 11.0.0 of pyarrow\n"
            f"Found version {pa.__version__}"
        )


alt.data_transformers.register("vegafusion-feather", feather_transformer)
alt.data_transformers.register("vegafusion-inline", inline_data_transformer)
