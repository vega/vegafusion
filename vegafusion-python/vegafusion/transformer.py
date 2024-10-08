import io
import sys
from weakref import WeakValueDictionary

DATASET_PREFIXES = ("vegafusion+dataset://", "table://")
BATCH_SIZE = 8096


def to_arrow_table(data):
    """
    Helper to convert a Pandas DataFrame to a PyArrow Table

    :param data: Pandas DataFrame
    :return: pyarrow.Table
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


def to_arrow_ipc_bytes(data, stream=False):
    """
    Helper to convert a DataFrame to the Arrow IPC binary format

    :param data: Pandas DataFrame, pyarrow Table, or object that supports
        the DataFrame Interchange Protocol
    :param stream: If True, write IPC Stream format. If False (default), write ipc file format.
    :return: bytes
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


def arrow_table_to_ipc_bytes(table, stream=False):
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
                name = url[len(prefix) :]
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


def is_dataframe_like(data):
    pa = sys.modules.get("pyarrow")
    pd = sys.modules.get("pandas")
    is_pa_table = pa is not None and isinstance(data, pa.Table)
    is_pd_table = pd is not None and isinstance(data, pd.DataFrame)
    return is_pa_table or is_pd_table or hasattr(data, "__dataframe__")


def has_geo_interface(data):
    return hasattr(data, "__geo_interface__")


def import_pyarrow_interchange():
    try:
        import pyarrow.interchange as pi

        return pi
    except ImportError:
        import pyarrow as pa

        raise ImportError(
            "Use of the DataFrame Interchange Protocol requires at least version 11.0.0 of pyarrow\n"
            f"Found version {pa.__version__}"
        )
