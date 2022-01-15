# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import io
import os
import pathlib
import time
from hashlib import sha1
from tempfile import NamedTemporaryFile

import altair as alt
import pandas as pd


def to_feather(data, file):
    """
    Helper to convert a Pandas DataFrame to a feather file that is optimized for
    use as a VegaFusion data source

    :param data: Pandas DataFrame
    :param file: File path string or writable file-like object
    :return: None
    """
    import pyarrow as pa

    # Reset named index(ex) into a column
    if data.index.name is not None:
        data = data.reset_index()

    # Localize naive datetimes to the local GMT offset
    dt_cols = []
    for col, dtype in data.dtypes.items():
        if dtype.kind == 'M' and not isinstance(dtype, pd.DatetimeTZDtype):
            dt_cols.append(col)

    if dt_cols:
        # Apply a timezone following the convention of JavaScript's Date.parse. Here a date without time info
        # is interpreted as UTC midnight. But a date with time into is treated as local time when it doesn't
        # have an explicit timezone
        offset_seconds = abs(time.timezone)
        offset_hours = offset_seconds // 3600
        offset_minutes = (offset_seconds - offset_hours * 3600) // 60
        sign = "-" if time.timezone > 0 else "+"
        local_timezone = f"{sign}{offset_hours:02}:{offset_minutes:02}"

        mapping = dict()
        for col in dt_cols:
            if (data[col].dt.time == datetime.time(0, 0)).all():
                # Assume no time info was provided, interpret as UTC
                mapping[col] = data[col].dt.tz_localize("+00:00")
            else:
                # Assume time info was provided, interpret as local
                mapping[col] = data[col].dt.tz_localize(local_timezone).dt.tz_convert(None)

        data = data.assign(**mapping)

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

    with pa.ipc.new_file(bytes_buffer, table.schema) as f:
        f.write_table(table, max_chunksize=8096)

    file_bytes = bytes_buffer.getvalue()

    # Either write to new file at path, or to writable file-like object
    if hasattr(file, "write"):
        file.write(file_bytes)
    else:
        with open(file, "wb") as f:
            f.write(file_bytes)


def feather_transformer(data, data_dir="_vegafusion_data"):
    if alt.renderers.active != "vegafusion" or not isinstance(data, pd.DataFrame):
        # Use default transformer if the vegafusion renderer is not active
        return alt.default_data_transformer(data)
    else:
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


alt.data_transformers.register('vegafusion-feather', feather_transformer)

