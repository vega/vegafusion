import pandas as pd
from ipywidgets import DOMWidget
import time
from traitlets import Unicode


from ._frontend import module_name, module_version
import altair as alt
import io
import os
import pathlib
from tempfile import NamedTemporaryFile
from hashlib import sha1

from .runtime import runtime


class VegaFusionWidget(DOMWidget):
    _model_name = Unicode('VegaFusionModel').tag(sync=True)
    _model_module = Unicode(module_name).tag(sync=True)
    _model_module_version = Unicode(module_version).tag(sync=True)
    _view_name = Unicode('VegaFusionView').tag(sync=True)
    _view_module = Unicode(module_name).tag(sync=True)
    _view_module_version = Unicode(module_version).tag(sync=True)

    spec = Unicode(None, allow_none=True).tag(sync=True)
    full_vega_spec = Unicode(None, allow_none=True, read_only=True).tag(sync=True)
    client_vega_spec = Unicode(None, allow_none=True, read_only=True).tag(sync=True)
    server_vega_spec = Unicode(None, allow_none=True, read_only=True).tag(sync=True)
    comm_plan = Unicode(None, allow_none=True, read_only=True).tag(sync=True)

    def __init__(self, *args, **kwargs):

        # Support altair object as single positional argument
        if len(args) == 1:
            chart = args[0]
            spec = chart.to_json()
            kwargs["spec"] = spec

        super().__init__(**kwargs)

        # Wire up widget message callback
        self.on_msg(self._handle_message)

    def _handle_message(self, widget, msg, buffers):
        # print(msg)
        if msg['type'] == "request":
            # print("py: handle request")
            # Build response
            response_bytes = runtime.process_request_bytes(
                buffers[0]
            )
            # print("py: send response")
            self.send(dict(type="response"), [response_bytes])


def vegafusion_renderer(spec):
    import json
    from IPython.display import display

    # Display widget as a side effect, then return empty string text representation
    # so that Altair doesn't also display a string representation
    widget = VegaFusionWidget(spec=json.dumps(spec))
    display(widget)
    return {'text/plain': ""}


alt.renderers.register('vegafusion', vegafusion_renderer)
alt.renderers.enable('vegafusion')


def arrow_transformer(data, data_dir="_vegafusion_data"):
    import pyarrow as pa

    if alt.renderers.active != "vegafusion" or not isinstance(data, pd.DataFrame):
        # Use default transformer if the vegafusion renderer is not active
        return alt.default_data_transformer(data)
    else:

        # Reset named index(ex) into a column
        if data.index.name is not None:
            data = data.reset_index()

        # Localize naive datetimes to the local GMT offset
        dt_cols = []
        for col, dtype in data.dtypes.items():
            if dtype.kind == 'M' and not isinstance(dtype, pd.DatetimeTZDtype):
                dt_cols.append(col)

        if dt_cols:
            offset_seconds = abs(time.timezone)
            offset_hours = offset_seconds // 3600
            offset_minutes = (offset_seconds - offset_hours * 3600) // 60
            sign = "-" if time.timezone > 0 else "+"
            local_timezone = f"{sign}{offset_hours:02}:{offset_minutes:02}"

            mapping = dict()
            for col in dt_cols:
                mapping[col] = data[col].dt.tz_localize(local_timezone)

            data = data.assign(**mapping)

        # Serialize DataFrame to bytes in the arrow file format
        table = pa.Table.from_pandas(data)
        bytes_buffer = io.BytesIO()

        with pa.ipc.new_file(bytes_buffer, table.schema) as f:
            f.write_table(table)

        file_bytes = bytes_buffer.getvalue()

        # Hash bytes to generate unique file name
        hasher = sha1()
        hasher.update(file_bytes)
        hashstr = hasher.hexdigest()
        fname = f"vegafusion-{hashstr}.arrow"

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


alt.data_transformers.register('vegafusion-arrow', arrow_transformer)
alt.data_transformers.enable('vegafusion-arrow')
