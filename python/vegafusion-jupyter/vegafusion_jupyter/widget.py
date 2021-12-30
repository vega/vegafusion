from ipywidgets import DOMWidget
from traitlets import Unicode, Bool, Float
import time

import logging
logger = logging.getLogger("vegafusion")

from ._frontend import module_name, module_version
import altair as alt
import json

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
    verbose = Bool(False).tag(sync=True)
    debounce_wait = Float(30, allow_none=False).tag(sync=True)
    debounce_max_wait = Float(60, allow_none=True).tag(sync=True)

    def __init__(self, *args, **kwargs):

        # Support altair object as single positional argument
        if len(args) == 1:
            chart = args[0]
            with alt.renderers.enable("vegafusion"):
                with alt.data_transformers.enable("vegafusion-feather"):
                    # Temporarily enable the vegafusion renderer and transformer so
                    # that we use them even if they are not enabled globally
                    spec = json.dumps(chart.to_dict(), indent=2)
            kwargs["spec"] = spec

        super().__init__(**kwargs)

        # Wire up widget message callback
        self.on_msg(self._handle_message)

    def _log(self, msg):
        if self.verbose:
            # Use print to show up in JupyterLab Log pane
            print(f"VegaFusionWidget(py): {msg}")

    def _handle_message(self, widget, msg, buffers):
        if msg['type'] == "request":
            start = time.time()
            self._log("Received request")

            # Build response
            response_bytes = runtime.process_request_bytes(
                buffers[0]
            )

            self.send(dict(type="response"), [response_bytes])

            duration = (time.time() - start) * 1000
            self._log(f"Sent response in {duration:.1f}ms")
