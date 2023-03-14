from ipywidgets import DOMWidget
from traitlets import Unicode, Bool, Float, CBytes, observe
import time

import logging
logger = logging.getLogger("vegafusion")

from ._frontend import module_name, module_version
import altair as alt
import json


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

    # Message transport properties
    _request_msg = CBytes(allow_none=True, read_only=True).tag(sync=True)
    _response_msg = CBytes(allow_none=True).tag(sync=True)

    def __init__(self, *args, **kwargs):
        # Make sure transformer and renderer and registered
        import vegafusion as vf
        import vegafusion.transformer
        import vegafusion_jupyter.renderer

        # Support altair object or spec as the single positional argument
        if len(args) == 1:
            # Use single positional argument as spec
            kwargs.setdefault("spec", args[0])

        # Handle spec as Altair chart
        if isinstance(kwargs.get("spec", None), alt.TopLevelMixin):
            spec = kwargs["spec"]

            # If vegafusion-feather renderer is already enabled, use the same options
            if alt.data_transformers.active == "vegafusion-feather":
                data_transformer_opts = alt.data_transformers.options
            else:
                data_transformer_opts = dict()

            with vf.enable_widget(**data_transformer_opts):
                # Temporarily enable the vegafusion renderer and transformer so
                # that we use them even if they are not enabled globally
                spec = spec.to_dict()

            # Set spec as a dict, which will be converted to string below
            kwargs["spec"] = spec

        # Handle spec as dict
        if isinstance(kwargs.get("spec", None), dict):
            kwargs["spec"] = json.dumps(kwargs["spec"], indent=2)

        # If vegafusion renderer is already enabled, use the configured debounce options as the default
        if alt.renderers.active == "vegafusion-widget":
            # Use configured debounce options, if any
            renderer_opts = alt.renderers.options
            for opt in ["debounce_wait", "debounce_max_wait"]:
                if opt in renderer_opts:
                    kwargs.setdefault(opt, renderer_opts[opt])

        super().__init__(**kwargs)

        # Wire up widget message callback
        self.on_msg(self._handle_message)

    def _log(self, msg):
        if self.verbose:
            # Use print to show up in JupyterLab Log pane
            print(f"VegaFusionWidget(py): {msg}")

    @observe("_request_msg")
    def _handle_message(self, change):
        from vegafusion.runtime import runtime
        change_new = change["new"]
        if change_new is not None:
            msg_bytes = change["new"]
            start = time.time()
            self._log("Received request")

            # Build response
            response_bytes = runtime.process_request_bytes(
                msg_bytes
            )

            message_len = len(response_bytes)
            self._response_msg = response_bytes

            duration = (time.time() - start) * 1000
            self._log(f"Sent response in {duration:.1f}ms ({message_len} bytes)")

            # Clean up temporary tables
            if runtime._connection is not None:
                runtime._connection.unregister_temporary_tables()
