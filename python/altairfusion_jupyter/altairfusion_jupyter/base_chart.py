from altair.utils.schemapi import SchemaValidationError
from ipywidgets import DOMWidget
from traitlets import Unicode
from ._frontend import module_name, module_version
from vegafusion import PyTaskGraphRuntime
from .meta import MetaAltairWrapper


transform_methods = (
    "transform_aggregate",
    "transform_bin",
    "transform_calculate",
    "transform_density",
    "transform_filter",
    "transform_flatten",
    "transform_fold",
    "transform_impute",
    "transform_joinaggregate",
    "transform_loess",
    "transform_lookup",
    "transform_pivot",
    "transform_quantile",
    "transform_regression",
    "transform_sample",
    "transform_stack",
    "transform_timeunit",
    "transform_window",
)

mark_methods = (
    "mark_area",
    "mark_bar",
    "mark_line",
    "mark_image",
    "mark_trail",
    "mark_point",
    "mark_text",
    "mark_tick",
    "mark_rect",
    "mark_rule",
    "mark_circle",
    "mark_square",
    "mark_geoshape",
    "mark_boxplot",
    "mark_errorbar",
    "mark_errorband",
)

all_chart_methods = transform_methods + mark_methods + (
    "encode",
    "interactive",
    "properties",
    "add_selection",
    "repeat",
)

class BaseChart(DOMWidget, metaclass=MetaAltairWrapper):
    _model_name = Unicode('AltairFusionModel').tag(sync=True)
    _model_module = Unicode(module_name).tag(sync=True)
    _model_module_version = Unicode(module_version).tag(sync=True)
    _view_name = Unicode('AltairFusionView').tag(sync=True)
    _view_module = Unicode(module_name).tag(sync=True)
    _view_module_version = Unicode(module_version).tag(sync=True)
    vegalite_spec = Unicode(None, allow_none=True).tag(sync=True)
    vega_spec_full = Unicode(None, allow_none=True).tag(sync=True)

    # vegalite_spec
    # vegaspec_full
    # vegaspec_client
    # vegaspec_server
    #
    # server_to

    _altair_wrap_class = type(None)
    _altair_wrap_methods = all_chart_methods

    def __init__(self, *args, **kwargs):

        if len(args) == 1 and not kwargs and isinstance(args[0], self._altair_wrap_class):
            alt_obj = args[0]
        else:
            alt_obj = self._altair_wrap_class(*args, **kwargs)

        # Handle special case of the layout argument to DOMWidget constructor
        widget_kwargs = dict()
        if "layout" in kwargs:
            widget_kwargs["layout"] = kwargs.pop("layout")

        super().__init__(**widget_kwargs)

        # Create inner altair Chart
        self._alt_obj = alt_obj
        try:
            self.vegalite_spec = self._alt_obj.to_json()
        except SchemaValidationError:
            pass

        # Wire up widget message callback
        self.on_msg(self._handle_message)

        # Create vega fusion runtime (this should be shared across charts)
        self.runtime = PyTaskGraphRuntime(16, None)
        self._msgs = []

    def _handle_message(self, widget, msg, buffers):
        self._msgs.append(msg)
        # print(msg)
        if msg['type'] == "request":
            # print("py: handle request")
            # Build response
            self._msgs.append(buffers[0])
            response_bytes = self.runtime.process_request_bytes(
                buffers[0]
            )
            self._msgs.append(type(response_bytes))
            # print("py: send response")
            self.send(dict(type="response"), [response_bytes])

    @property
    def alt_obj(self):
        return self._alt_obj

    def __repr__(self):
        return "altairfusion.{}(...)".format(self.__class__.__name__)
