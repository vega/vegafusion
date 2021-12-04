from altair import Chart, Selection
from altair.utils.schemapi import SchemaValidationError
from ipywidgets import DOMWidget
from traitlets import Unicode, MetaHasTraits
from ._frontend import module_name, module_version
from vegafusion import PyTaskGraphRuntime

class MetaAltairWrapper(MetaHasTraits):
    def __new__(cls, clsname, bases, dct):
        newclass = super(MetaAltairWrapper, cls).__new__(cls, clsname, bases, dct)

        wrap_cls = getattr(newclass, "_altair_wrap_class", type(None))
        for name in getattr(newclass, "_altair_wrap_methods", ()):
            setattr(newclass, name, MetaAltairWrapper.get_wrapped_method(name, wrap_cls))
        return newclass

    @staticmethod
    def get_wrapped_method(name, wrap_cls):
        def wrapper_fn(self, *args, **kwargs):
            return self._from_altair(
                getattr(self.alt_obj, name)(*args, **kwargs)
            )

        chart_fn = getattr(wrap_cls, name)
        wrapper_fn.__doc__ = chart_fn.__doc__
        wrapper_fn.__wrapped__ = chart_fn
        return wrapper_fn


class MyChart(DOMWidget, metaclass=MetaAltairWrapper):
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

    # Type of Altair object being wrapped
    _altair_wrap_class = Chart

    # Methods that should be wrapped
    _altair_wrap_methods = (
        # Encoding
        "encode",
        "interactive",

        # Marks
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

        # Transforms
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

    def __init__(self, *args, **kwargs):

        # Handle special case of the layout argument to DOMWidget constructor
        widget_kwargs = dict()
        if "layout" in kwargs:
            widget_kwargs["layout"] = kwargs.pop("layout")

        super(MyChart, self).__init__(**widget_kwargs)

        # Create inner altair Chart
        self._alt_obj = None

        self.alt_obj = Chart(*args, **kwargs)
        self.on_msg(self._handle_message)

        # Create vega fusion runtime (this should be shared across charts)
        self.runtime = PyTaskGraphRuntime(16, None)

        self._msgs = []

    def _handle_message(self, widget, msg, buffers):
        print("py: handle request")
        self._msgs.append(msg)
        print(msg)
        if msg['type'] == "request":
            print("py: handle request")
            # Build response
            self._msgs.append(buffers[0])
            response_bytes = self.runtime.process_request_bytes(
                buffers[0]
            )
            self._msgs.append(type(response_bytes))
            print("py: send response")
            self.send(dict(type="response"), [response_bytes])

    @classmethod
    def _from_altair(cls, alt_obj):
        new_chart = MyChart()
        new_chart.alt_obj = alt_obj
        return new_chart

    def add_selection(self, *selections):
        # Support selection widgets here
        inner = self.alt_obj.add_selection
        return self._from_altair(
            inner(*selections)
        )

    @property
    def alt_obj(self):
        return self._alt_obj

    @alt_obj.setter
    def alt_obj(self, value):
        self._alt_obj = value
        try:
            self.vegalite_spec = self._alt_obj.to_json()
        except SchemaValidationError:
            pass

    add_selection.__doc__ = Chart.add_selection.__doc__
    add_selection.__wrapped__ = Chart.add_selection

    def __repr__(self):
        return "altairfusion.{}(...)".format(self.__class__.__name__)
