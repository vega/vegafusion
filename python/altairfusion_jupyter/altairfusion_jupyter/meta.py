from traitlets import MetaHasTraits
import altair as alt
import wrapt


class MetaAltairWrapper(MetaHasTraits):
    def __new__(cls, clsname, bases, dct):
        newclass = super(MetaAltairWrapper, cls).__new__(cls, clsname, bases, dct)

        wrap_cls = getattr(newclass, "_altair_wrap_class", type(None))
        for name in getattr(newclass, "_altair_wrap_methods", ()):
            if hasattr(wrap_cls, name):
                setattr(newclass, name, MetaAltairWrapper.get_wrapped_method(name, wrap_cls))
        return newclass

    @staticmethod
    def get_wrapped_method(name, wrap_cls):
        def wrapper_fn(self, *args, **kwargs):
            return from_altair(
                getattr(self.alt_obj, name)(
                    *unwrap_args(args),
                    **unwrap_kwargs(kwargs)
                )
            )

        chart_fn = getattr(wrap_cls, name)
        wrapper_fn.__doc__ = chart_fn.__doc__
        wrapper_fn.__wrapped__ = chart_fn
        return wrapper_fn


def from_altair(value):
    from altairfusion_jupyter import Chart, RepeatChart, LayerChart, ConcatChart, VConcatChart, HConcatChart

    if isinstance(value, alt.Chart):
        value = Chart(value)
    elif isinstance(value, alt.LayerChart):
        value = LayerChart(value)
    elif isinstance(value, alt.RepeatChart):
        value = RepeatChart(value)
    elif isinstance(value, alt.ConcatChart):
        value = ConcatChart(value)
    elif isinstance(value, alt.VConcatChart):
        value = VConcatChart(value)
    elif isinstance(value, alt.HConcatChart):
        value = HConcatChart(value)
    # else, return as-is

    return value


@wrapt.decorator
def altair_builder_fn(wrapped, instance, args, kwargs):
    alt_obj = wrapped(
        *unwrap_args(args),
        **unwrap_kwargs(kwargs),
    )
    return from_altair(alt_obj)


def unwrap_args(args):
    from altairfusion_jupyter.base_chart import BaseChart
    return [
        arg.alt_obj if isinstance(arg, BaseChart) else arg
        for arg in args
    ]


def unwrap_kwargs(kwargs):
    from altairfusion_jupyter.base_chart import BaseChart
    return {
        k: arg.alt_obj if isinstance(arg, BaseChart) else arg
        for k, arg in kwargs.items()
    }
