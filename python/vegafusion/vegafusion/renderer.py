import altair as alt
from . import transformer, runtime


def vegafusion_mime_renderer(spec):
    import vl_convert as vlc
    vega_spec = vlc.vegalite_to_vega(spec)
    inline_datasets = transformer.get_inline_datasets_for_spec(vega_spec)
    tx_vega_spec, warnings = runtime.pre_transform_spec(
        vega_spec, "UTC", inline_datasets=inline_datasets
    )
    return {"application/vnd.vega.v5+json": tx_vega_spec}


alt.renderers.register('vegafusion-mime', vegafusion_mime_renderer)
