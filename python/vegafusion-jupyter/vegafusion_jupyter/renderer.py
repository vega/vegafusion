# VegaFusion
# Copyright (C) 2022, VegaFusion Technologies LLC
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.

import altair as alt
import vegafusion as vf

def vegafusion_renderer(spec, **widget_options):
    """
    Altair renderer that displays charts using a VegaFusionWidget
    """
    from IPython.display import display
    from vegafusion_jupyter import VegaFusionWidget

    # Display widget as a side effect, then return empty string text representation
    # so that Altair doesn't also display a string representation
    widget = VegaFusionWidget(spec, **widget_options)
    display(widget)
    return {'text/plain': ""}

def vegafusion_mime_renderer(spec):
    import vl_convert as vlc
    vega_spec = vlc.vegalite_to_vega(spec)
    inline_datasets = vf.transformer.get_inline_datasets_for_spec(vega_spec)
    tx_vega_spec, warnings = vf.runtime.pre_transform_spec(
        vega_spec, "UTC", inline_datasets=inline_datasets
    )
    return {"application/vnd.vega.v5+json": tx_vega_spec}

alt.renderers.register('vegafusion-widget', vegafusion_renderer)
alt.renderers.register('vegafusion-mime', vegafusion_mime_renderer)
