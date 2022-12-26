import altair as alt
from altair.utils.html import spec_to_html
from . import transformer, runtime, local_tz


def vegafusion_mime_renderer(spec, mimetype="html", embed_options=None):
    import vl_convert as vlc
    from . import altair_vl_version
    vega_spec = vlc.vegalite_to_vega(spec, vl_version=altair_vl_version())

    # Remove background if non-default theme is active
    # Not sure why this is needed, but otherwise dark theme will end up with a
    # white background
    if alt.themes.active != "default":
        vega_spec.pop("background", None)

    inline_datasets = transformer.get_inline_datasets_for_spec(vega_spec)
    tx_vega_spec, warnings = runtime.pre_transform_spec(
        vega_spec,
        local_tz.get_local_tz(),
        inline_datasets=inline_datasets
    )

    # Handle default embed options
    embed_options = embed_options or {}
    embed_options = dict({"mode": "vega"}, **embed_options)

    if mimetype == "vega":
        vega_mimetype = "application/vnd.vega.v5+json"
        return (
            {vega_mimetype: tx_vega_spec},
            {vega_mimetype: {"embed_options": embed_options}}
        )
    elif mimetype == "html":
        html = spec_to_html(
            tx_vega_spec,
            mode="vega",
            vega_version="5",
            vegalite_version="4.17.0",
            vegaembed_version="6",
            fullhtml=False,
            output_div="altair-viz-{}",
            template="universal",
            embed_options=embed_options
        )
        return {"text/html": html}
    elif mimetype == "svg":
        svg = vlc.vega_to_svg(tx_vega_spec)
        return {"image/svg+xml": svg}
    elif mimetype == "png":
        png = vlc.vega_to_png(tx_vega_spec)
        return {"image/png": png}
    else:
        raise ValueError(f"Unsupported mimetype: {mimetype}")


alt.renderers.register('vegafusion-mime', vegafusion_mime_renderer)
