from uuid import uuid4
import altair as alt
from altair.utils.html import spec_to_html


class RowLimitError(ValueError):
    def __init__(self, row_limit):
        self.row_limit = row_limit

    def __str__(self):
        return (
            f"Limit of {self.row_limit} rows was exceeded.\n"
            "Either introduce an aggregation to reduce the number of rows sent to the client or\n"
            "increase the row_limit argument to the vegafusion.enable() function"
        )


def vegafusion_mime_renderer(spec, mimetype="html", row_limit=None, embed_options=None):
    return spec_to_mime_bundle(
        spec,
        mimetype=mimetype,
        row_limit=row_limit,
        embed_options=embed_options
    )


def spec_to_mime_bundle(
        spec,
        mimetype="html",
        row_limit=None,
        embed_options=None,
        html_template="universal",
        full_html=False,
        scale=1,
):
    from . import transformer, runtime, local_tz, vegalite_compilers, altair_vl_version
    vega_spec = vegalite_compilers.get()(spec)

    inline_datasets = transformer.get_inline_datasets_for_spec(vega_spec)
    tx_vega_spec, warnings = runtime.pre_transform_spec(
        vega_spec,
        local_tz.get_local_tz(),
        row_limit=row_limit,
        inline_datasets=inline_datasets
    )

    for warning in warnings:
        if warning.get("type", "") == "RowLimitExceeded":
            raise RowLimitError(row_limit)

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
        output_div = f"altair-viz-{uuid4().hex}"
        html = spec_to_html(
            tx_vega_spec,
            mode="vega",
            vega_version="5",
            vegalite_version=altair_vl_version(),
            vegaembed_version="6",
            fullhtml=full_html,
            output_div=output_div,
            template=html_template,
            embed_options=embed_options
        )
        return {"text/html": html}
    elif mimetype == "svg":
        import vl_convert as vlc
        svg = vlc.vega_to_svg(tx_vega_spec)
        return {"image/svg+xml": svg}
    elif mimetype == "png":
        import vl_convert as vlc
        png = vlc.vega_to_png(tx_vega_spec, scale=scale)
        return {"image/png": png}
    else:
        raise ValueError(f"Unsupported mimetype: {mimetype}")


alt.renderers.register('vegafusion-mime', vegafusion_mime_renderer)
