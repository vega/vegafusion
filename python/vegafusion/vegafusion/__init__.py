# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.
from .runtime import runtime
from .transformer import to_feather, get_inline_datasets_for_spec
from .renderer import RowLimitError
from .local_tz import set_local_tz, get_local_tz
from .evaluation import transformed_data
from .save import save_html, save_vega, save_png, save_svg
from . import renderer
from .compilers import vegalite_compilers
import altair as alt

from ._version import __version__
from .utils import RendererTransformerEnabler

# Import optional subpackages
try:
    import vegafusion.jupyter
except ImportError:
    pass

try:
    import vegafusion.embed
except ImportError:
    pass


def altair_vl_version(vl_convert=False):
    """
    Get Altair's preferred Vega-Lite version

    :param vl_convert: If True, return a version string compatible with vl_convert
        (e.g. v4_17 rather than 4.17.0)
    :return: str with Vega-Lite version
    """
    from altair import SCHEMA_VERSION

    if vl_convert:
        # Compute VlConvert's vl_version string (of the form 'v5_2')
        # from SCHEMA_VERSION (of the form 'v5.2.0')
        return "_".join(SCHEMA_VERSION.split(".")[:2])
    else:
        # Return full version without leading v
        return SCHEMA_VERSION.lstrip("v")


def enable(mimetype="html", row_limit=10000, embed_options=None):
    """
    Enable the VegaFusion mime renderer so that all Altair charts
    are displayed using VegaFusion.

    :param mimetype: Mime type. One of:
        - "html" (default)
        - "vega"
        - "svg"
        - "png": Note, the PNG renderer can be quite slow for charts with lots of marks
    :param row_limit: Maximum number of rows (after transforms are applied) that may be
        included in the Vega specifications that will be displayed. An error will
        be raised if the limit is exceeded. None for unlimited.
    :param embed_options: dict (optional)
        Dictionary of options to pass to the vega-embed. Default
        entry is {'mode': 'vega'}.
    """
    return RendererTransformerEnabler(
        renderer_ctx=alt.renderers.enable(
            'vegafusion-mime', mimetype=mimetype, row_limit=row_limit, embed_options=embed_options
        ),
        data_transformer_ctx=alt.data_transformers.enable('vegafusion-inline'),
        repr_str=(
            "vegafusion.enable("
            f"mimetype={repr(mimetype)}, row_limit={row_limit}, embed_options={repr(embed_options)}"
            ")"
        )
    )


def enable_widget(
    debounce_wait=30,
    debounce_max_wait=60,
    data_dir="_vegafusion_data",
    download_source_link=None,
):
    """
    Enable the VegaFusion widget renderer so that all charts are displayed
    using VegaFusion.

    This isn't necessary in order to use the VegaFusionWidget directly

    :param debounce_wait: Debounce wait period for interactions (milliseconds)
    :param debounce_max_wait: Max debounce wait for interactions (milliseconds)
    :param download_source_link: URL of notebooks source code for inclusion
        in dropdown menu
    :param data_dir: Path that temporary feather files will be written to
    """
    from vegafusion.jupyter import enable
    return enable(
        download_source_link=download_source_link,
        debounce_wait=debounce_wait,
        debounce_max_wait=debounce_max_wait,
        data_dir=data_dir
    )


def disable():
    """
    Disable the VegaFusion data transformers and renderers so that Charts
    are not displayed using VegaFusion

    Equivalent to

    ```python
    import altair as alt
    alt.renderers.enable('default')
    alt.data_transformers.enable('default')
    ```

    This does not affect the behavior of VegaFusionWidget
    """
    return RendererTransformerEnabler(
        renderer_ctx=alt.renderers.enable('default'),
        data_transformer_ctx=alt.data_transformers.enable('default'),
        repr_str="vegafusion.disable()"
    )
