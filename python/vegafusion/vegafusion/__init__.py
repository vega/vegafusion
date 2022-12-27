# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.
from .runtime import runtime
from .transformer import to_feather, get_inline_datasets_for_spec
from .local_tz import set_local_tz, get_local_tz
from . import renderer
from .compilers import vegalite_compilers
import altair as alt

from ._version import __version__
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
    from altair.vegalite.v4 import SCHEMA_VERSION
    if vl_convert:
        # Compute VlConvert's vl_version string (of the form 'v5_2')
        # from SCHEMA_VERSION (of the form 'v5.2.0')
        return "_".join(SCHEMA_VERSION.split(".")[:2])
    else:
        # Return full version without leading v
        return SCHEMA_VERSION.rstrip("v")


def enable_mime(mimetype="html", embed_options=None):
    """
    Enable the VegaFusion data transformer and renderer so that all Charts
    are displayed using VegaFusion.

    This isn't necessary in order to use the VegaFusionWidget directly

    :param mimetype: Mime type. One of:
        - "html" (default)
        - "vega"
        - "svg"
        - "png": Note: the PNG renderer can be quite slow for charts with lots of marks
    :param embed_options: dict (optional)
        Dictionary of options to pass to the vega-embed. Default
        entry is {'mode': 'vega'}.
    """
    # Import vegafusion.transformer so that vegafusion-inline transform
    # will be registered
    alt.renderers.enable('vegafusion-mime', mimetype=mimetype, embed_options=embed_options)
    alt.data_transformers.enable('vegafusion-inline')


def enable_widget(
    download_source_link=None,
    debounce_wait=30,
    debounce_max_wait=60,
    data_dir="_vegafusion_data"
):
    from vegafusion.jupyter import enable
    enable(
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
    alt.renderers.enable('default')
    alt.data_transformers.enable('default')
