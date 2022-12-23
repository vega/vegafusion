# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.
from .runtime import runtime
from .transformer import to_feather, get_inline_datasets_for_spec
from .local_tz import set_local_tz, get_local_tz
from ._version import __version__
from . import renderer
import altair as alt

# Import optional subpackages
try:
    import vegafusion.jupyter
except ImportError:
    pass

try:
    import vegafusion.embed
except ImportError:
    pass


def enable_mime():
    """
    Enable the VegaFusion data transformer and renderer so that all Charts
    are displayed using VegaFusion.

    This isn't necessary in order to use the VegaFusionWidget directly
    """
    # Import vegafusion.transformer so that vegafusion-inline transform
    # will be registered
    alt.renderers.enable('vegafusion-mime',)
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
