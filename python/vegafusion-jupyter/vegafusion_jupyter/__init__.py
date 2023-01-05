import altair as alt

from vegafusion import RendererTransformerEnabler
from . import renderer
from . import widget
from .widget import VegaFusionWidget
from ._version import __version__


def enable(
        download_source_link=None,
        debounce_wait=30,
        debounce_max_wait=60,
        data_dir="_vegafusion_data"
):
    """
    Enable the VegaFusion data transformer and renderer so that all Charts
    are displayed using VegaFusion.

    This isn't necessary in order to use the VegaFusionWidget directly
    """
    # Import vegafusion.transformer so that vegafusion-feather transform
    # will be registered
    import vegafusion.transformer
    return RendererTransformerEnabler(
        renderer_ctx=alt.renderers.enable(
            'vegafusion-widget',
            debounce_wait=debounce_wait,
            debounce_max_wait=debounce_max_wait,
            download_source_link=download_source_link,
        ),
        data_transformer_ctx=alt.data_transformers.enable(
            'vegafusion-feather', data_dir=data_dir
        ),
        repr_str=f"vegafusion.enable_widget()"
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
    from vegafusion import disable
    disable()


def _jupyter_labextension_paths():
    """Called by Jupyter Lab Server to detect if it is a valid labextension and
    to install the widget
    Returns
    =======
    src: Source directory name to copy files from. Webpack outputs generated files
        into this directory and Jupyter Lab copies from this directory during
        widget installation
    dest: Destination directory name to install widget files to. Jupyter Lab copies
        from `src` directory into <jupyter path>/labextensions/<dest> directory
        during widget installation
    """
    return [{
        'src': 'labextension',
        'dest': 'vegafusion-jupyter',
    }]


def _jupyter_nbextension_paths():
    """Called by Jupyter Notebook Server to detect if it is a valid nbextension and
    to install the widget
    Returns
    =======
    section: The section of the Jupyter Notebook Server to change.
        Must be 'notebook' for widget extensions
    src: Source directory name to copy files from. Webpack outputs generated files
        into this directory and Jupyter Notebook copies from this directory during
        widget installation
    dest: Destination directory name to install widget files to. Jupyter Notebook copies
        from `src` directory into <jupyter path>/nbextensions/<dest> directory
        during widget installation
    require: Path to importable AMD Javascript module inside the
        <jupyter path>/nbextensions/<dest> directory
    """
    return [{
        'section': 'notebook',
        'src': 'nbextension',
        'dest': 'vegafusion_jupyter',
        'require': 'vegafusion_jupyter/extension'
    }]
