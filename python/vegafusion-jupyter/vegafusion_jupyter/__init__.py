# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import altair as alt
from ._version import __version__
from .widget import VegaFusionWidget
from .transformer import to_feather
from .renderer import vegafusion_renderer
from .runtime import runtime


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
    alt.renderers.enable(
        'vegafusion',
        debounce_wait=debounce_wait,
        debounce_max_wait=debounce_max_wait,
        download_source_link=download_source_link,
    )
    alt.data_transformers.enable(
        'vegafusion-feather', data_dir=data_dir
    )


def disable():
    """
    Disable the VegaFusion data transformer and renderer so that Charts
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
