#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Jon Mease.
# Distributed under the terms of the Modified BSD License.

import altair as alt
from ._version import __version__, version_info
from .widget import VegaFusionWidget
from .transformer import to_feather
import vegafusion_jupyter.renderer


def enable():
    """
    Enable the VegaFusion data transformer and renderer so that all Charts
    are displayed using VegaFusion.

    Equivalent to

    ```python
    import altair as alt
    alt.renderers.enable('vegafusion')
    alt.data_transformers.enable('vegafusion-feather')
    ```

    This isn't necessary in order to use the VegaFusionWidget directly
    """
    alt.renderers.enable('vegafusion')
    alt.data_transformers.enable('vegafusion-feather')


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
