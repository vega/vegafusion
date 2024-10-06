from .runtime import runtime
from .transformer import to_feather, get_inline_datasets_for_spec
from .local_tz import set_local_tz, get_local_tz
from importlib.abc import MetaPathFinder, Loader
from importlib.metadata import version as _original_version
import importlib.metadata

from ._vegafusion import __version__

def patched_version(distribution_name):
    """
    Fake the version of the vegafusion-python-embed package to match the version of the
    vegafusion package. This is just to satisfy Altair's version check.
    """
    if distribution_name == "vegafusion-python-embed":
        return __version__
    return _original_version(distribution_name)

# Patch importlib.metadata.version to handle our dummy package
importlib.metadata.version = patched_version