import importlib.metadata
from importlib.abc import Loader, MetaPathFinder
from importlib.metadata import version as _original_version

from ._vegafusion import __version__
from .local_tz import get_local_tz, set_local_tz
from .runtime import runtime
from .transformer import get_inline_datasets_for_spec, to_feather


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
