import importlib.metadata
from importlib.metadata import version as _original_version
from typing import cast

from ._vegafusion import __version__
from .local_tz import get_local_tz, set_local_tz
from .runtime import runtime
from .utils import get_column_usage


def patched_version(distribution_name: str) -> str:
    """
    Fake the version of the vegafusion-python-embed package to match the version of the
    vegafusion package. This is just to satisfy Altair's version check.
    """
    if distribution_name == "vegafusion-python-embed":
        return cast(str, __version__).replace("-", "")
    return _original_version(distribution_name)


# Patch importlib.metadata.version to handle our dummy package
importlib.metadata.version = patched_version


__all__ = [
    "runtime",
    "set_local_tz",
    "get_local_tz",
    "get_column_usage",
]
