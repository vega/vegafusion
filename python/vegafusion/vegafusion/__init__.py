# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.
from .runtime import runtime
from .transformer import to_feather
from ._version import __version__

# Import subpackages
try:
    import vegafusion.jupyter
except ImportError:
    pass

try:
    import vegafusion.embed
except ImportError:
    pass
