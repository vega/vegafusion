from .runtime import runtime
from .transformer import to_feather, get_inline_datasets_for_spec
from .local_tz import set_local_tz, get_local_tz


from ._version import __version__
from .utils import RendererTransformerEnabler

try:
    import vegafusion.embed
except ImportError:
    pass
