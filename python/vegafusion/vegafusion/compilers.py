from altair.utils.plugin_registry import PluginRegistry
from typing import Callable


VegaLiteCompilerType = Callable[..., dict]


class VegaLiteCompilerRegistry(PluginRegistry[VegaLiteCompilerType]):
    pass


vegalite_compilers = VegaLiteCompilerRegistry()


def vl_convert_compiler(vegalite_spec) -> dict:
    try:
        import vl_convert as vlc
    except ImportError:
        raise ImportError(
            "The vl-convert Vega-Lite compiler requires the vl-convert-python package"
        )

    from . import altair_vl_version
    vega_spec = vlc.vegalite_to_vega(vegalite_spec, vl_version=altair_vl_version(vl_convert=True))
    return vega_spec


vegalite_compilers.register("vl-convert", vl_convert_compiler)
vegalite_compilers.enable("vl-convert")
