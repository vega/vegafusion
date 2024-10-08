from __future__ import annotations

__tz_config: dict[str, str | None] = {"local_tz": None}


def get_local_tz() -> str:
    """
    Get the named local timezone that the VegaFusion mimetype renderer
    will use for calculations.

    Defaults to the kernel's local timezone as determined by vl-convert.

    Has no effect on VegaFusionWidget, which always uses the
    browser's local timezone

    :return: named timezone string
    """
    local_tz = __tz_config["local_tz"]
    if local_tz is None:
        # Fall back to getting local_tz from vl-convert if not set
        try:
            import vl_convert as vlc

            local_tz = vlc.get_local_tz() or "UTC"
            __tz_config["local_tz"] = local_tz
        except ImportError as e:
            raise ImportError(
                "vl-convert is not installed and so the local system timezone cannot "
                "be determined.\nEither install the vl-convert-python package or set "
                "the local timezone manually using\nthe vegafusion.set_local_tz "
                "function"
            ) from e

    return local_tz


def set_local_tz(local_tz: str) -> None:
    """
    Set the named local timezone that the VegaFusion mimetype renderer
    will use for calculations.

    Has no effect on VegaFusionWidget, which always uses the
    browser's local timezone

    :param local_tz: Named local timezone (e.g. "America/New_York")
    """
    __tz_config["local_tz"] = local_tz
