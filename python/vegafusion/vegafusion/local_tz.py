__tz_config = dict(local_tz=None)


def get_local_tz():
    """
    Get the named local timezone that the VegaFusion mimetype renderer
    will use for calculations.

    Defaults to the kernel's local timezone as determined by vl-convert

    Has no effect on VegaFusionWidget, which always uses the
    browser's local timezone

    :return: named timezone string
    """
    if __tz_config["local_tz"] is None:
        # Fall back to getting local_tz from vl-convert if not set
        import vl_convert as vlc
        __tz_config["local_tz"] = vlc.get_local_tz()

    return __tz_config["local_tz"]


def set_local_tz(local_tz):
    """
    Set the named local timezone that the VegaFusion mimetype renderer
    will use for calculations.

    Has no effect on VegaFusionWidget, which always uses the
    browser's local timezone

    :param local_tz: Named local timezone (e.g. "America/New_York")
    """
    __tz_config["local_tz"] = local_tz
