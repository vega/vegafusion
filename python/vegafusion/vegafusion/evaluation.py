from altair import data_transformers
from altair.vegalite.v4.api import Chart
from altair.utils.schemapi import Undefined

MAGIC_MARK_NAME = "_vf_mark"


def eval_transforms(chart: Chart):
    """
    Evaluate the transform associated with a Chart and return the transformed
    data as a DataFrame

    :param chart: altair.vegalite.v4.api.Chart object
    :return: pandas DataFrame of transformed data
    """
    import vl_convert as vlc
    from . import runtime, get_local_tz
    from . import get_inline_datasets_for_spec

    if not isinstance(chart, Chart):
        raise ValueError(
            "eval_transforms accepts an instance of "
            "altair.vegalite.v4.api.Chart\n"
            f"Received value of type: {type(chart)}"
        )

    # Perform shallow copy of chart so that we can change
    # the name
    chart = chart.copy(deep=False)
    chart.name = MAGIC_MARK_NAME

    # Add dummy mark if None specified
    if chart.mark == Undefined:
        chart = chart.mark_point()

    # Drop row and col encoding channels
    chart.encoding.row = Undefined
    chart.encoding.column = Undefined

    with data_transformers.enable("vegafusion-inline"):
        vega_spec = vlc.vegalite_to_vega(chart.to_json(validate=False), vl_version="4.17")
        inline_datasets = get_inline_datasets_for_spec(vega_spec)

    dataset = get_dataset_for_magic_mark(vega_spec)

    (data,), warnings = runtime.pre_transform_datasets(
        vega_spec,
        [dataset],
        get_local_tz(),
        inline_datasets=inline_datasets
    )

    return data


def get_dataset_for_magic_mark(vega_spec):
    for mark in vega_spec.get("marks", []):
        if mark.get("name", "").startswith(MAGIC_MARK_NAME):
            return mark["from"]["data"]

    raise ValueError("Magic mark not found")
