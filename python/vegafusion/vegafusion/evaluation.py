from altair import data_transformers
from altair.vegalite.v4.api import Chart
from altair.utils.schemapi import Undefined

MAGIC_MARK_NAME = "_vf_mark"


def eval_transforms(chart: Chart, row_limit=None):
    """
    Evaluate the transform associated with a Chart and return the transformed
    data as a DataFrame

    :param chart: altair.vegalite.v4.api.Chart object
    :param row_limit: Maximum number of rows to return. None (default) for unlimited
    :return: pandas DataFrame of transformed data
    """
    import vl_convert as vlc
    from . import runtime, get_local_tz, get_inline_datasets_for_spec, altair_vl_version

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

    with data_transformers.enable("vegafusion-inline"):
        vega_spec = vlc.vegalite_to_vega(chart.to_json(validate=False), vl_version=altair_vl_version())
        inline_datasets = get_inline_datasets_for_spec(vega_spec)

    dataset = get_dataset_for_magic_mark(vega_spec)
    if dataset is None:
        raise ValueError("Failed to identify mark for Altair chart")

    (data,), warnings = runtime.pre_transform_datasets(
        vega_spec,
        [dataset],
        get_local_tz(),
        row_limit=row_limit,
        inline_datasets=inline_datasets
    )

    return data


def transformed_dtypes(chart: Chart):
    """
    Get the dtypes of the Chart's transformed data

    :param chart: altair.vegalite.v4.api.Chart object
    :return: pandas Series of the dtypes of the transformed data
    """
    df = eval_transforms(chart, row_limit=1)
    return df.dtypes


def get_dataset_for_magic_mark(vega_spec):
    for mark in vega_spec.get("marks", []):
        if mark.get("name", "") == f"{MAGIC_MARK_NAME}_marks":
            return mark.get("from", {}).get("data", None)

        elif mark.get("name", "") == f"{MAGIC_MARK_NAME}_cell":
            return mark.get("from", {}).get("facet", None).get("data", None)

        elif mark.get("type", "") == "group":
            dataset = get_dataset_for_magic_mark(mark)
            if dataset is not None:
                return dataset

    return None
