import json

from altair import data_transformers, Chart, FacetChart
from altair.utils.schemapi import Undefined

MAGIC_MARK_NAME = "_vf_mark"


def transformed_data(chart: Chart, row_limit=None):
    """
    Evaluate the transform associated with a Chart and return the transformed
    data as a DataFrame

    :param chart: altair Chart object
    :param row_limit: Maximum number of rows to return. None (default) for unlimited
    :return: pandas DataFrame of transformed data
    """

    from . import runtime, get_local_tz, get_inline_datasets_for_spec, vegalite_compilers

    if not isinstance(chart, (Chart, FacetChart)):
        raise ValueError(
            "transformed_data accepts an instance of "
            "Chart or FacetChart\n"
            f"Received value of type: {type(chart)}"
        )

    # Perform shallow copy of chart so that we can change
    # the name
    chart = chart.copy(deep=False)
    chart.name = MAGIC_MARK_NAME

    if isinstance(chart, Chart):
        # Add dummy mark if None specified
        if chart.mark == Undefined:
            chart = chart.mark_point()

    with data_transformers.enable("vegafusion-inline"):
        vega_spec = vegalite_compilers.get()(chart.to_json(validate=False))
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


def get_facet_mapping(vega_spec):
    facet_mapping = {}
    for mark in vega_spec.get("marks", []):
        if mark.get("type", None) == "group":
            facet = mark.get("from", {}).get("facet", None)
            if facet is not None:
                facet_name = facet.get("name", None)
                facet_data = facet.get("data", None)
                if facet_name is not None and facet_data is not None:
                    facet_mapping[facet_name] = facet_data

    return facet_mapping


def get_dataset_for_magic_mark(vega_spec):
    facet_mapping = get_facet_mapping(vega_spec)
    data_name = None
    for mark in vega_spec.get("marks", []):
        name = mark.get("name", "")
        if name.startswith(MAGIC_MARK_NAME) and name.endswith("_marks"):
            data_name = mark.get("from", {}).get("data", None)

        elif mark.get("name", "") == f"{MAGIC_MARK_NAME}_cell":
            data_name = mark.get("from", {}).get("facet", None).get("data", None)

        elif mark.get("type", "") == "group":
            dataset = get_dataset_for_magic_mark(mark)
            if dataset is not None:
                data_name = dataset

    if data_name is not None:
        return facet_mapping.get(data_name, data_name)
    else:
        return None

