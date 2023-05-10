import json

import altair as alt
from altair import (
    data_transformers, Chart, FacetChart, LayerChart, HConcatChart, VConcatChart
)
from altair.utils.schemapi import Undefined

MAGIC_MARK_NAME = "_vf_mark{}"


def make_magic_mark_name(i):
    return MAGIC_MARK_NAME.format(i)


def transformed_data(chart, row_limit=None, exclude=None):
    """
    Evaluate the transform associated with a Chart and return the transformed
    data as a DataFrame

    :param chart: altair Chart, LayerChart, HConcatChart or VConcatChart
    :param row_limit: Maximum number of rows to return. None (default) for unlimited
    :param exclude: list of mark names to exclude.
        Not supported for simple (non-compound) Charts
    :return:
        - DataFrame of transformed data for Chart input
        - list of DataFrames for LayerChart, HConcatChart, VConcatChart
    """

    from . import runtime, get_local_tz, get_inline_datasets_for_spec, vegalite_compilers

    if isinstance(chart, Chart):
        # Add dummy mark if None specified
        if chart.mark == Undefined:
            chart = chart.mark_point()

    # Deep copy chart so that we can rename marks without affecting caller
    chart = chart.copy(deep=True)

    # Rename leaf chart with magic names
    magic_names = magic_name_chart(chart, 0, exclude=exclude)

    # Compile to vega and retrieve inline datasets
    with data_transformers.enable("vegafusion-inline"):
        vega_spec = vegalite_compilers.get()(chart.to_json(validate=False))
        inline_datasets = get_inline_datasets_for_spec(vega_spec)

    # Build mapping from magic mark names to vega datasets
    facet_mapping = get_facet_mapping(vega_spec)
    dataset_mapping = get_datasets_for_magic_marks(vega_spec, magic_names, facet_mapping)

    # Build a list of vega dataset names that corresponds to the order
    # of the chart components
    dataset_names = []
    for magic_name in magic_names:
        if magic_name in dataset_mapping:
            dataset_names.append(dataset_mapping[magic_name])
        else:
            raise ValueError("Failed to locate all datasets")

    # Extract transformed datasets
    datasets, warnings = runtime.pre_transform_datasets(
        vega_spec,
        dataset_names,
        get_local_tz(),
        row_limit=row_limit,
        inline_datasets=inline_datasets
    )

    if isinstance(chart, Chart):
        # Return DataFrame if input was a simple Chart
        return datasets[0]
    else:
        # Otherwise return the list of DataFrames
        return datasets


def magic_name_chart(chart, i=0, exclude=None):
    """
    Rename charts (and subcharts) with unique names that we can look up
    later in the compiled Vega spec

    :param chart: Chart instance
    :param i: starting name index
    :return:
    """
    exclude = exclude or []
    if isinstance(chart, (Chart, FacetChart)):
        # Perform shallow copy of chart so that we can change
        # the name
        if chart.name not in exclude:
            name = make_magic_mark_name(i)
            chart.name = name
            return [name]
        else:
            return []
    else:
        if isinstance(chart, LayerChart):
            subcharts = chart.layer
        elif isinstance(chart, HConcatChart):
            subcharts = chart.hconcat
        elif isinstance(chart, VConcatChart):
            subcharts = chart.vconcat
        else:
            raise ValueError(
                "transformed_data accepts an instance of "
                "Chart, FacetChart, LayerChart, HConcatChart, or VConcatChart\n"
                f"Received value of type: {type(chart)}"
            )

        magic_names = []
        for subchart in subcharts:
            for name in magic_name_chart(subchart, i=i + len(magic_names), exclude=exclude):
                magic_names.append(name)
        return magic_names


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


def get_datasets_for_magic_marks(vega_spec, magic_names, facet_mapping):
    data_names = {}
    group_index = 0
    for mark in vega_spec.get("marks", []):
        name = mark.get("name", "")
        if mark.get("type", "") == "group":
            group_data_names = get_datasets_for_magic_marks(mark, magic_names, facet_mapping)
            data_names.update(group_data_names)
            group_index += 1
        else:
            for magic_name in magic_names:
                if name.startswith(magic_name) and name.endswith("_marks"):
                    data_name = mark.get("from", {}).get("data", None)
                    data_names[magic_name] = facet_mapping.get(data_name, data_name)
                    break

                elif mark.get("name", "") == f"{magic_name}_cell":
                    data_name = mark.get("from", {}).get("facet", None).get("data", None)
                    data_names[magic_name] = facet_mapping.get(data_name, data_name)
                    break

    return data_names
