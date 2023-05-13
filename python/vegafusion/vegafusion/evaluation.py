from altair import (
    data_transformers, Chart, FacetChart, LayerChart, HConcatChart, VConcatChart, ConcatChart
)
from altair.utils.schemapi import Undefined

MAGIC_MARK_NAME = "_vf_mark{}"


def make_magic_mark_name(i):
    return MAGIC_MARK_NAME.format(i)


def transformed_data(chart, row_limit=None, exclude=None):
    """
    Evaluate the transform associated with a Chart and return the transformed
    data as one or more DataFrames

    :param chart: altair Chart, LayerChart, HConcatChart, VConcatChart, or ConcatChart
    :param row_limit: Maximum number of rows to return. None (default) for unlimited
    :param exclude: list of mark names to exclude.
    :return:
        - DataFrame of transformed data for Chart input
        - list of DataFrames for LayerChart, HConcatChart, VConcatChart, ConcatChart
    """

    from . import runtime, get_local_tz, get_inline_datasets_for_spec, vegalite_compilers

    if isinstance(chart, Chart):
        # Add dummy mark if None specified
        if chart.mark == Undefined:
            chart = chart.mark_point()

    # Deep copy chart so that we can rename marks without affecting caller
    chart = chart.copy(deep=True)

    # Rename leaf chart with magic names
    chart_names = name_chart(chart, 0, exclude=exclude)

    # Compile to vega and retrieve inline datasets
    with data_transformers.enable("vegafusion-inline"):
        vega_spec = vegalite_compilers.get()(chart.to_json(validate=False))
        inline_datasets = get_inline_datasets_for_spec(vega_spec)

    # Build mapping from mark names to vega datasets
    facet_mapping = get_facet_mapping(vega_spec)
    dataset_mapping = get_datasets_for_named_marks(vega_spec, chart_names, facet_mapping)

    # Build a list of vega dataset names that corresponds to the order
    # of the chart components
    dataset_names = []
    for chart_name in chart_names:
        if chart_name in dataset_mapping:
            dataset_names.append(dataset_mapping[chart_name])
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

    if isinstance(chart, (Chart, FacetChart)):
        # Return DataFrame (or None if it was excluded) if input was a simple Chart
        if not datasets:
            return None
        else:
            return datasets[0]
    else:
        # Otherwise return the list of DataFrames
        return datasets


def name_chart(chart, i=0, exclude=None):
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
            if chart.name in (None, Undefined):
                name = make_magic_mark_name(i)
                chart.name = name
            return [chart.name]
        else:
            return []
    else:
        if isinstance(chart, LayerChart):
            subcharts = chart.layer
        elif isinstance(chart, HConcatChart):
            subcharts = chart.hconcat
        elif isinstance(chart, VConcatChart):
            subcharts = chart.vconcat
        elif isinstance(chart, ConcatChart):
            subcharts = chart.concat
        else:
            raise ValueError(
                "transformed_data accepts an instance of "
                "Chart, FacetChart, LayerChart, HConcatChart, VConcatChart, or ConcatChart\n"
                f"Received value of type: {type(chart)}"
            )

        chart_names = []
        for subchart in subcharts:
            for name in name_chart(subchart, i=i + len(chart_names), exclude=exclude):
                chart_names.append(name)
        return chart_names


def get_facet_mapping(vega_spec, scope=()):
    facet_mapping = {}
    group_index = 0
    mark_group = get_mark_group_for_scope(vega_spec, scope) or {}
    for mark in mark_group.get("marks", []):
        if mark.get("type", None) == "group":
            # Get facet for this group
            group_scope = scope + (group_index,)
            facet = mark.get("from", {}).get("facet", None)
            if facet is not None:
                facet_name = facet.get("name", None)
                facet_data = facet.get("data", None)
                if facet_name is not None and facet_data is not None:
                    scoped_facet_data = get_scoped_data_reference(vega_spec, facet_data, scope)
                    if scoped_facet_data is not None:
                        facet_mapping[(facet_name, group_scope)] = scoped_facet_data

            # Handle children recursively
            child_mapping = get_facet_mapping(vega_spec, scope=group_scope)
            facet_mapping.update(child_mapping)
            group_index += 1

    return facet_mapping


def get_datasets_for_named_marks(vega_spec, magic_names, facet_mapping, scope=()):
    datasets = {}
    group_index = 0
    mark_group = get_mark_group_for_scope(vega_spec, scope) or {}
    for mark in mark_group.get("marks", []):
        for magic_name in magic_names:
            if mark.get("name", "") == f"{magic_name}_cell":
                data_name = mark.get("from", {}).get("facet", None).get("data", None)
                scoped_data_name = (data_name, scope)
                datasets[magic_name] = get_from_facet_mapping(scoped_data_name, facet_mapping)
                break

        name = mark.get("name", "")
        if mark.get("type", "") == "group":
            group_data_names = get_datasets_for_named_marks(
                vega_spec, magic_names, facet_mapping, scope=scope + (group_index,)
            )
            for k, v in group_data_names.items():
                datasets.setdefault(k, v)
            group_index += 1
        else:
            for magic_name in magic_names:
                if name.startswith(magic_name) and name.endswith("_marks"):
                    data_name = mark.get("from", {}).get("data", None)
                    scoped_data_name = get_scoped_data_reference(vega_spec, data_name, scope)
                    if scoped_data_name is not None:
                        datasets[magic_name] = get_from_facet_mapping(scoped_data_name, facet_mapping)
                        break

    return datasets


def get_from_facet_mapping(data_name, facet_mapping):
    while data_name in facet_mapping:
        data_name = facet_mapping[data_name]
    return data_name


def get_mark_group_for_scope(vega_spec, scope):
    group = vega_spec

    # Find group at scope
    for scope_value in scope:
        group_index = 0
        child_group = None
        for mark in group.get("marks", []):
            if mark.get("type") == "group":
                if group_index == scope_value:
                    child_group = mark
                    break
                group_index += 1
        if child_group is None:
            return None
        group = child_group

    return group


def get_datasets_for_scope(vega_spec, scope):
    group = get_mark_group_for_scope(vega_spec, scope) or {}

    # get datasets from group
    datasets = []
    for dataset in group.get("data", []):
        datasets.append(dataset["name"])

    # Add facet dataset
    facet_dataset = group.get("from", {}).get("facet", {}).get("name", None)
    if facet_dataset:
        datasets.append(facet_dataset)
    return datasets


def get_scoped_data_reference(vega_spec, data_name, usage_scope):
    for i in reversed(range(len(usage_scope) + 1)):
        scope = usage_scope[:i]
        datasets = get_datasets_for_scope(vega_spec, scope) or []
        if data_name in datasets:
            return (data_name, scope)
    return None
