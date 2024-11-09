from __future__ import annotations

import json
from typing import Any, TypedDict, cast

from ._vegafusion import (
    build_pre_transform_spec_plan as _build_pre_transform_spec_plan,
)
from ._vegafusion import (
    get_column_usage as _get_column_usage,
)


def get_column_usage(spec: dict[str, Any]) -> dict[str, list[str] | None]:
    """
    Compute the columns from each root dataset that are referenced in a
    Vega spec.

    Args:
        spec: Vega spec

    Returns:
        dict[str, list[str] | None]:
        Dict from root-level dataset name to either

        * A list of columns that are referenced in this dataset if this can
          be determined precisely
        * None if it was not possible to determine the full set of columns
          that are referenced from this dataset

    """
    return cast("dict[str, list[str] | None]", _get_column_usage(spec))


def get_inline_column_usage(
    spec: dict[str, Any] | str,
) -> dict[str, list[str]]:
    """
    Get the columns used by each inline dataset, if known
    """
    if isinstance(spec, str):
        spec_dict = cast("dict[str, Any]", json.loads(spec))
    else:
        spec_dict = spec

    # Build mapping from inline_dataset name to Vega dataset name
    inline_dataset_mapping: dict[str, str] = {}
    for dataset in spec_dict.get("data", []):
        url = cast("str | None", dataset.get("url", None))
        if url and (
            url.startswith("vegafusion+dataset://") or url.startswith("table://")
        ):
            parts = url.split("//")
            if len(parts) == 2:
                inline_dataset_name = parts[1]
                inline_dataset_mapping[inline_dataset_name] = dataset["name"]

    # Compute column usage
    usage = get_column_usage(spec_dict)
    return {
        inline_dataset_name: columns
        for inline_dataset_name in inline_dataset_mapping
        if (columns := usage[inline_dataset_mapping[inline_dataset_name]]) is not None
    }


class PreTransformSpecPlan(TypedDict):
    client_spec: dict[str, Any]
    server_spec: dict[str, Any]
    comm_plan: dict[str, Any]
    warnings: list[dict[str, Any]]


def build_pre_transform_spec_plan(
    spec: dict[str, Any],
    preserve_interactivity: bool | None = None,
    keep_signals: list[tuple[str, list[int]]] | None = None,
    keep_datasets: list[tuple[str, list[int]]] | None = None,
) -> PreTransformSpecPlan:
    """
    Diagnostic function that returns the plan used by the pre_transform_spec method

    Args:
        spec: A Vega specification dict or JSON string.
        preserve_interactivity: If True (default), the interactive behavior of the
            chart will be preserved. This requires that all the data that
            participates in interactions be included in the resulting spec rather
            than being pre-transformed. If False, all possible data transformations
            are applied even if they break the original interactive behavior of the
            chart.
        keep_signals: Signals from the input spec that must be included in the
            pre-transformed spec. A list with elements that are either:
            - The name of a top-level signal as a string
            - A two-element tuple where the first element is the name of a signal
                as a string and the second element is the nested scope of the dataset
                as a list of integers
        keep_datasets: Datasets from the input spec that must be included in the
            pre-transformed spec. A list with elements that are either:
            - The name of a top-level dataset as a string
            - A two-element tuple where the first element is the name of a dataset
                as a string and the second element is the nested scope of the dataset
                as a list of integers

    Returns:
        PreTransformSpecPlan: A TypedDict with the following keys:
            - "client_spec": Planned client spec
            - "server_spec": Planned server spec
            - "comm_plan": Communication plan
            - "warnings": List of planner warnings
    """
    return cast(
        PreTransformSpecPlan,
        _build_pre_transform_spec_plan(
            spec, preserve_interactivity, keep_signals, keep_datasets
        ),
    )
