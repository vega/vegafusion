from __future__ import annotations

from typing import Any, cast

from ._vegafusion import get_column_usage as _get_column_usage


def get_column_usage(spec: dict[str, Any]) -> dict[str, list[str] | None]:
    """
    Compute the columns from each root dataset that are referenced in a
    Vega spec.

    Args:
        spec: Vega spec

    Returns:
        dict[str, list[str] | None]: Dict from root-level dataset name
            to either:
               - A list of columns that are referenced in this dataset if this can
                 be determined precisely
               - None if it was not possible to determine the full set of columns
                 that are referenced from this dataset
    """
    return cast("dict[str, list[str] | None]", _get_column_usage(spec))
