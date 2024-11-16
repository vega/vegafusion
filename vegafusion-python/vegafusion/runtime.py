from __future__ import annotations

import sys
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal, TypedDict, Union, cast

import narwhals.stable.v1 as nw
from arro3.core import Table

from vegafusion._vegafusion import get_cpu_count, get_virtual_memory
from vegafusion.transformer import DataFrameLike
from vegafusion.utils import get_inline_column_usage

from .local_tz import get_local_tz

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl  # noqa: F401
    import pyarrow as pa
    from narwhals.typing import IntoFrameT

    from vegafusion._vegafusion import (
        PyChartState,
        PyChartStateGrpc,
        PyVegaFusionRuntime,
    )

# This type isn't defined in the grpcio package, so let's at least name it
UnaryUnaryMultiCallable = Any


def _get_common_full_namespace(
    inline_datasets: dict[str, Any] | None,
) -> ModuleType | None:
    namespaces: set[ModuleType] = set()
    try:
        if inline_datasets is not None:
            for df in inline_datasets.values():
                nw_df = nw.from_native(df)
                if nw.get_level(nw_df) == "full":
                    namespaces.add(nw.get_native_namespace(nw_df))

        if len(namespaces) == 1:
            return next(iter(namespaces))
        else:
            return None
    except TypeError:
        # Types not compatible with Narwhals
        return None


def _get_default_namespace() -> ModuleType:
    # Returns a default narwhals namespace, based on what is installed
    if pd := sys.modules.get("pandas") and sys.modules.get("pyarrow"):
        return pd
    elif pl := sys.modules.get("polars"):
        return pl
    elif pa := sys.modules.get("pyarrow"):
        return pa
    else:
        raise ImportError("Could not determine default narwhals namespace")


class VariableUpdate(TypedDict):
    name: str
    namespace: Literal["data", "signal"]
    scope: list[int]
    value: Any


class Variable(TypedDict):
    name: str
    namespace: Literal["data", "signal"]
    scope: list[int]


class CommPlan(TypedDict):
    client_to_server: list[Variable]
    server_to_client: list[Variable]


class PreTransformWarning(TypedDict):
    type: Literal["RowLimitExceeded", "BrokenInteractivity", "Unsupported"]
    message: str


DatasetFormat = Literal["auto", "polars", "pandas", "pyarrow", "arro3"]


class ChartState:
    def __init__(self, chart_state: PyChartState | PyChartStateGrpc) -> None:
        self._chart_state = chart_state

    def update(self, client_updates: list[VariableUpdate]) -> list[VariableUpdate]:
        """
        Update chart state with updates from the client.

        Args:
            client_updates: List of VariableUpdate values from the client.

        Returns:
            list of VariableUpdates that should be pushed to the client.
        """
        return cast(list[VariableUpdate], self._chart_state.update(client_updates))

    def get_comm_plan(self) -> CommPlan:
        """
        Get ChartState's communication plan.

        Returns:
            WatchPlan specifying the signals and datasets that should be communicated
            between ChartState and client to preserve the input Vega spec's
            interactivity.
        """
        return cast(CommPlan, self._chart_state.get_comm_plan())

    def get_watch_plan(self) -> CommPlan:
        """
        Get ChartState's communication plan.

        Alias for get_comm_plan() for backwards compatibility.

        Returns:
            WatchPlan specifying the signals and datasets that should be communicated
            between ChartState and client to preserve the input Vega spec's
            interactivity.
        """
        return self.get_comm_plan()

    def get_transformed_spec(self) -> dict[str, Any]:
        """
        Get initial transformed spec.

        Returns:
            The initial transformed spec, equivalent to the spec produced by
            vf.runtime.pre_transform_spec().
        """
        return cast(dict[str, Any], self._chart_state.get_transformed_spec())

    def get_warnings(self) -> list[PreTransformWarning]:
        """Get transformed spec warnings

        Returns:
            list[PreTransformWarning]: A list of warnings as dictionaries.
                Each warning dict has a 'type' key indicating the warning type,
                and a 'message' key containing a description of the warning.

                Potential warning types include:
                    'RowLimitExceeded': Some datasets in resulting Vega specification
                        have been truncated to the provided row limit
                    'BrokenInteractivity': Some interactive features may have been
                        broken in the resulting Vega specification
                    'Unsupported': No transforms in the provided Vega specification were
                        eligible for pre-transforming
        """
        return cast(list[PreTransformWarning], self._chart_state.get_warnings())

    def get_server_spec(self) -> dict[str, Any]:
        """
        Returns:
            dict: The server spec.
        """
        return cast(dict[str, Any], self._chart_state.get_server_spec())

    def get_client_spec(self) -> dict[str, Any]:
        """
        Get client spec.

        Returns:
            dict: The client spec.
        """
        return cast(dict[str, Any], self._chart_state.get_client_spec())


class VegaFusionRuntime:
    def __init__(
        self,
        cache_capacity: int = 64,
        memory_limit: int | None = None,
        worker_threads: int | None = None,
    ) -> None:
        """
        Initialize a VegaFusionRuntime.

        Args:
            cache_capacity: Cache capacity.
            memory_limit: Memory limit.
            worker_threads: Number of worker threads.
        """
        self._runtime = None
        self._grpc_url: str | None = None
        self._cache_capacity = cache_capacity
        self._memory_limit = memory_limit
        self._worker_threads = worker_threads

    @property
    def runtime(self) -> PyVegaFusionRuntime:
        """
        Get or initialize a VegaFusion runtime.

        Returns:
            The VegaFusion runtime.
        """
        if self._runtime is None:
            # Try to initialize a VegaFusion runtime
            from vegafusion._vegafusion import PyVegaFusionRuntime

            if self.memory_limit is None:
                self.memory_limit = get_virtual_memory() // 2
            if self.worker_threads is None:
                self.worker_threads = get_cpu_count()

            self._runtime = PyVegaFusionRuntime.new_embedded(
                self.cache_capacity,
                self.memory_limit,
                self.worker_threads,
            )
        return self._runtime

    def grpc_connect(self, url: str) -> None:
        """
        Connect to a VegaFusion server over gRPC at the provided gRPC url

        Args:
            url: URL of a running VegaFusion server
        """
        from vegafusion._vegafusion import PyVegaFusionRuntime

        self._grpc_url = url
        self._runtime = PyVegaFusionRuntime.new_grpc(url)

    @property
    def using_grpc(self) -> bool:
        """
        Check if using gRPC.

        Returns:
            True if using gRPC, False otherwise.
        """
        return self._grpc_url is not None

    def _import_inline_datasets(
        self,
        inline_datasets: dict[str, IntoFrameT] | None = None,
        inline_dataset_usage: dict[str, list[str]] | None = None,
    ) -> dict[str, Table]:
        """
        Import or register inline datasets.

        Args:
            inline_datasets: A dictionary from dataset names to pandas DataFrames or
                pyarrow Tables. Inline datasets may be referenced by the input
                specification using the following url syntax
                'vegafusion+dataset://{dataset_name}' or 'table://{dataset_name}'.
            inline_dataset_usage: Columns that are referenced by datasets. If no
                entry is found, then all columns should be included.
        """
        if not TYPE_CHECKING:
            pd = sys.modules.get("pandas", None)
            pa = sys.modules.get("pyarrow", None)

        inline_datasets = inline_datasets or {}
        inline_dataset_usage = inline_dataset_usage or {}
        imported_inline_datasets: dict[str, Table] = {}
        for name, value in inline_datasets.items():
            columns = inline_dataset_usage.get(name)
            if pd is not None and pa is not None and isinstance(value, pd.DataFrame):
                # rename to help mypy
                inner_value: pd.DataFrame = value
                del value

                # Project down columns if possible
                if columns is not None:
                    inner_value = inner_value[columns]

                # Convert problematic object columns to strings
                for col, dtype in inner_value.dtypes.items():
                    if dtype.kind == "O":
                        try:
                            # See if the Table constructor can handle column by itself
                            col_tbl = Table(inner_value[[col]])

                            # If so, keep the arrow version so that it's more efficient
                            # to convert as part of the whole table later
                            inner_value = inner_value.assign(
                                **{
                                    col: pd.arrays.ArrowExtensionArray(
                                        pa.chunked_array(col_tbl.column(0))
                                    )
                                }
                            )
                        except TypeError:
                            # If the Table constructor can't handle the object column,
                            # convert the column to pyarrow strings
                            inner_value = inner_value.assign(
                                **{col: inner_value[col].astype("string[pyarrow]")}
                            )
                if hasattr(inner_value, "__arrow_c_stream__"):
                    # TODO: this requires pyarrow 14.0.0 or later
                    imported_inline_datasets[name] = Table(inner_value)
                else:
                    # Older pandas, convert through pyarrow
                    imported_inline_datasets[name] = Table(pa.from_pandas(inner_value))
            elif isinstance(value, dict):
                # Let narwhals import the dict using a default backend
                df_nw = nw.from_dict(value, native_namespace=_get_default_namespace())
                imported_inline_datasets[name] = Table(df_nw)
            else:
                # Import through PyCapsule interface or narwhals
                try:
                    df_nw = nw.from_native(value)

                    # Project down columns if possible
                    if columns is not None:
                        missing_col = [
                            col for col in columns if col not in df_nw.columns
                        ]
                        if missing_col:
                            msg = (
                                "Columns found in chart spec but not in DataFrame: "
                                f"{missing_col}"
                            )
                            raise ValueError(msg)
                        df_nw = df_nw.select(columns)

                    imported_inline_datasets[name] = Table(df_nw)  # type: ignore[arg-type]
                except TypeError:
                    # Not supported by Narwhals, try pycapsule interface directly
                    if hasattr(value, "__arrow_c_stream__"):
                        imported_inline_datasets[name] = Table(value)  # type: ignore[arg-type]
                    else:
                        raise

        return imported_inline_datasets

    def pre_transform_spec(
        self,
        spec: Union[dict[str, Any], str],
        local_tz: str | None = None,
        default_input_tz: str | None = None,
        row_limit: int | None = None,
        preserve_interactivity: bool = True,
        inline_datasets: dict[str, Any] | None = None,
        keep_signals: list[Union[str, tuple[str, list[int]]]] | None = None,
        keep_datasets: list[Union[str, tuple[str, list[int]]]] | None = None,
    ) -> tuple[dict[str, Any], list[PreTransformWarning]]:
        """
        Evaluate supported transforms in an input Vega specification

        Produces a new specification with pre-transformed datasets included inline.

        Args:
            spec: A Vega specification dict or JSON string
            local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
                Defaults to the value of vf.get_local_tz(), which defaults to the system
                timezone if one can be determined.
            default_input_tz: Name of timezone (e.g. 'America/New_York') that naive
                datetime strings should be interpreted in. Defaults to `local_tz`.
            row_limit: Maximum number of dataset rows to include in the returned
                specification. If exceeded, datasets will be truncated to this number
                of rows and a RowLimitExceeded warning will be included in the
                resulting warnings list
            preserve_interactivity: If True (default) then the interactive behavior of
                the chart will be preserved. This requires that all the data that
                participates in interactions be included in the resulting spec rather
                than being pre-transformed. If False, then all possible data
                transformations are applied even if they break the original interactive
                behavior of the chart.
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}' or
                'table://{dataset_name}'.
            keep_signals: Signals from the input spec that must be included in the
                pre-transformed spec, even if they are no longer referenced.
                A list with elements that are either:

                * The name of a top-level signal as a string
                * A two-element tuple where the first element is the name of a signal
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers
            keep_datasets: Datasets from the input spec that must be included in the
                pre-transformed spec even if they are no longer referenced.
                A list with elements that are either:

                * The name of a top-level dataset as a string
                * A two-element tuple where the first element is the name of a dataset
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers

        Returns:
            tuple[dict[str, Any], list[PreTransformWarning]]:
            Two-element tuple of

            * The Vega specification as a dict with pre-transformed datasets
              included inline
            * A list of warnings as dictionaries. Each warning dict has a ``'type'``
              key indicating the warning type, and a ``'message'`` key containing
              a description of the warning. Potential warning types include:

              * ``'RowLimitExceeded'``: Some datasets in resulting Vega specification
                have been truncated to the provided row limit
              * ``'BrokenInteractivity'``: Some interactive features may have been
                broken in the resulting Vega specification
              * ``'Unsupported'``: No transforms in the provided Vega specification
                were eligible for pre-transforming
        """
        local_tz = local_tz or get_local_tz()
        imported_inline_dataset = self._import_inline_datasets(
            inline_datasets, get_inline_column_usage(spec)
        )

        new_spec, warnings = self.runtime.pre_transform_spec(
            spec,
            local_tz=local_tz,
            default_input_tz=default_input_tz,
            row_limit=row_limit,
            preserve_interactivity=preserve_interactivity,
            inline_datasets=imported_inline_dataset,
            keep_signals=parse_variables(keep_signals),
            keep_datasets=parse_variables(keep_datasets),
        )

        return new_spec, warnings

    def new_chart_state(
        self,
        spec: Union[dict[str, Any], str],
        local_tz: str | None = None,
        default_input_tz: str | None = None,
        row_limit: int | None = None,
        inline_datasets: dict[str, DataFrameLike] | None = None,
    ) -> ChartState:
        """
        Construct new ChartState object.

        Args:
            spec: A Vega specification dict or JSON string.
            local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
                Defaults to the value of vf.get_local_tz(), which defaults to the system
                timezone if one can be determined.
            default_input_tz: Name of timezone (e.g. 'America/New_York') that naive
                datetime strings should be interpreted in. Defaults to `local_tz`.
            row_limit: Maximum number of dataset rows to include in the returned
                datasets. If exceeded, datasets will be truncated to this number of
                rows and a RowLimitExceeded warning will be included in the ChartState's
                warnings list.
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}' or
                'table://{dataset_name}'.

        Returns:
            ChartState
        """
        local_tz = local_tz or get_local_tz()
        inline_arrow_dataset = self._import_inline_datasets(
            inline_datasets, get_inline_column_usage(spec)
        )
        return ChartState(
            self.runtime.new_chart_state(
                spec, local_tz, default_input_tz, row_limit, inline_arrow_dataset
            )
        )

    def pre_transform_datasets(
        self,
        spec: Union[dict[str, Any], str],
        datasets: list[Union[str, tuple[str, list[int]]]],
        local_tz: str | None = None,
        default_input_tz: str | None = None,
        row_limit: int | None = None,
        inline_datasets: dict[str, DataFrameLike] | None = None,
        trim_unused_columns: bool = False,
        dataset_format: DatasetFormat = "auto",
    ) -> tuple[list[DataFrameLike], list[PreTransformWarning]]:
        """
        Extract the fully evaluated form of the requested datasets from a Vega
        specification.

        Args:
            spec: A Vega specification dict or JSON string.
            datasets: A list with elements that are either:

                * The name of a top-level dataset as a string
                * A two-element tuple where the first element is the name of a dataset
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers
            local_tz: Name of timezone to be considered local. E.g.
                ``'America/New_York'``. Defaults to the value of vf.get_local_tz(),
                which defaults to the system timezone if one can be determined.
            default_input_tz: Name of timezone (e.g. ``'America/New_York'``) that naive
                datetime strings should be interpreted in. Defaults to ``local_tz``.
            row_limit: Maximum number of dataset rows to include in the returned
                datasets. If exceeded, datasets will be truncated to this number of
                rows and a RowLimitExceeded warning will be included in the resulting
                warnings list.
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}'
                or 'table://{dataset_name}'.
            trim_unused_columns: If True, unused columns are removed from returned
                datasets.
            dataset_format: Format for returned datasets. One of:

                * ``"auto"``: (default) Infer the result type based on the types of
                  inline datasets. If no inline datasets are provided, return type will
                  depend on installed packages.
                * ``"polars"``: polars.DataFrame
                * ``"pandas"``: pandas.DataFrame
                * ``"pyarrow"``: pyarrow.Table
                * ``"arro3"``: arro3.Table

        Returns:
            tuple[list[DataFrameLike], list[PreTransformWarning]]:
            Two-element tuple of

            * List of pandas DataFrames corresponding to the input datasets list
            * A list of warnings as dictionaries. Each warning dict has a 'type'
              key indicating the warning type, and a 'message' key containing a
              description of the warning.
        """
        local_tz = local_tz or get_local_tz()

        # Build input variables
        pre_tx_vars = parse_variables(datasets)

        # Serialize inline datasets
        inline_arrow_dataset = self._import_inline_datasets(
            inline_datasets,
            inline_dataset_usage=get_inline_column_usage(spec)
            if trim_unused_columns
            else None,
        )

        values, warnings = self.runtime.pre_transform_datasets(
            spec,
            pre_tx_vars,
            local_tz=local_tz,
            default_input_tz=default_input_tz,
            row_limit=row_limit,
            inline_datasets=inline_arrow_dataset,
        )

        def normalize_timezones(
            dfs: list[nw.DataFrame[IntoFrameT] | nw.LazyFrame[IntoFrameT]],
        ) -> list[DataFrameLike]:
            # Convert to `local_tz` (or, set to UTC and then convert if starting
            # from time-zone-naive data), then extract the native DataFrame to return.
            processed_datasets = []
            for df in dfs:
                df = df.with_columns(
                    nw.col(col).dt.convert_time_zone(local_tz)
                    for col, dtype in df.schema.items()
                    if dtype == nw.Datetime
                )
                processed_datasets.append(df.to_native())
            return processed_datasets

        # Wrap result dataframes in Narwhals, using the input type and arrow
        # PyCapsule interface
        if dataset_format != "auto":
            if dataset_format == "polars":
                import polars as pl

                datasets = normalize_timezones(
                    [nw.from_native(pl.DataFrame(value)) for value in values]
                )
            elif dataset_format == "pandas":
                import pyarrow as pa

                datasets = normalize_timezones(
                    [nw.from_native(pa.table(value).to_pandas()) for value in values]
                )
            elif dataset_format == "pyarrow":
                import pyarrow as pa

                datasets = normalize_timezones(
                    [nw.from_native(pa.table(value)) for value in values]
                )
            elif dataset_format == "arro3":
                # Pass through arrof3
                datasets = values
            else:
                raise ValueError(f"Unrecognized dataset_format: {dataset_format}")
        elif (namespace := _get_common_full_namespace(inline_datasets)) is not None:
            # Infer the type from the inline datasets
            datasets = normalize_timezones(
                [nw.from_arrow(value, native_namespace=namespace) for value in values]
            )
        else:
            # Either no inline datasets, inline datasets with mixed or
            # unrecognized types
            try:
                # Try pandas
                import pandas as _pd  # noqa: F401
                import pyarrow as pa

                datasets = normalize_timezones(
                    [nw.from_native(pa.table(value).to_pandas()) for value in values]
                )
            except ImportError:
                try:
                    # Try polars
                    import polars as pl

                    datasets = normalize_timezones(
                        [nw.from_native(pl.DataFrame(value)) for value in values]
                    )
                except ImportError:
                    # Fall back to arro3
                    datasets = values

        return datasets, warnings

    def pre_transform_extract(
        self,
        spec: dict[str, Any] | str,
        local_tz: str | None = None,
        default_input_tz: str | None = None,
        preserve_interactivity: bool = True,
        extract_threshold: int = 20,
        extracted_format: str = "arro3",
        inline_datasets: dict[str, DataFrameLike] | None = None,
        keep_signals: list[str | tuple[str, list[int]]] | None = None,
        keep_datasets: list[str | tuple[str, list[int]]] | None = None,
    ) -> tuple[
        dict[str, Any], list[tuple[str, list[int], pa.Table]], list[PreTransformWarning]
    ]:
        """
        Evaluate supported transforms in an input Vega specification.

        Produces a new specification with small pre-transformed datasets
        (under ``extract_threshold`` rows) included inline and larger inline
        datasets (``extract_threshold`` rows or more) extracted into arrow tables.

        Args:
            spec: A Vega specification dict or JSON string.
            local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
                Defaults to the value of vf.get_local_tz(), which defaults to the system
                timezone if one can be determined.
            default_input_tz: Name of timezone (e.g. 'America/New_York') that naive
                datetime strings should be interpreted in. Defaults to `local_tz`.
            preserve_interactivity: If True (default) then the interactive behavior of
                the chart will be preserved. This requires that all the data that
                participates in interactions be included in the resulting spec rather
                than being pre-transformed. If False, then all possible data
                transformations are applied even if they break the original interactive
                behavior of the chart.
            extract_threshold: Datasets with length below extract_threshold will be
                inlined.
            extracted_format: The format for the extracted datasets. Options are:

                * ``"arro3"``: (default) arro3.Table
                * ``"pyarrow"``: pyarrow.Table
                * ``"arrow-ipc"``: bytes in arrow IPC format
                * ``"arrow-ipc-base64"``: base64 encoded arrow IPC format
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}' or
                'table://{dataset_name}'.
            keep_signals: Signals from the input spec that must be included in the
                pre-transformed spec, even if they are no longer referenced.
                A list with elements that are either:

                * The name of a top-level signal as a string
                * A two-element tuple where the first element is the name of a signal
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers
            keep_datasets: Datasets from the input spec that must be included in the
                pre-transformed spec even if they are no longer referenced.
                A list with elements that are either:

                * The name of a top-level dataset as a string
                * A two-element tuple where the first element is the name of a dataset
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers

        Returns:
            tuple[dict[str, Any], list[tuple[str, list[int], pa.Table]], list[PreTransformWarning]]:
            Three-element tuple of

            * The Vega specification as a dict with pre-transformed datasets
              included but left empty.
            * Extracted datasets as a list of three element tuples
               * dataset name
               * dataset scope list
               * arrow data
            * A list of warnings as dictionaries. Each warning dict has a ``'type'``
              key indicating the warning type, and a ``'message'`` key containing
              a description of the warning. Potential warning types include:

              * ``'RowLimitExceeded'``: Some datasets in resulting Vega specification
                have been truncated to the provided row limit
              * ``'BrokenInteractivity'``: Some interactive features may have been
                broken in the resulting Vega specification
              * ``'Unsupported'``: No transforms in the provided Vega specification
                were eligible for pre-transforming
        """  # noqa: E501
        local_tz = local_tz or get_local_tz()

        inline_arrow_dataset = self._import_inline_datasets(
            inline_datasets, get_inline_column_usage(spec)
        )

        new_spec, datasets, warnings = self.runtime.pre_transform_extract(
            spec,
            local_tz=local_tz,
            default_input_tz=default_input_tz,
            preserve_interactivity=preserve_interactivity,
            extract_threshold=extract_threshold,
            extracted_format=extracted_format,
            inline_datasets=inline_arrow_dataset,
            keep_signals=keep_signals,
            keep_datasets=keep_datasets,
        )

        return new_spec, datasets, warnings

    @property
    def worker_threads(self) -> int | None:
        """
        Get the number of worker threads for the runtime.

        Returns:
            Number of threads for the runtime
        """
        return self._worker_threads

    @worker_threads.setter
    def worker_threads(self, value: int) -> None:
        """
        Restart the runtime with the specified number of worker threads

        Args:
            value: Number of threads for the new runtime
        """
        if value != self._worker_threads:
            self._worker_threads = value
            self.reset()

    @property
    def total_memory(self) -> int | None:
        if self._runtime:
            return self._runtime.total_memory()
        else:
            return None

    @property
    def _protected_memory(self) -> int | None:
        if self._runtime:
            return self._runtime.protected_memory()
        else:
            return None

    @property
    def _probationary_memory(self) -> int | None:
        if self._runtime:
            return self._runtime.probationary_memory()
        else:
            return None

    @property
    def size(self) -> int | None:
        if self._runtime:
            return self._runtime.size()
        else:
            return None

    @property
    def memory_limit(self) -> int | None:
        return self._memory_limit

    @memory_limit.setter
    def memory_limit(self, value: int) -> None:
        """
        Restart the runtime with the specified memory limit

        Args:
            value: Max approximate memory usage of cache
        """
        if value != self._memory_limit:
            self._memory_limit = value
            self.reset()

    @property
    def cache_capacity(self) -> int:
        return self._cache_capacity

    @cache_capacity.setter
    def cache_capacity(self, value: int) -> None:
        """
        Restart the runtime with the specified cache capacity

        Args:
            value: Max task graph values to cache
        """
        if value != self._cache_capacity:
            self._cache_capacity = value
            self.reset()

    def reset(self) -> None:
        """
        Restart the runtime
        """
        if self._runtime is not None:
            self._runtime.clear_cache()
            self._runtime = None

    def clear_cache(self) -> None:
        """
        Clear the VegaFusion runtime cache
        """
        if self._runtime is not None:
            self._runtime.clear_cache()

    def __repr__(self) -> str:
        if self.using_grpc:
            return f"VegaFusionRuntime(url={self._grpc_url})"
        else:
            return (
                f"VegaFusionRuntime(cache_capacity={self.cache_capacity}, "
                f"worker_threads={self.worker_threads})"
            )


def parse_variables(
    variables: list[str | tuple[str, list[int]]] | None,
) -> list[tuple[str, list[int]]]:
    """
    Parse VegaFusion variables.

    Args:
        variables: List of VegaFusion variables.

    Returns:
        List of parsed VegaFusion variables.
    """
    # Build input variables
    pre_tx_vars: list[tuple[str, list[int]]] = []
    if variables is None:
        return []

    if isinstance(variables, str):
        variables = [variables]

    err_msg = "Elements of variables argument must be strings or two-element tuples"
    for var in variables:
        if isinstance(var, str):
            pre_tx_vars.append((var, []))
        elif isinstance(var, (list, tuple)):
            if len(var) == 2:
                pre_tx_vars.append((var[0], list(var[1])))
            else:
                raise ValueError(err_msg)
        else:
            raise ValueError(err_msg)
    return pre_tx_vars


def get_mark_group_for_scope(
    vega_spec: dict[str, Any], scope: list[int]
) -> dict[str, Any] | None:
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


runtime = VegaFusionRuntime(64, None, None)
