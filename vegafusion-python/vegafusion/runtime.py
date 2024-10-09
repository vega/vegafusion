from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Literal, TypedDict, Union, cast

import psutil

from vegafusion.datasource.datasource import Datasource
from vegafusion.transformer import DataFrameLike

from .connection import SqlConnection
from .dataset import SqlDataset
from .datasource import DfiDatasource, PandasDatasource, PyArrowDatasource
from .local_tz import get_local_tz

if TYPE_CHECKING:
    import pyarrow as pa
    from duckdb import DuckDBPyConnection
    from grpc import Channel

    from vegafusion._vegafusion import PyChartState, PyVegaFusionRuntime

# This type isn't defined in the grpcio package, so let's at least name it
UnaryUnaryMultiCallable = Any


def _all_datasets_have_type(
    inline_datasets: dict[str, Any] | None, types: tuple[type, ...]
) -> bool:
    """
    Check if all datasets in inline_datasets are instances of the given types.

    Args:
        inline_datasets: A dictionary of inline datasets.
        types: A tuple of types to check against.

    Returns:
        bool: True if all datasets are instances of the given types, False otherwise.
    """
    if not inline_datasets:
        # If there are no inline datasets, return false
        # (we want the default pandas behavior in this case)
        return False
    else:
        for dataset in inline_datasets.values():
            if not isinstance(dataset, types):
                return False
        return True


class VariableUpdate(TypedDict):
    name: str
    namespace: Literal["data", "signal"]
    scope: list[int]
    value: Any


class Watch(TypedDict):
    name: str
    namespace: Literal["data", "signal"]
    scope: list[int]


class WatchPlan(TypedDict):
    client_to_server: list[Watch]
    server_to_client: list[Watch]


class PreTransformWarning(TypedDict):
    type: Literal["RowLimitExceeded", "BrokenInteractivity", "Unsupported"]
    message: str


class ChartState:
    def __init__(self, chart_state: PyChartState) -> None:
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

    def get_watch_plan(self) -> WatchPlan:
        """
        Get ChartState's watch plan.

        Returns:
            WatchPlan specifying the signals and datasets that should be communicated
            between ChartState and client to preserve the input Vega spec's
            interactivity.
        """
        return cast(WatchPlan, self._chart_state.get_watch_plan())

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
        cache_capacity: int,
        memory_limit: int,
        worker_threads: int,
        connection: SqlConnection | None = None,
    ) -> None:
        """
        Initialize a VegaFusionRuntime.

        Args:
            cache_capacity: Cache capacity.
            memory_limit: Memory limit.
            worker_threads: Number of worker threads.
            connection: SQL connection (optional).
        """
        self._embedded_runtime = None
        self._grpc_channel = None
        self._grpc_query = None
        self._cache_capacity = cache_capacity
        self._memory_limit = memory_limit
        self._worker_threads = worker_threads
        self._connection = connection

    @property
    def embedded_runtime(self) -> PyVegaFusionRuntime:
        """
        Get or initialize the embedded runtime.

        Returns:
            The embedded runtime.
        """
        if self._embedded_runtime is None:
            # Try to initialize an embedded runtime
            from vegafusion._vegafusion import PyVegaFusionRuntime

            self._embedded_runtime = PyVegaFusionRuntime(
                self.cache_capacity,
                self.memory_limit,
                self.worker_threads,
                connection=self._connection,
            )
        return self._embedded_runtime

    def set_connection(
        self,
        connection: Literal["datafusion", "duckdb"]
        | SqlConnection
        | DuckDBPyConnection
        | None = "datafusion",
    ) -> None:
        """
        Sets the connection to use to evaluate Vega data transformations.

        Named tables returned by the connection's `tables` method may be referenced in
        Vega/Altair chart specifications using special dataset URLs. For example, if the
        connection's `tables` method returns a dictionary that includes "tableA" as a
        key, then this table may be referenced in a chart specification using the URL
        "table://tableA" or "vegafusion+dataset://tableA".

        Args:
            connection: One of:
              - An instance of vegafusion.connection.SqlConnection
              - An instance of a duckdb connection
              - A string, one of:
                    - "datafusion" (default)
                    - "duckdb"
        """
        # Don't import duckdb unless it's already loaded. If it's not loaded,
        # then the input connection can't be a duckdb connection.
        duckdb = sys.modules.get("duckdb", None)
        if isinstance(connection, str):
            if connection == "datafusion":
                # Connection of None uses DataFusion
                connection = None
            elif connection == "duckdb":
                from vegafusion.connection.duckdb import DuckDbConnection

                connection = DuckDbConnection()
            else:
                raise ValueError(f"Unsupported connection name: {connection}")
        elif duckdb is not None and isinstance(connection, duckdb.DuckDBPyConnection):
            from vegafusion.connection.duckdb import DuckDbConnection

            connection = DuckDbConnection(connection)
        elif not isinstance(connection, SqlConnection):
            raise ValueError(
                "connection argument must be a string or an instance of SqlConnection\n"
                f"Received value of type {type(connection).__name__}: {connection}"
            )

        self._connection = connection
        self.reset()

    def grpc_connect(self, channel: Channel) -> None:
        """
        Connect to a VegaFusion server over gRPC using the provided gRPC channel

        Args:
            channel: grpc.Channel instance configured with the address of a running
                     VegaFusion server
        """
        self._grpc_channel = channel

    @property
    def using_grpc(self) -> bool:
        """
        Check if using gRPC.

        Returns:
            True if using gRPC, False otherwise.
        """
        return self._grpc_channel is not None

    @property
    def grpc_query(self) -> UnaryUnaryMultiCallable:
        """
        Get the gRPC query object.

        Returns:
            The gRPC query object.

        Raises:
            ValueError: If no gRPC channel is registered.
        """
        if self._grpc_channel is None:
            raise ValueError(
                "No grpc channel registered. Use runtime.grpc_connect to provide "
                "a grpc channel"
            )

        if self._grpc_query is None:
            self._grpc_query = self._grpc_channel.unary_unary(
                "/services.VegaFusionRuntime/TaskGraphQuery",
            )
        return self._grpc_query

    def process_request_bytes(self, request: bytes) -> bytes:
        """
        Process a request in bytes format.

        Args:
            request: The request in bytes format.

        Returns:
            The processed request in bytes format.
        """
        if self._grpc_channel:
            return self.grpc_query(request)
        else:
            # No grpc channel, get or initialize an embedded runtime
            return cast(bytes, self.embedded_runtime.process_request_bytes(request))

    def _import_or_register_inline_datasets(
        self, inline_datasets: dict[str, DataFrameLike] | None = None
    ) -> dict[str, Datasource | SqlDataset]:
        """
        Import or register inline datasets.

        Args:
            inline_datasets: A dictionary from dataset names to pandas DataFrames or
                pyarrow Tables. Inline datasets may be referenced by the input
                specification using the following url syntax
                'vegafusion+dataset://{dataset_name}' or 'table://{dataset_name}'.
        """
        pl = sys.modules.get("polars", None)
        pa = sys.modules.get("pyarrow", None)
        pd = sys.modules.get("pandas", None)

        inline_datasets = inline_datasets or {}
        imported_inline_datasets: dict[str, Datasource | SqlDataset] = {}
        for name, value in inline_datasets.items():
            if isinstance(value, SqlDataset):
                imported_inline_datasets[name] = value
            elif pd is not None and isinstance(value, pd.DataFrame):
                if self._connection is not None:
                    try:
                        # Try registering DataFrame if supported
                        self._connection.register_pandas(name, value, temporary=True)
                        continue
                    except ValueError:
                        pass

                imported_inline_datasets[name] = PandasDatasource(value)
            elif hasattr(value, "__dataframe__"):
                # Let polars convert to pyarrow since it has broader support than the
                # raw dataframe interchange protocol, and "This operation is mostly
                # zero copy."
                try:
                    if pl is not None and isinstance(value, pl.DataFrame):
                        value = value.to_arrow()
                except ImportError:
                    pass

                if pa is not None and isinstance(value, pa.Table):
                    try:
                        if self._connection is not None:
                            # Try registering Arrow Table if supported
                            self._connection.register_arrow(name, value, temporary=True)
                            continue
                    except ValueError:
                        pass
                    imported_inline_datasets[name] = PyArrowDatasource(value)
                else:
                    imported_inline_datasets[name] = DfiDatasource(value)
            else:
                raise ValueError(f"Unsupported DataFrame type: {type(value)}")

        return imported_inline_datasets

    def build_pre_transform_spec_plan(
        self,
        spec: dict[str, Any] | str,
        preserve_interactivity: bool = True,
        keep_signals: list[str | tuple[str, list[int]]] | None = None,
        keep_datasets: list[str | tuple[str, list[int]]] | None = None,
    ) -> dict[str, Any]:
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
            dict: A dictionary with the following keys:
                - "client_spec": Planned client spec
                - "server_spec": Planned server spec
                - "comm_plan": Communication plan
                - "warnings": List of planner warnings
        """
        if self._grpc_channel:
            raise ValueError(
                "build_pre_transform_spec_plan not yet supported over gRPC"
            )
        else:
            plan = self.embedded_runtime.build_pre_transform_spec_plan(
                spec,
                preserve_interactivity=preserve_interactivity,
                keep_signals=parse_variables(keep_signals),
                keep_datasets=parse_variables(keep_datasets),
            )
            return cast(dict[str, Any], plan)

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
        data_encoding_threshold: int | None = None,
        data_encoding_format: str = "pyarrow",
    ) -> tuple[Union[dict[str, Any], str], list[dict[str, str]]]:
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
                the chart will pre preserved. This requires that all the data that
                participates in interactions be included in the resulting spec rather
                than being pre-transformed. If False, then all possible data
                transformations are applied even if they break the original interactive
                behavior of the chart.
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}' or
                'table://{dataset_name}'.
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
            data_encoding_threshold: threshold for encoding datasets. When length of
                pre-transformed datasets exceeds data_encoding_threshold, datasets are
                encoded into an alternative format (as determined by the
                data_encoding_format argument). When None (the default),
                pre-transformed datasets are never encoded and are always included as
                JSON compatible lists of dictionaries.
            data_encoding_format: format of encoded datasets. Format to use to encode
                datasets with length exceeding the data_encoding_threshold argument.
                - "pyarrow": Encode datasets as pyarrow Tables. Not JSON compatible.
                - "arrow-ipc": Encode datasets as bytes in Arrow IPC format. Not JSON
                  compatible.
                - "arrow-ipc-base64": Encode datasets as strings in base64 encoded
                  Arrow IPC format. JSON compatible.

        Returns:
            A tuple containing:
            - A string containing the JSON representation of a Vega specification
              with pre-transformed datasets included inline
            - A list of warnings as dictionaries. Each warning dict has a 'type'
              key indicating the warning type, and a 'message' key containing
              a description of the warning. Potential warning types include:
                'RowLimitExceeded': Some datasets in resulting Vega specification
                    have been truncated to the provided row limit
                'BrokenInteractivity': Some interactive features may have been
                    broken in the resulting Vega specification
                'Unsupported': No transforms in the provided Vega specification were
                    eligible for pre-transforming
        """
        if self._grpc_channel:
            raise ValueError("pre_transform_spec not yet supported over gRPC")
        else:
            local_tz = local_tz or get_local_tz()
            imported_inline_dataset = self._import_or_register_inline_datasets(
                inline_datasets
            )

            try:
                if data_encoding_threshold is None:
                    new_spec, warnings = self.embedded_runtime.pre_transform_spec(
                        spec,
                        local_tz=local_tz,
                        default_input_tz=default_input_tz,
                        row_limit=row_limit,
                        preserve_interactivity=preserve_interactivity,
                        inline_datasets=imported_inline_dataset,
                        keep_signals=parse_variables(keep_signals),
                        keep_datasets=parse_variables(keep_datasets),
                    )
                else:
                    # Use pre_transform_extract to extract large datasets
                    new_spec, datasets, warnings = (
                        self.embedded_runtime.pre_transform_extract(
                            spec,
                            local_tz=local_tz,
                            default_input_tz=default_input_tz,
                            preserve_interactivity=preserve_interactivity,
                            extract_threshold=data_encoding_threshold,
                            extracted_format=data_encoding_format,
                            inline_datasets=imported_inline_dataset,
                            keep_signals=parse_variables(keep_signals),
                            keep_datasets=parse_variables(keep_datasets),
                        )
                    )

                    # Insert encoded datasets back into spec
                    for name, scope, tbl in datasets:
                        group = get_mark_group_for_scope(new_spec, scope) or {}
                        for data in group.get("data", []):
                            if data.get("name", None) == name:
                                data["values"] = tbl

            finally:
                # Clean up temporary tables
                if self._connection is not None:
                    self._connection.unregister_temporary_tables()

            return new_spec, warnings

    def new_chart_state(
        self,
        spec: Union[dict[str, Any], str],
        local_tz: str | None = None,
        default_input_tz: str | None = None,
        row_limit: int | None = None,
        inline_datasets: dict[str, DataFrameLike] | None = None,
    ) -> ChartState:
        """Construct new ChartState object.

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
            ChartState object.
        """
        if self._grpc_channel:
            raise ValueError("new_chart_state not yet supported over gRPC")
        else:
            local_tz = local_tz or get_local_tz()
            inline_arrow_dataset = self._import_or_register_inline_datasets(
                inline_datasets
            )
            return ChartState(
                self.embedded_runtime.new_chart_state(
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
    ) -> tuple[list[DataFrameLike], list[dict[str, str]]]:
        """Extract the fully evaluated form of the requested datasets from a Vega
        specification.

        Extracts datasets as pandas DataFrames.

        Args:
            spec: A Vega specification dict or JSON string.
            datasets: A list with elements that are either:
                - The name of a top-level dataset as a string
                - A two-element tuple where the first element is the name of a dataset
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers
            local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
                Defaults to the value of vf.get_local_tz(), which defaults to the
                system timezone if one can be determined.
            default_input_tz: Name of timezone (e.g. 'America/New_York') that naive
                datetime strings should be interpreted in. Defaults to `local_tz`.
            row_limit: Maximum number of dataset rows to include in the returned
                datasets. If exceeded, datasets will be truncated to this number of
                rows and a RowLimitExceeded warning will be included in the resulting
                warnings list.
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}'
                or 'table://{dataset_name}'.

        Returns:
            A tuple containing:
                - List of pandas DataFrames corresponding to the input datasets list
                - A list of warnings as dictionaries. Each warning dict has a 'type'
                  key indicating the warning type, and a 'message' key containing a
                  description of the warning.
        """
        if self._grpc_channel:
            raise ValueError("pre_transform_datasets not yet supported over gRPC")
        else:
            local_tz = local_tz or get_local_tz()

            # Build input variables
            pre_tx_vars = parse_variables(datasets)

            # Serialize inline datasets
            inline_arrow_dataset = self._import_or_register_inline_datasets(
                inline_datasets
            )
            try:
                values, warnings = self.embedded_runtime.pre_transform_datasets(
                    spec,
                    pre_tx_vars,
                    local_tz=local_tz,
                    default_input_tz=default_input_tz,
                    row_limit=row_limit,
                    inline_datasets=inline_arrow_dataset,
                )
            finally:
                # Clean up registered tables (both inline and internal temporary tables)
                if self._connection is not None:
                    self._connection.unregister_temporary_tables()

            pl = sys.modules.get("polars", None)
            pa = sys.modules.get("pyarrow", None)
            if pl is not None and _all_datasets_have_type(
                inline_datasets, (pl.DataFrame, pl.LazyFrame)
            ):
                if TYPE_CHECKING:
                    import polars as pl

                # Deserialize values to Polars tables
                pl_dataframes = [pl.from_arrow(value) for value in values]

                # Localize datetime columns to UTC
                processed_datasets = []
                for df in pl_dataframes:
                    for name, dtype in zip(df.columns, df.dtypes):
                        if dtype == pl.Datetime:
                            df = df.with_columns(
                                df[name]
                                .dt.replace_time_zone("UTC")
                                .dt.convert_time_zone(local_tz)
                            )
                    processed_datasets.append(df)

                return processed_datasets, warnings
            elif pa is not None and _all_datasets_have_type(inline_datasets, pa.Table):
                return values, warnings
            else:
                # Deserialize values to pandas DataFrames
                datasets = [value.to_pandas() for value in values]

                # Localize datetime columns to UTC
                for df in datasets:
                    for name, dtype in df.dtypes.items():
                        if dtype.kind == "M":
                            df[name] = (
                                df[name].dt.tz_localize("UTC").dt.tz_convert(local_tz)
                            )

                return datasets, warnings

    def pre_transform_extract(
        self,
        spec: dict[str, Any] | str,
        local_tz: str | None = None,
        default_input_tz: str | None = None,
        preserve_interactivity: bool = True,
        extract_threshold: int = 20,
        extracted_format: str = "pyarrow",
        inline_datasets: dict[str, DataFrameLike] | None = None,
        keep_signals: list[str | tuple[str, list[int]]] | None = None,
        keep_datasets: list[str | tuple[str, list[int]]] | None = None,
    ) -> tuple[
        dict[str, Any], list[tuple[str, list[int], pa.Table]], list[dict[str, str]]
    ]:
        """
        Evaluate supported transforms in an input Vega specification.

        Produces a new specification with small pre-transformed datasets (under 100
        rows) included inline and larger inline datasets (100 rows or more) extracted
        into pyarrow tables.

        Args:
            spec: A Vega specification dict or JSON string.
            local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
                Defaults to the value of vf.get_local_tz(), which defaults to the system
                timezone if one can be determined.
            default_input_tz: Name of timezone (e.g. 'America/New_York') that naive
                datetime strings should be interpreted in. Defaults to `local_tz`.
            preserve_interactivity: If True (default) then the interactive behavior of
                the chart will pre preserved. This requires that all the data that
                participates in interactions be included in the resulting spec rather
                than being pre-transformed. If False, then all possible data
                transformations are applied even if they break the original interactive
                behavior of the chart.
            extract_threshold: Datasets with length below extract_threshold will be
                inlined.
            extracted_format: The format for the extracted datasets. Options are:
                - "pyarrow": pyarrow.Table
                - "arrow-ipc": bytes in arrow IPC format
                - "arrow-ipc-base64": base64 encoded arrow IPC format
            inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
                Tables. Inline datasets may be referenced by the input specification
                using the following url syntax 'vegafusion+dataset://{dataset_name}' or
                'table://{dataset_name}'.
            keep_signals: Signals from the input spec that must be included in the
                pre-transformed spec. A list with elements that are either:
                - The name of a top-level signal as a string
                - A two-element tuple where the first element is the name of a signal as
                  a string and the second element is the nested scope of the dataset as
                  a list of integers
            keep_datasets: Datasets from the input spec that must be included in the
                pre-transformed spec. A list with elements that are either:
                - The name of a top-level dataset as a string
                - A two-element tuple where the first element is the name of a dataset
                  as a string and the second element is the nested scope of the dataset
                  as a list of integers

        Returns:
            A tuple containing three elements:
            1. A dict containing the JSON representation of the pre-transformed Vega
               specification without pre-transformed datasets included inline
            2. Extracted datasets as a list of three element tuples:
               - dataset name
               - dataset scope
               - pyarrow Table
            3. A list of warnings as dictionaries. Each warning dict has a 'type' key
               indicating the warning type, and a 'message' key containing a description
               of the warning. Potential warning types include:
               - 'Planner': Planner warning
        """
        if self._grpc_channel:
            raise ValueError("pre_transform_spec not yet supported over gRPC")
        else:
            local_tz = local_tz or get_local_tz()

            inline_arrow_dataset = self._import_or_register_inline_datasets(
                inline_datasets
            )
            try:
                new_spec, datasets, warnings = (
                    self.embedded_runtime.pre_transform_extract(
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
                )
            finally:
                # Clean up temporary tables
                if self._connection is not None:
                    self._connection.unregister_temporary_tables()

            return new_spec, datasets, warnings

    def patch_pre_transformed_spec(
        self,
        spec1: dict[str, Any] | str,
        pre_transformed_spec1: dict[str, Any] | str,
        spec2: dict[str, Any] | str,
    ) -> dict[str, Any] | None:
        """
        Attempt to patch a Vega spec returned by the pre_transform_spec method.

        This method tries to patch a Vega spec without rerunning the pre_transform_spec
        logic. When possible, this can be significantly faster than rerunning the
        pre_transform_spec method.

        Args:
            spec1: The input Vega spec to a prior call to pre_transform_spec.
            pre_transformed_spec1: The prior result of passing spec1 to
                pre_transform_spec.
            spec2: A Vega spec that is assumed to be a small delta compared to spec1.

        Returns:
            If the delta between spec1 and spec2 is in the portions of spec1 that were
            not modified by pre_transform_spec, then this delta can be applied cleanly
            to pre_transform_spec1 and the result is returned. If the delta cannot be
            applied cleanly, None is returned and spec2 should be passed through the
            pre_transform_spec method.
        """
        if self._grpc_channel:
            raise ValueError("patch_pre_transformed_spec not yet supported over gRPC")
        else:
            pre_transformed_spec2 = self.embedded_runtime.patch_pre_transformed_spec(
                spec1, pre_transformed_spec1, spec2
            )
            return cast(dict[str, Any], pre_transformed_spec2)

    @property
    def worker_threads(self) -> int:
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
        if self._embedded_runtime:
            return self._embedded_runtime.total_memory()
        else:
            return None

    @property
    def _protected_memory(self) -> int | None:
        if self._embedded_runtime:
            return self._embedded_runtime.protected_memory()
        else:
            return None

    @property
    def _probationary_memory(self) -> int | None:
        if self._embedded_runtime:
            return self._embedded_runtime.probationary_memory()
        else:
            return None

    @property
    def size(self) -> int | None:
        if self._embedded_runtime:
            return self._embedded_runtime.size()
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
        if self._embedded_runtime is not None:
            self._embedded_runtime.clear_cache()
            self._embedded_runtime = None

    def __repr__(self) -> str:
        if self._grpc_channel:
            return f"VegaFusionRuntime(channel={self._grpc_channel})"
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


runtime = VegaFusionRuntime(64, psutil.virtual_memory().total // 2, psutil.cpu_count())
