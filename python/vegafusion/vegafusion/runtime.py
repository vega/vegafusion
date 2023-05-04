# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.
import json

import pandas as pd
import psutil
import pyarrow as pa
from typing import Union
from .connection import SqlConnection
from .transformer import import_pyarrow_interchange, to_arrow_table

try:
    from duckdb import DuckDBPyConnection
except ImportError:
    DuckDBPyConnection = None

try:
    import polars as pl
except ImportError:
    pl = None


def _all_datasets_have_type(inline_datasets, types):
    if not inline_datasets:
        # If there are no inline datasets, return false
        # (we want the default pandas behavior in this case)
        return False
    else:
        for dataset in inline_datasets.values():
            if not isinstance(dataset, types):
                return False
        return True


class VegaFusionRuntime:
    def __init__(self, cache_capacity, memory_limit, worker_threads, connection=None):
        self._embedded_runtime = None
        self._grpc_channel = None
        self._grpc_query = None
        self._cache_capacity = cache_capacity
        self._memory_limit = memory_limit
        self._worker_threads = worker_threads
        self._connection = connection

    @property
    def embedded_runtime(self):
        if self._embedded_runtime is None:
            # Try to initialize an embedded runtime
            from vegafusion_embed import PyVegaFusionRuntime
            self._embedded_runtime = PyVegaFusionRuntime(
                self.cache_capacity, self.memory_limit, self.worker_threads, connection=self._connection
            )
        return self._embedded_runtime

    def set_connection(self, connection: Union[str, SqlConnection, DuckDBPyConnection] = "datafusion"):
        """
        Sets the connection to use to evaluate Vega data transformations.

        Named tables returned by the connection's `tables` method may be referenced in Vega/Altair
        chart specifications using special dataset URLs. For example, if the connection's `tables`
        method returns a dictionary that includes "tableA" as a key, then this table may be
        referenced in a chart specification using the URL "table://tableA" or
        "vegafusion+dataset://tableA".

        :param connection: One of:
          - An instance of vegafusion.connection.SqlConnection
          - An instance of a duckdb connection
          - A string, one of:
                - "datafusion" (default)
                - "duckdb"
        """
        if isinstance(connection, str):
            if connection == "datafusion":
                # Connection of None uses DataFusion
                connection = None
            elif connection == "duckdb":
                from vegafusion.connection.duckdb import DuckDbConnection
                connection = DuckDbConnection()
            else:
                raise ValueError(f"Unsupported connection name: {connection}")
        elif DuckDBPyConnection is not None and isinstance(connection, DuckDBPyConnection):
            from vegafusion.connection.duckdb import DuckDbConnection
            connection = DuckDbConnection(connection)
        elif not isinstance(connection, SqlConnection):
            raise ValueError(
                "connection argument must be a string or an instance of SqlConnection\n"
                f"Received value of type {type(connection).__name__}: {connection}"
            )

        self._connection = connection
        self.reset()

    def grpc_connect(self, channel):
        """
        Connect to a VegaFusion server over gRPC using the provided gRPC channel

        :param channel: grpc.Channel instance configured with the address of a running VegaFusion server
        """
        # TODO: check channel type
        self._grpc_channel = channel

    @property
    def using_grpc(self):
        return self._grpc_channel is not None

    @property
    def grpc_query(self):
        if self._grpc_channel is None:
            raise ValueError(
                "No grpc channel registered. Use runtime.grpc_connect to provide a grpc channel"
            )

        if self._grpc_query is None:
            self._grpc_query = self._grpc_channel.unary_unary(
                '/services.VegaFusionRuntime/TaskGraphQuery',
            )
        return self._grpc_query

    def process_request_bytes(self, request):
        if self._grpc_channel:
            return self.grpc_query(request)
        else:
            # No grpc channel, get or initialize an embedded runtime
            return self.embedded_runtime.process_request_bytes(request)

    def _arrowify_or_register_inline_datasets(self, inline_datasets=None):
        from .transformer import to_arrow_ipc_bytes, arrow_table_to_ipc_bytes
        inline_datasets = inline_datasets or dict()
        inline_arrow_datasets = dict()
        for name, value in inline_datasets.items():
            if isinstance(value, pa.Table):
                if self._connection is not None:
                    try:
                        # Try registering Arrow Table if supported
                        self._connection.register_arrow(name, value, temporary=True)
                        continue
                    except ValueError:
                        pass

                inline_arrow_datasets[name] = value
            elif isinstance(value, pd.DataFrame):
                if self._connection is not None:
                    try:
                        # Try registering DataFrame if supported
                        self._connection.register_pandas(name, value, temporary=True)
                        continue
                    except ValueError:
                        pass

                inline_arrow_datasets[name] = to_arrow_table(value)
            elif hasattr(value, "__dataframe__"):
                pi = import_pyarrow_interchange()
                value = pi.from_dataframe(value)
                if self._connection is not None:
                    try:
                        # Try registering Arrow Table if supported
                        self._connection.register_arrow(name, value, temporary=True)
                        continue
                    except ValueError:
                        pass

                inline_arrow_datasets[name] = value
            else:
                raise ValueError(f"Unsupported DataFrame type: {type(value)}")

        return inline_arrow_datasets

    def pre_transform_spec(
        self,
        spec,
        local_tz,
        default_input_tz=None,
        row_limit=None,
        preserve_interactivity=True,
        inline_datasets=None
    ):
        """
        Evaluate supported transforms in an input Vega specification and produce a new
        specification with pre-transformed datasets included inline.

        :param spec: A Vega specification dict or JSON string
        :param local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
            This can be computed for the local system using the tzlocal package and the
            tzlocal.get_localzone_name() function.
        :param default_input_tz: Name of timezone (e.g. 'America/New_York') that naive datetime
            strings should be interpreted in. Defaults to `local_tz`.
        :param row_limit: Maximum number of dataset rows to include in the returned
            specification. If exceeded, datasets will be truncated to this number of rows
            and a RowLimitExceeded warning will be included in the resulting warnings list
        :param preserve_interactivity: If True (default) then the interactive behavior of
            the chart will pre preserved. This requires that all the data that participates
            in interactions be included in the resulting spec rather than being pre-transformed.
            If False, then all possible data transformations are applied even if they break
            the original interactive behavior of the chart.
        :param inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
            Tables. Inline datasets may be referenced by the input specification using
            the following url syntax 'vegafusion+dataset://{dataset_name}' or
            'table://{dataset_name}'.
        :return:
            Two-element tuple:
                0. A string containing the JSON representation of a Vega specification
                   with pre-transformed datasets included inline
                1. A list of warnings as dictionaries. Each warning dict has a 'type'
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
            inline_arrow_dataset = self._arrowify_or_register_inline_datasets(inline_datasets)
            try:
                new_spec, warnings = self.embedded_runtime.pre_transform_spec(
                    spec,
                    local_tz=local_tz,
                    default_input_tz=default_input_tz,
                    row_limit=row_limit,
                    preserve_interactivity=preserve_interactivity,
                    inline_datasets=inline_arrow_dataset
                )
            finally:
                # Clean up temporary tables
                if self._connection is not None:
                    self._connection.unregister_temporary_tables()

            return new_spec, warnings

    def pre_transform_datasets(self, spec, datasets, local_tz, default_input_tz=None, row_limit=None, inline_datasets=None):
        """
        Extract the fully evaluated form of the requested datasets from a Vega specification
        as pandas DataFrames.

        :param spec: A Vega specification dict or JSON string
        :param datasets: A list with elements that are either:
          - The name of a top-level dataset as a string
          - A two-element tuple where the first element is the name of a dataset as a string
            and the second element is the nested scope of the dataset as a list of integers
        :param local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
            This can be computed for the local system using the tzlocal package and the
            tzlocal.get_localzone_name() function.
        :param default_input_tz: Name of timezone (e.g. 'America/New_York') that naive datetime
            strings should be interpreted in. Defaults to `local_tz`.
        :param row_limit: Maximum number of dataset rows to include in the returned
            datasets. If exceeded, datasets will be truncated to this number of rows
            and a RowLimitExceeded warning will be included in the resulting warnings list
        :param inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
            Tables. Inline datasets may be referenced by the input specification using
            the following url syntax 'vegafusion+dataset://{dataset_name}' or
            'table://{dataset_name}'..
        :return:
            Two-element tuple:
                0. List of pandas DataFrames corresponding to the input datasets list
                1. A list of warnings as dictionaries. Each warning dict has a 'type'
                   key indicating the warning type, and a 'message' key containing
                   a description of the warning.
        """
        if self._grpc_channel:
            raise ValueError("pre_transform_datasets not yet supported over gRPC")
        else:

            # Build input variables
            pre_tx_vars = []
            err_msg = "Elements of variables argument must be strings are two-element tuples"
            for var in datasets:
                if isinstance(var, str):
                    pre_tx_vars.append((var, []))
                elif isinstance(var, (list, tuple)):
                    if len(var) == 2:
                        pre_tx_vars.append(tuple(var))
                    else:
                        raise ValueError(err_msg)
                else:
                    raise ValueError(err_msg)

            # Serialize inline datasets
            inline_arrow_dataset = self._arrowify_or_register_inline_datasets(inline_datasets)
            try:
                values, warnings = self.embedded_runtime.pre_transform_datasets(
                    spec,
                    pre_tx_vars,
                    local_tz=local_tz,
                    default_input_tz=default_input_tz,
                    row_limit=row_limit,
                    inline_datasets=inline_arrow_dataset
                )
            finally:
                # Clean up registered tables (both inline and internal temporary tables)
                if self._connection is not None:
                    self._connection.unregister_temporary_tables()

            if pl is not None and _all_datasets_have_type(inline_datasets, (pl.DataFrame, pl.LazyFrame)):
                # Deserialize values to Polars tables
                datasets = [pl.from_arrow(value) for value in values]

                # Localize datetime columns to UTC
                processed_datasets = []
                for df in datasets:
                    for name, dtype in zip(df.columns, df.dtypes):
                        if dtype == pl.Datetime:
                            df = df.with_columns(df[name].dt.replace_time_zone("UTC").dt.convert_time_zone(local_tz))
                    processed_datasets.append(df)

                return processed_datasets, warnings
            elif _all_datasets_have_type(inline_datasets, pa.Table):
                return values, warnings
            else:
                # Deserialize values to pandas DataFrames
                datasets = [value.to_pandas() for value in values]

                # Localize datetime columns to UTC
                for df in datasets:
                    for name, dtype in df.dtypes.items():
                        if dtype.kind == "M":
                            df[name] = df[name].dt.tz_localize("UTC").dt.tz_convert(local_tz)

                return datasets, warnings

    def pre_transform_extract(
            self,
            spec,
            local_tz,
            default_input_tz=None,
            preserve_interactivity=True,
            inline_datasets=None
    ):
        """
        Evaluate supported transforms in an input Vega specification and produce a new
        specification with small pre-transformed datasets (under 100 rows) included inline
        and larger inline datasets (100 rows or more) are extracted into pyarrow tables.

        :param spec: A Vega specification dict or JSON string
        :param local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
            This can be computed for the local system using the tzlocal package and the
            tzlocal.get_localzone_name() function.
        :param default_input_tz: Name of timezone (e.g. 'America/New_York') that naive datetime
            strings should be interpreted in. Defaults to `local_tz`.
        :param preserve_interactivity: If True (default) then the interactive behavior of
            the chart will pre preserved. This requires that all the data that participates
            in interactions be included in the resulting spec rather than being pre-transformed.
            If False, then all possible data transformations are applied even if they break
            the original interactive behavior of the chart.
        :param inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
            Tables. Inline datasets may be referenced by the input specification using
            the following url syntax 'vegafusion+dataset://{dataset_name}' or
            'table://{dataset_name}'.
        :return:
            Three-element tuple:
                0. A dict containing the JSON representation of the pre-transformed Vega
                   specification without pre-transformed datasets included inline
                1. Extracted datasets as a list of three element tuples
                    0. dataset name
                    1. dataset scope
                    2. pyarrow Table
                2. A list of warnings as dictionaries. Each warning dict has a 'type'
                   key indicating the warning type, and a 'message' key containing
                   a description of the warning. Potential warning types include:
                    'Planner': Planner warning
        """
        if self._grpc_channel:
            raise ValueError("pre_transform_spec not yet supported over gRPC")
        else:
            inline_arrow_dataset = self._arrowify_or_register_inline_datasets(inline_datasets)
            try:
                new_spec, datasets, warnings = self.embedded_runtime.pre_transform_extract(
                    spec,
                    local_tz=local_tz,
                    default_input_tz=default_input_tz,
                    preserve_interactivity=preserve_interactivity,
                    inline_datasets=inline_arrow_dataset
                )
            finally:
                # Clean up temporary tables
                if self._connection is not None:
                    self._connection.unregister_temporary_tables()

            return new_spec, datasets, warnings

    def patch_pre_transformed_spec(self, spec1, pre_transformed_spec1, spec2):
        """
        Attempt to patch a Vega spec was returned by the pre_transform_spec method without
        rerunning the pre_transform_spec logic. When possible, this can be significantly
        faster than rerunning the pre_transform_spec method.

        :param spec1: The input Vega spec to a prior call to pre_transform_spec
        :param pre_transformed_spec1: The prior result of passing spec1 to pre_transform_spec
        :param spec2: A Vega spec that is assumed to be a small delta compared to spec1

        :return: dict or None
            If the delta between spec1 and spec2 is in the portions of spec1 that were not
            modified by pre_transform_spec, then this delta can be applied cleanly to
            pre_transform_spec1 and the result is returned. If the delta cannot be
            applied cleanly, None is returned and spec2 should be passed through the
            pre_transform_spec method.
        """
        if self._grpc_channel:
            raise ValueError("patch_pre_transformed_spec not yet supported over gRPC")
        else:
            pre_transformed_spec2 = self.embedded_runtime.patch_pre_transformed_spec(
                spec1, pre_transformed_spec1, spec2
            )
            return pre_transformed_spec2

    @property
    def worker_threads(self):
        return self._worker_threads

    @worker_threads.setter
    def worker_threads(self, value):
        """
        Restart the runtime with the specified number of worker threads

        :param threads: Number of threads for the new runtime
        """
        if value != self._worker_threads:
            self._worker_threads = value
            self.reset()

    @property
    def total_memory(self):
        if self._embedded_runtime:
            return self._embedded_runtime.total_memory()
        else:
            return None

    @property
    def _protected_memory(self):
        if self._embedded_runtime:
            return self._embedded_runtime.protected_memory()
        else:
            return None

    @property
    def _probationary_memory(self):
        if self._embedded_runtime:
            return self._embedded_runtime.probationary_memory()
        else:
            return None

    @property
    def size(self):
        if self._embedded_runtime:
            return self._embedded_runtime.size()
        else:
            return None

    @property
    def memory_limit(self):
        return self._memory_limit

    @memory_limit.setter
    def memory_limit(self, value):
        """
        Restart the runtime with the specified memory limit

        :param threads: Max approximate memory usage of cache
        """
        if value != self._memory_limit:
            self._memory_limit = value
            self.reset()

    @property
    def cache_capacity(self):
        return self._cache_capacity

    @cache_capacity.setter
    def cache_capacity(self, value):
        """
        Restart the runtime with the specified cache capacity

        :param threads: Max task graph values to cache
        """
        if value != self._cache_capacity:
            self._cache_capacity = value
            self.reset()

    def reset(self):
        if self._embedded_runtime is not None:
            self._embedded_runtime.clear_cache()
            self._embedded_runtime = None

    def __repr__(self):
        if self._grpc_channel:
            return f"VegaFusionRuntime(channel={self._grpc_channel})"
        else:
            return (
                f"VegaFusionRuntime("
                f"cache_capacity={self.cache_capacity}, worker_threads={self.worker_threads}"
                f")"
            )


runtime = VegaFusionRuntime(64, psutil.virtual_memory().total // 2, psutil.cpu_count())
