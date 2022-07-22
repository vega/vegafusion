# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is distributed under multiple licenses.
# Please consult the license documentation provided alongside
# this program the details of the active license.
import json
import psutil
import pyarrow as pa


class VegaFusionRuntime:
    def __init__(self, cache_capacity, memory_limit, worker_threads):
        self._embedded_runtime = None
        self._grpc_channel = None
        self._grpc_query = None
        self._cache_capacity = cache_capacity
        self._memory_limit = memory_limit
        self._worker_threads = worker_threads

    @property
    def embedded_runtime(self):
        if self._embedded_runtime is None:
            # Try to initialize an embedded runtime
            from vegafusion_embed import PyTaskGraphRuntime

            self._embedded_runtime = PyTaskGraphRuntime(self.cache_capacity, self.memory_limit, self.worker_threads)
        return self._embedded_runtime

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

    @staticmethod
    def _serialize_inline_datasets(inline_datasets=None):
        from .transformer import to_arrow_ipc_bytes, arrow_table_to_ipc_bytes

        # Preprocess inline_dataset
        inline_datasets = inline_datasets or dict()
        inline_dataset_bytes = dict()
        for name, value in inline_datasets.items():
            if isinstance(value, pa.Table):
                table_bytes = arrow_table_to_ipc_bytes(value, stream=True)
            else:
                table_bytes = to_arrow_ipc_bytes(value, stream=True)
            inline_dataset_bytes[name] = table_bytes
        return inline_dataset_bytes

    def pre_transform_spec(self, spec, local_tz, default_input_tz=None, row_limit=None, inline_datasets=None):
        """
        Evaluate supported transforms in an input Vega specification and produce a new
        specification with pre-transformed datasets included inline.

        :param spec: A Vega specification
        :param local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
            This can be computed for the local system using the tzlocal package and the
            tzlocal.get_localzone_name() function.
        :param default_input_tz: Name of timezone (e.g. 'America/New_York') that naive datetime
            strings should be interpreted in. Defaults to `local_tz`.
        :param row_limit: Maximum number of dataset rows to include in the returned
            specification. If exceeded, datasets will be truncated to this number of rows
            and a RowLimitExceeded warning will be included in the resulting warnings list
        :param inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
            Tables. Inline datasets may be referenced by the input specification using
            the following url syntax 'vegafusion+dataset://{dataset_name}'.
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
            inline_dataset_bytes = self._serialize_inline_datasets(inline_datasets)
            new_spec, warnings = self.embedded_runtime.pre_transform_spec(
                spec,
                local_tz=local_tz,
                default_input_tz=default_input_tz,
                row_limit=row_limit,
                inline_datasets=inline_dataset_bytes
            )
            warnings = json.loads(warnings)
            return new_spec, warnings

    def pre_transform_datasets(self, spec, datasets, local_tz, default_input_tz=None, inline_datasets=None):
        """
        Extract the fully evaluated form of the requested datasets from a Vega specification
        as pandas DataFrames.

        :param spec: A Vega specification
        :param datasets: A list with elements that are either:
          - The name of a top-level dataset as a string
          - A two-element tuple where the first element is the name of a dataset as a string
            and the second element is the nested scope of the dataset as a list of integers
        :param local_tz: Name of timezone to be considered local. E.g. 'America/New_York'.
            This can be computed for the local system using the tzlocal package and the
            tzlocal.get_localzone_name() function.
        :param default_input_tz: Name of timezone (e.g. 'America/New_York') that naive datetime
            strings should be interpreted in. Defaults to `local_tz`.
        :param inline_datasets: A dict from dataset names to pandas DataFrames or pyarrow
            Tables. Inline datasets may be referenced by the input specification using
            the following url syntax 'vegafusion+dataset://{dataset_name}'.
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
            # Serialize inline datasets
            inline_dataset_bytes = self._serialize_inline_datasets(inline_datasets)

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

            values, warnings = self.embedded_runtime.pre_transform_datasets(
                spec,
                pre_tx_vars,
                local_tz=local_tz,
                default_input_tz=default_input_tz,
                inline_datasets=inline_dataset_bytes
            )

            # Deserialize values to Arrow tables
            datasets = [pa.ipc.deserialize_pandas(value) for value in values]

            warnings = json.loads(warnings)
            return datasets, warnings

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
