# VegaFusion
# Copyright (C) 2022, Jon Mease
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import psutil


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
