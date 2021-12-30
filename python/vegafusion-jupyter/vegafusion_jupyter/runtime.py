from vegafusion import PyTaskGraphRuntime
import multiprocessing


class VegaFusionRuntime:
    def __init__(self, cache_capacity, worker_threads):
        self._runtime = None
        self._cache_capacity = cache_capacity
        self._worker_threads = worker_threads

    @property
    def runtime(self):
        if self._runtime is None:
            self._runtime = PyTaskGraphRuntime(self.cache_capacity, self.worker_threads)
        return self._runtime

    def process_request_bytes(self, request):
        return self.runtime.process_request_bytes(request)

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
        if self._runtime is not None:
            self._runtime.clear_cache()
            self._runtime = None

    def __repr__(self):
        return f"VegaFusionRuntime(" \
               f"cache_capacity={self.cache_capacity}, worker_threads={self.worker_threads}" \
               f")"


runtime = VegaFusionRuntime(16, multiprocessing.cpu_count())
