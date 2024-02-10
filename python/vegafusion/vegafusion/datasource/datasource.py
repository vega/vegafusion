from abc import ABC, abstractmethod
from typing import Iterable
import pyarrow as pa


class Datasource(ABC):
    @abstractmethod
    def schema(self) -> pa.Schema:
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, columns: Iterable[str]) -> pa.Table:
        raise NotImplementedError()
