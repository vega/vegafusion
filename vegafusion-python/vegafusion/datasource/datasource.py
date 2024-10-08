from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


class Datasource(ABC):
    @abstractmethod
    def schema(self) -> pa.Schema:
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, columns: Iterable[str]) -> pa.Table:
        raise NotImplementedError()
