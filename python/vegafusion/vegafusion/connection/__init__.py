from typing import Dict, Optional
import pyarrow as pa

from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class CsvReadOptions:
    """
    CSV Read configuration options
    """
    has_header: bool
    delimeter: str
    file_extension: str
    schema: Optional[pa.Schema]


class SqlConnection(ABC):
    """
    Python interface for SQL connections
    """
    @classmethod
    def dialect(cls) -> str:
        raise NotImplementedError()

    @abstractmethod
    def tables(self) -> Dict[str, pa.Schema]:
        raise NotImplementedError()

    @abstractmethod
    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        raise NotImplementedError()

    def register_arrow(self, name: str, table: pa.Table):
        raise ValueError("Connection does not support registration of arrow datasets")

    def register_json(self, name: str, path: str):
        raise ValueError("Connection does not support registration of json datasets")

    def register_csv(self, name: str, path: str, options: CsvReadOptions):
        raise ValueError("Connection does not support registration of csv datasets")

    def register_parquet(self, name: str, path: str):
        raise ValueError("Connection does not support registration of parquet datasets")
