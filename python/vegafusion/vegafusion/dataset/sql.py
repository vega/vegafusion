from abc import ABC, abstractmethod
import pyarrow as pa
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .dfi import SqlDatasetDataFrame


class SqlDataset(ABC):
    """
    Python interface for VegaFusion Sql Dataset
    """

    def dialect(self) -> str:
        """
        Returns SQL dialect accepted by the connection

        :return: One of:
         - "athena"
         - "bigquery"
         - "clickhouse"
         - "databricks"
         - "datafusion"
         - "duckdb"
         - "mysql"
         - "postgres"
         - "redshift"
         - "snowflake"
        """
        raise NotImplementedError()

    @abstractmethod
    def table_name(self) -> str:
        """Name of source table for use in queries"""
        raise NotImplementedError()

    @abstractmethod
    def table_schema(self) -> pa.Schema:
        """Schema of source table"""
        raise NotImplementedError()

    @abstractmethod
    def fetch_query(self, query: str, schema: pa.Schema) -> pa.Table:
        """
        Returns the result of evaluating the requested query. The resulting pa.Table
        should have a schema matching the provided schema

        :param query: SQL query string
        :param schema: expected pyarrow Schema of resulting pyarrow Table
        :return: pyarrow Table with query results
        """
        raise NotImplementedError()

    def fallback(self) -> bool:
        """
        Whether VegaFusion should fall back to the built-in DataFusion connection
        when SQL is encountered that is not supported by this connection's SQL dialect

        :return: bool
        """
        return True

    def main_thread(self) -> bool:
        """
        True if the dataset must be evaluated run on the main thread.
        This blocks multithreaded parallelization, but is sometimes required

        :return: bool
        """
        return True

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True, **kwargs
    ) -> "SqlDatasetDataFrame":
        """DataFrame interchange protocol support"""
        from .dfi import SqlDatasetDataFrame
        return SqlDatasetDataFrame(self, nan_as_null=nan_as_null, allow_copy=allow_copy)
