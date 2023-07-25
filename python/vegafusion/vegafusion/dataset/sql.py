from abc import ABC, abstractmethod
import pyarrow as pa
from typing import Any


class SqlDataset(ABC):
    """
    Python interface for VegaFusion Sql Dataset
    """

    @classmethod
    def dialect(cls) -> str:
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

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True, **kwargs
    ) -> Any:
        """DataFrame interchange protocol support"""
        raise NotImplementedError()
