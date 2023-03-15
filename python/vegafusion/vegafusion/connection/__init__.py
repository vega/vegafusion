from typing import Dict, Optional

import pandas as pd
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
    def tables(self) -> Dict[str, pa.Schema]:
        """
        Returns the names and schema for the tables that are provided by the connection.
        These are the tables that may be referenced by SQL queries passed to the
        fetch_query method

        :return: dict from table name to pa.Schema
        """
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

    def register_pandas(self, name: str, df: pd.DataFrame, temporary: bool = False):
        """
        Register the provided pandas DataFrame as a table with the provided name

        :param name: Table name
        :param df: pandas DataFrame
        :param temporary: Whether table is considered temporary,
            and should be removed by unregister_temporary_tables
        """
        raise ValueError("Connection does not support registration of pandas datasets")

    def register_arrow(self, name: str, table: pa.Table, temporary: bool = False):
        """
        Register the provided pyarrow Table as a table with the provided name
        :param name: Table name
        :param table: pyarrow Table
        :param temporary: Whether table is considered temporary,
            and should be removed by unregister_temporary_tables
        """
        raise ValueError("Connection does not support registration of arrow datasets")

    def register_json(self, name: str, path: str, temporary: bool = False):
        """
        Register the JSON file at the provided path as a table with the provided name
        :param name: Table name
        :param path: Path to JSON file
        :param temporary: Whether table is considered temporary,
            and should be removed by unregister_temporary_tables
        """
        raise ValueError("Connection does not support registration of json datasets")

    def register_csv(self, name: str, path: str, options: CsvReadOptions, temporary: bool = False):
        """
        Register the CSV file at the provided path as a table with the provided name
        :param name: Table name
        :param path: Path to CSV file
        :param options: CSV options
        :param temporary: Whether table is considered temporary,
            and should be removed by unregister_temporary_tables
        """
        raise ValueError("Connection does not support registration of csv datasets")

    def register_parquet(self, name: str, path: str, temporary: bool = False):
        """
        Register the Parquet file at the provided path as a table with the provided name
        :param name: Table name
        :param path: Path to parquet file
        :param temporary: Whether table is considered temporary,
            and should be removed by unregister_temporary_tables
        """
        raise ValueError("Connection does not support registration of parquet datasets")

    def unregister(self, name: str):
        """
        Unregister a table (temporary or otherwise) by name
        :param name: Table name
        """
        raise ValueError("Connection does not support unregistration")

    def unregister_temporary_tables(self):
        """
        Unregister all dynamically registered tables
        """
        raise ValueError("Connection does not support unregistering temporary tables")
