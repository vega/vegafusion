import pyarrow as pa
from functools import cached_property
from typing import Sequence, Optional
from .sql import SqlDataset


class SqlDatasetDataFrame:
    """An implementation of the dataframe interchange protocol.
    Based on https://github.com/ibis-project/ibis/pull/6733

    This is a thin shim around the pyarrow implementation to allow for:
     - Accessing a few of the metadata queries without executing the expression.
     - Caching the execution on the dataframe object to avoid re-execution if
       multiple methods are accessed.

     The dataframe interchange protocol may be found here:
     https://data-apis.org/dataframe-protocol/latest/API.html
    """

    def __init__(
        self,
        dataset: SqlDataset,
        nan_as_null: bool = False,
        allow_copy: bool = True,
        pyarrow_table: Optional[pa.Table] = None,
    ):
        self._dataset = dataset
        self._nan_as_null = nan_as_null
        self._allow_copy = allow_copy
        self._pyarrow_table = pyarrow_table

    @cached_property
    def _pyarrow_df(self):
        """Returns the pyarrow implementation of the __dataframe__ protocol.
        If the backing Dataset hasn't been executed yet, this will result
        in executing and caching the result."""
        if self._pyarrow_table is None:
            self._pyarrow_table = self._dataset.fetch_query(
                f"select * from {self._dataset.table_name()}",
                schema=self._dataset.table_schema(),
            )
        return self._pyarrow_table.__dataframe__(
            nan_as_null=self._nan_as_null,
            allow_copy=self._allow_copy,
        )

    @cached_property
    def _empty_pyarrow_df(self):
        """A pyarrow implementation of the __dataframe__ protocol for an
        empty table with the same schema as this table.
        Used for returning dtype information without executing the backing ibis
        expression.
        """
        schema = self._schema
        return schema.empty_table().__dataframe__()

    @property
    def _schema(self) -> pa.Schema:
        return self._dataset.table_schema()

    def _get_dtype(self, name):
        """Get the dtype info for a column named `name`."""
        return self._empty_pyarrow_df.get_column_by_name(name).dtype

    # These methods may all be handled without executing the query
    def num_columns(self):
        return len(self._schema.names)

    def column_names(self):
        return self._schema.names

    def get_column(self, i: int) -> "DatasetColumn":
        name = self._schema.names[i]
        return self.get_column_by_name(name)

    def get_column_by_name(self, name: str) -> "DatasetColumn":
        return DatasetColumn(self, name)

    def get_columns(self):
        return [DatasetColumn(self, name) for name in self._schema.names]

    def select_columns(self, indices: Sequence[int]) -> "SqlDatasetDataFrame":
        names = [self._schema.names[i] for i in indices]
        return self.select_columns_by_name(names)

    def select_columns_by_name(self, names: Sequence[str]) -> "SqlDatasetDataFrame":
        return self._pyarrow_df.select_columns_by_name(names)

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> "SqlDatasetDataFrame":
        return SqlDatasetDataFrame(
            self._dataset,
            nan_as_null=nan_as_null,
            allow_copy=allow_copy,
            pyarrow_table=self._pyarrow_table,
        )

    # These methods require executing the query
    @property
    def metadata(self):
        return self._pyarrow_df.metadata

    def num_rows(self) -> Optional[int]:
        return self._pyarrow_df.num_rows()

    def num_chunks(self) -> int:
        return self._pyarrow_df.num_chunks()

    def get_chunks(self, n_chunks: Optional[int] = None):
        return self._pyarrow_df.get_chunks(n_chunks=n_chunks)


class DatasetColumn:
    def __init__(self, df: SqlDatasetDataFrame, name: str):
        self._df = df
        self._name = name

    @cached_property
    def _pyarrow_col(self):
        """Returns the pyarrow implementation of the __dataframe__ protocol's
        Column type.
        If the backing SqlDataset hasn't been executed yet, this will result
        in executing and caching the result."""
        return self._df._pyarrow_df.get_column_by_name(self._name)

    # These methods may all be handled without executing the query
    @property
    def dtype(self):
        return self._df._get_dtype(self._name)

    # These methods require executing the query
    def size(self):
        return self._pyarrow_col.size()

    @property
    def describe_categorical(self):
        return self._pyarrow_col.describe_categorical

    @property
    def offset(self):
        return self._pyarrow_col.offset

    @property
    def describe_null(self):
        return self._pyarrow_col.describe_null

    @property
    def null_count(self):
        return self._pyarrow_col.null_count

    @property
    def metadata(self):
        return self._pyarrow_col.metadata

    def num_chunks(self) -> int:
        return self._pyarrow_col.num_chunks()

    def get_chunks(self, n_chunks: Optional[int] = None):
        return self._pyarrow_col.get_chunks(n_chunks=n_chunks)

    def get_buffers(self):
        return self._pyarrow_col.get_buffers()
