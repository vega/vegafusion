from abc import ABC, abstractmethod
import pyarrow as pa
from vegafusion.proto.datafusion_pb2 import LogicalExprNode

from typing import Optional, List, Literal, Union, Any


class DataFrameOperationNotSupportedError(RuntimeError):
    """
    Exception raised when a DataFrame transform method is not
    supported
    """
    pass


class DataFrameDataset(ABC):
    """
    Python interface for VegaFusion DataFrame
    """

    @abstractmethod
    def schema(self) -> pa.Schema:
        """DataFrame's pyarrow schema"""
        raise NotImplementedError()

    @abstractmethod
    def collect(self) -> pa.Table:
        """Return DataFrame's value as a pyarrow Table"""
        raise NotImplementedError()

    def fallback(self) -> bool:
        """
        Whether VegaFusion should fall back to the built-in DataFusion connection
        when DataFrame operations are encountered that are not supported

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

    def sort(
        self, exprs: List[LogicalExprNode], limit: Optional[int]
    ) -> "DataFrameDataset":
        """
        Sort and optionally limit dataset

        :param exprs: Sort expressions
        :param limit: Max number of rows to return
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def select(self, exprs: List[LogicalExprNode]) -> "DataFrameDataset":
        """
        Select columns from Dataset. Selection expressions may include column names,
        column expressions, or window expressions

        :param exprs: Selection expressions
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def aggregate(
        self, group_exprs: List[LogicalExprNode], agg_exprs: List[LogicalExprNode]
    ) -> "DataFrameDataset":
        """
        Perform dataset aggregation. Resulting dataset includes grouping
        columns and aggregate expressions

        :param group_exprs: Expressions to group by
        :param agg_exprs: Aggregate expressions
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def joinaggregate(
        self, group_exprs: List[LogicalExprNode], agg_exprs: List[LogicalExprNode]
    ) -> "DataFrameDataset":
        """
        Perform joinaggregate dataset operation.

        See: https://vega.github.io/vega/docs/transforms/joinaggregate/

        :param group_exprs: Expressions to group by
        :param agg_exprs: Aggregate expressions
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def filter(self, predicate: LogicalExprNode) -> "DataFrameDataset":
        """
        Filter dataset by predicate expression

        :param predicate: Predicate expression
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def limit(self, limit: int) -> "DataFrameDataset":
        """
        Limit dataset to max number of rows

        :param limit: Max number of rows
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def fold(
        self,
        fields: List[str],
        value_col: str,
        key_col: str,
        order_field: Optional[str],
    ) -> "DataFrameDataset":
        """
        See: https://vega.github.io/vega/docs/transforms/fold/

        :param fields: List of fields to fold
        :param value_col: Name of output value column
        :param key_col: Name of output key column
        :param order_field: Name of input ordering column or
            None if input ordering is not defined
        :return: DataFrameDataset
        """
        raise DataFrameOperationNotSupportedError()

    def stack(
        self,
        field: str,
        orderby: List[LogicalExprNode],
        groupby: List[str],
        start_field: str,
        stop_field: str,
        mode: Literal["zero", "center", "normalize"],
    ) -> "DataFrameDataset":
        """
        Computes a layout of stacking groups of values

        See: https://vega.github.io/vega/docs/transforms/stack/

        :param field: Column that determines stack height
        :param orderby: Criteria for sorting values within each stack
        :param groupby: List of columns by which to partition data into separate stacks
        :param start_field: Name of output stack start column
        :param stop_field: Name of output stack stop column
        :param mode: Stack mode. One of: "zero", "center", "normalize"
        :return:
        """
        raise DataFrameOperationNotSupportedError()

    def impute(
        self,
        field: str,
        value: Union[str, int, float],
        key: str,
        groupby: List[str],
        order_field: Optional[str],
    ) -> "DataFrameDataset":
        """
        Performs imputation of missing data objects.

        See: https://vega.github.io/vega/docs/transforms/impute/

        :param field: Column for which missing values should be imputed
        :param value: Value to impute with
        :param key: Key column that uniquely identifies data objects within a group.
            Missing key values (those occurring in the data but not in the current group)
            will be imputed.
        :param groupby: Optional list of columns to group by
        :param order_field: Optional input ordering field. If not provided, input is
            assumed to have arbitrary ordering
        :return:
        """
        raise DataFrameOperationNotSupportedError()

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True, **kwargs
    ) -> Any:
        """DataFrame interchange protocol support"""
        table = self.collect()
        return table.__dataframe__(nan_as_null=nan_as_null, allow_copy=allow_copy)
