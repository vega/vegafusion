from abc import ABC, abstractmethod
import pyarrow as pa
from vegafusion.proto.datafusion_pb2 import LogicalExprNode, SortExprNode
from pyarrow.interchange.dataframe import _PyArrowDataFrame

from typing import Optional, List, Literal


class DataFrameOperationNotSupportedError(RuntimeError):
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

    def sort(
        self, exprs: List[SortExprNode], limit: Optional[int]
    ) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def select(self, exprs: List[LogicalExprNode]) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def aggregate(
        self, group_exprs: List[LogicalExprNode], agg_exprs: List[LogicalExprNode]
    ) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def joinaggregate(
        self, group_exprs: List[LogicalExprNode], agg_exprs: List[LogicalExprNode]
    ) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def filter(self, predicate: LogicalExprNode) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def limit(self, limit: int) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def fold(
        self,
        fields: List[str],
        value_col: str,
        key_col: str,
        order_field: Optional[str],
    ) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def stack(
        self,
        field: str,
        orderby: List[SortExprNode],
        groupby: List[str],
        start_field: str,
        stop_field: str,
        mode: Literal["zero", "center", "normalize"],
    ) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()

    def impute(
        self,
        field: str,
        value: str | int | float,
        key: str,
        groupby: List[str],
        order_field: Optional[str],
    ) -> "DataFrameDataset":
        raise DataFrameOperationNotSupportedError()
