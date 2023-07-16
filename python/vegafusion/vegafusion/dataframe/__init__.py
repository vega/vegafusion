from abc import ABC, abstractmethod
import pyarrow as pa
from vegafusion.proto.datafusion_pb2 import LogicalExprNode, SortExprNode

from typing import Optional, List, Literal


class DataFrame(ABC):
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

    @abstractmethod
    def sort(self, exprs: List[SortExprNode], limit: Optional[int]) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def select(self, exprs: List[LogicalExprNode]) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def aggregate(self, group_exprs: List[LogicalExprNode], agg_exprs: List[LogicalExprNode]) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def joinaggregate(self, group_exprs: List[LogicalExprNode], agg_exprs: List[LogicalExprNode]) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def filter(self, predicate: LogicalExprNode) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def limit(self, limit: int) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def fold(
        self,
        fields: List[str],
        value_col: str,
        key_col: str,
        order_field: Optional[str],
    ) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def stack(
        self,
        field: str,
        orderby: List[SortExprNode],
        groupby: List[str],
        start_field: str,
        stop_field: str,
        mode: Literal["zero", "center", "normalize"],
    ) -> "DataFrame":
        raise NotImplementedError()

    @abstractmethod
    def impute(
        self,
        field: str,
        value: str | int | float,
        key: str,
        groupby: List[str],
        order_field: Optional[str],
    ) -> "DataFrame":
        raise NotImplementedError()
