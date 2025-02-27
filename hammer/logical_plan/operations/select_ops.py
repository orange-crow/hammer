from typing import Any, Dict, List

from .build_ops import register_op
from .operation import OperationNode


@register_op()
class LocOp(OperationNode):
    pandas_name: str = "loc"

    def __init__(
        self,
        pandas_positional_args: List,
        pandas_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("loc", pandas_positional_args, pandas_keyword_args)
        self._select_cols = (
            [self.pandas_positional_args]
            if isinstance(self.pandas_positional_args, str)
            else self.pandas_positional_args
        )

    @property
    def is_data_method(self) -> bool:
        return True

    @property
    def positional_args_name(self) -> List[str]:
        return []

    def to_pyspark(self) -> str:
        return f"select({self._select_cols})"


@register_op()
class SelectOp(OperationNode):
    pandas_name: str = "select"

    def __init__(
        self,
        pandas_positional_args: List,
        pandas_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("select", pandas_positional_args, pandas_keyword_args)
        self._select_cols = (
            [self.pandas_positional_args]
            if isinstance(self.pandas_positional_args, str)
            else self.pandas_positional_args
        )

    @property
    def is_data_method(self) -> bool:
        return True

    @property
    def positional_args_name(self) -> List[str]:
        return []

    def to_pyspark(self) -> str:
        return f"select({self._select_cols})"
