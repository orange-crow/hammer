from typing import Any, Dict, List

from .build_ops import register_op
from .operation import OperationNode


@register_op()
class LocOp(OperationNode):
    function_name: str = "loc"

    def __init__(
        self,
        function_positional_args: List,
        function_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("loc", function_positional_args, function_keyword_args)
        self._select_cols = (
            [self.function_positional_args]
            if isinstance(self.function_positional_args, str)
            else self.function_positional_args
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
    function_name: str = "select"

    def __init__(
        self,
        function_positional_args: List,
        function_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("select", function_positional_args, function_keyword_args)
        self._select_cols = (
            [self.function_positional_args]
            if isinstance(self.function_positional_args, str)
            else self.function_positional_args
        )

    @property
    def is_data_method(self) -> bool:
        return True

    @property
    def positional_args_name(self) -> List[str]:
        return []

    def to_pyspark(self) -> str:
        return f"select({self._select_cols})"
