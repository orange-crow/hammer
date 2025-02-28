from typing import Any, Dict, List

from .build_ops import register_op
from .operation import OperationNode


@register_op()
class GroupbyOp(OperationNode):
    function_name: str = "groupby"

    def __init__(
        self,
        function_positional_args: List,
        function_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("groupby", function_positional_args, function_keyword_args)
        self.by = self.function_keyword_args.get("by")

    @property
    def is_data_method(self) -> bool:
        return True

    @property
    def positional_args_name(self) -> List[str]:
        return ["by", "axis", "level", "as_index", "sort", "group_keys", "observed", "dropna"]

    def to_pyspark(self) -> str:
        return f"groupBy({self.by})"
