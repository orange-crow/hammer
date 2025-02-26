from typing import Any, Dict, List

from .build_ops import register_op
from .operation import OperationNode


@register_op()
class GroupbyOp(OperationNode):
    pandas_name: str = "groupby"

    def __init__(
        self,
        pandas_positional_args: List,
        pandas_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("groupby", pandas_positional_args, pandas_keyword_args)
        self.by = self.pandas_keyword_args.get("by")

    @property
    def is_data_method(self) -> bool:
        return True

    @property
    def positional_args_name(self) -> List[str]:
        return ["by", "axis", "level", "as_index", "sort", "group_keys", "observed", "dropna"]

    def to_pyspark(self) -> str:
        return f"groupBy({self.by})"
