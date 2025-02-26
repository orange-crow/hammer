from typing import List

from .build_ops import register_op
from .operation import OperationNode


@register_op()
class SumOp(OperationNode):
    pandas_name: str = "sum"

    def __init__(self):
        super().__init__("sum", None, None)

    @property
    def is_data_method(self) -> bool:
        return True

    @property
    def positional_args_name(self) -> List[str]:
        return []

    def to_pyspark(self) -> str:
        return "sum()"
