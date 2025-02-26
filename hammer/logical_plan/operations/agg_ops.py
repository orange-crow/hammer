from typing import Dict, List, Literal

from .. import Operation


class SumOp(Operation):
    def __init__(
        self,
        pandas_params: List[List, Dict],
        *,
        target_ops=None,
        engine: Literal["pandas", "pyarrow", "pyspark"] = "pandas"
    ):
        super().__init__("sum", pandas_params, target_ops=target_ops)
        self.engine = engine

    @property
    def positional_args_name(self) -> List[str]:
        return []

    def to_pyspark(self) -> str:
        return "sum()"
