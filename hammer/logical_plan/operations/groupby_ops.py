from typing import Dict, List, Literal

from .. import Operation


class GroupbyOp(Operation):
    def __init__(
        self,
        pandas_params: List[List, Dict],
        *,
        target_ops=None,
        engine: Literal["pandas", "pyarrow", "pyspark"] = "pandas",
    ):
        super().__init__("groupby", pandas_params, target_ops=target_ops)
        self.by = self.pandas_keword_args.get("by")
        self.engine = engine

    @property
    def positional_args_name(self) -> List[str]:
        return ["by", "axis", "level", "as_index", "sort", "group_keys", "observed", "dropna"]

    def to_pyspark(self) -> str:
        return f"groupBy({self.by})"
