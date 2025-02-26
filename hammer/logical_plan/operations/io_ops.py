from typing import Dict, List, Literal

from .. import Operation


class ReadcsvOp(Operation):
    def __init__(
        self,
        pandas_params: List[List, Dict],
        *,
        target_ops=None,
        engine: Literal["pandas", "pyarrow", "pyspark"] = "pandas",
    ):
        super().__init__("pd.read_csv", pandas_params, target_ops=target_ops)
        self.file_path = self.pandas_keword_args.get("filepath_or_buffer")
        self.engine = engine

    @property
    def positional_args_name(self) -> List[str]:
        return ["filepath_or_buffer"]

    def to_pyspark(self) -> str:
        return f'spark.read.load({self.file_path}, format="csv", header=True)'
