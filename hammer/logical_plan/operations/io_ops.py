from typing import Any, Dict, List

from .build_ops import register_op
from .operation import OperationNode


@register_op()
class ReadcsvOp(OperationNode):
    # 设置 function_name 类属性, 作为注册时的标识.
    function_name: str = "pd.read_csv"

    def __init__(
        self,
        function_positional_args: List,
        function_keyword_args: Dict[str, Any] = None,
    ):
        super().__init__("pd.read_csv", function_positional_args, function_keyword_args)
        self.file_path = self.function_keyword_args.get("filepath_or_buffer")

    @property
    def is_data_method(self) -> bool:
        return False

    @property
    def positional_args_name(self) -> List[str]:
        return ["filepath_or_buffer"]

    def to_pyspark(self) -> str:
        return f'spark.read.load({self.file_path}, format="csv", header=True)'
