from typing import List

from .operation import OperationNode


class UserDefinedFunctionOp(OperationNode):
    function_name: str = "udf"

    def __init__(self, udf_name: str, udf_block: str, *args, **kwargs):
        super().__init__("udf", args, kwargs)
        self.udf_name = udf_name
        self.udf_block = udf_block

    @property
    def is_data_method(self) -> bool:
        return False

    @property
    def positional_args_name(self) -> List[str]:
        return []

    def register_udf(self) -> str:
        return f"@udf\n{self.udf_block}"

    def to_pyspark(self) -> str:
        _args = ""
        if self.positional_args_name:
            _args = ",".join(self.positional_args_name)

        if self.function_keyword_args:
            _args += ",".join([f"{var_name}={var_value}" for var_name, var_value in self.function_keyword_args.items()])

        return f"{self.udf_name}({_args})" if _args else f"{self.udf_name}()"
