from typing import Literal

from . import Operation


class LocalCSVFile(Operation):
    def __init__(
        self,
        name,
        pandas_params,
        input_nodes,
        output_node,
        *,
        target_ops=None,
        engine: Literal["pandas", "pyarrow", "pyspark"] = "pandas"
    ):
        super().__init__(name, pandas_params, input_nodes, output_node, target_ops=target_ops)
        self.sep = self.pandas_params.get("sep", ",")
        self.index_col = self.pandas_params.get("index_col")
        self.engine = engine

    def to_pyspark(self):
        return super().to_pyspark()
