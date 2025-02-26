from abc import abstractmethod
from typing import Any, Dict, List, Literal, Tuple


class Operation(object):
    def __init__(
        self,
        pandas_name: str,
        pandas_params: Tuple[List, Dict],
        *,
        target_ops: Dict = {},
        engine: Literal["pandas", "pyarrow", "pyspark"] = "pandas",
    ):
        self.pandas_name = pandas_name  # name from pandas.
        self.pandas_params = pandas_params or {}  # params of operation excluding input nodes
        self.target_ops = target_ops
        self.engine = engine

    def __repr__(self):
        return f"{self.__class__.__name__}({self.pandas_keword_args})"

    @property
    @abstractmethod
    def positional_args_name(self):
        pass

    @property
    def pandas_keword_args(self) -> Dict[str, Any]:
        positional_args = self.pandas_params[0]
        keyword_args = dict(self.pandas_params[1])
        for arg_name, value in zip(self.positional_args_name, positional_args):
            keyword_args[arg_name] = value
        return keyword_args

    def to_pyspark(self) -> str:
        raise NotImplementedError

    def to_dask(self) -> str:
        raise NotImplementedError
