from abc import abstractmethod
from typing import Any, Dict, List


class OperationNode(object):
    def __init__(
        self,
        function_name: str,
        function_positional_args: List[str] = None,
        function_keyword_args: Dict[str, Any] = None,
        *,
        target_ops: Dict = {},
    ):
        self.function_name = function_name  # name from pandas.
        self.function_positional_args = function_positional_args or []
        self._function_keyword_args = function_keyword_args or {}
        self.target_ops = target_ops

    def __repr__(self):
        return f"{self.__class__.__name__}({self.function_keyword_args})"

    @property
    @abstractmethod
    def is_data_method(self) -> bool:
        pass

    @property
    @abstractmethod
    def positional_args_name(self):
        pass

    @property
    def function_keyword_args(self) -> Dict[str, Any]:
        keyword_args = dict(self._function_keyword_args)
        for arg_name, value in zip(self.positional_args_name, self.function_positional_args):
            keyword_args[arg_name] = value
        return keyword_args

    def to_pyspark(self) -> str:
        raise NotImplementedError

    def to_dask(self) -> str:
        raise NotImplementedError
