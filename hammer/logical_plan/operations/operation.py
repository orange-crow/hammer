import json
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
        **kwargs,
    ):
        self.function_name = function_name  # name from pandas.
        self.function_positional_args = function_positional_args or []
        self._function_keyword_args = function_keyword_args or {}
        self.target_ops = target_ops

    def __repr__(self):
        return f"{self.__class__.__name__}({self.function_keyword_args})"

    def __eq__(self, other: "OperationNode") -> bool:
        if not isinstance(other, OperationNode):
            raise TypeError("Comparison should only involve OperationNode class object.")

        if (
            self.function_name != other.function_name
            or self.function_positional_args != other.positional_args_name
            or self.function_keyword_args != other.function_keyword_args
            or self.target_ops != other.target_ops
        ):
            return False

        if hasattr(self, "udf_name"):
            if self.udf_name != other.udf_name or self.udf_block != other.udf_block:
                return False

        return True

    def to_dict(self):
        """Convert the DataNode to a dictionary."""
        default_dict = {
            "function_name": self.function_name,
            "function_positional_args": self.function_positional_args,
            "function_keyword_args": self.function_keyword_args,
            "target_ops": self.target_ops,
        }
        if hasattr(self, "udf_name"):
            default_dict.update({"udf_name": self.udf_name, "udf_block": self.udf_block})
        return default_dict

    def to_json(self) -> str:
        """Convert the OperationNode to a JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, op_node_json: str) -> "OperationNode":
        """Create a OperationNode instance from a JSON string.

        Args:
            op_node_json (str): JSON string containing DataNode data

        Returns:
            DataNode: A new DataNode instance
        """
        data = json.loads(op_node_json)
        if "udf_name" not in data:
            return cls(
                function_name=data["function_name"],
                function_positional_args=data["function_positional_args"],
                function_keyword_args=data["function_keyword_args"],
                target_ops=data["target_ops"],
            )
        return cls(
            function_name=data["function_name"],
            function_positional_args=data["function_positional_args"],
            function_keyword_args=data["function_keyword_args"],
            target_ops=data["target_ops"],
            udf_name=data["udf_name"],
            udf_block=data["udf_block"],
        )

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
