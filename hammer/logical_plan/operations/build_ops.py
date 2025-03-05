import inspect
from typing import Any, Dict, List, Set

from .udf_ops import UserDefinedFunctionOp

_OPERATION_REGISTRY: Dict[str, type] = {}
_DATA_METHOD_OPERATION: Set[str] = set()


def register_op() -> type:
    def decorator(cls):
        if not hasattr(cls, "function_name"):
            raise AttributeError(f"类 {cls.__name__} 必须定义类属性 function_name")
        if cls.function_name != "udf":
            # 注册 pandas 中的内置方法
            _OPERATION_REGISTRY[cls.function_name] = cls
        # udf 只有给定入参 udf_name 和 udf_block 之后才能实例化，因此不走注册。
        # 注册方法链的方法
        if cls.is_data_method:
            _DATA_METHOD_OPERATION.add(cls.function_name)
        return cls

    return decorator


def create_ops(
    pandas_func_name: str = None,
    function_positional_args: List = None,
    function_keyword_args: Dict[str, Any] = None,
    *,
    target_ops: Dict = None,
    udf_name: str = None,
    udf_block: str = None,
    **kwargs,
):
    function_positional_args = function_positional_args or []
    function_keyword_args = function_keyword_args or {}

    pandas_func_name = kwargs.get("function_name") or pandas_func_name

    if udf_name is None:
        op_cls = _OPERATION_REGISTRY.get(pandas_func_name)
        if op_cls is None:
            raise ValueError(f"未注册算子: '{pandas_func_name}',\n已经注册的算子有: {_OPERATION_REGISTRY}")
        sig = inspect.signature(op_cls)
        if (
            "function_positional_args" in sig.parameters
            and "function_keyword_args" in sig.parameters
            and "target_ops" in sig.parameters
        ):
            return op_cls(function_positional_args, function_keyword_args, target_ops=target_ops)
        elif "function_positional_args" in sig.parameters and "function_keyword_args" in sig.parameters:
            return op_cls(function_positional_args, function_keyword_args)
        elif "function_positional_args" in sig.parameters:
            return op_cls(function_positional_args)
        elif "function_keyword_args" in sig.parameters:
            return op_cls(function_keyword_args)
        else:
            return op_cls()
    else:
        return UserDefinedFunctionOp(udf_name, udf_block, *function_positional_args, **function_keyword_args)
