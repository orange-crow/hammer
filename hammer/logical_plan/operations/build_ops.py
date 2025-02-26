import inspect
from typing import Any, Dict, List, Set

_OPERATION_REGISTRY: Dict[str, type] = {}
_DATA_METHOD_OPERATION: Set[str] = set()


def register_op() -> type:
    def decorator(cls):
        if not hasattr(cls, "pandas_name"):
            raise AttributeError(f"类 {cls.__name__} 必须定义类属性 pandas_name")
        # 将类按 pandas_name 注册到字典中
        _OPERATION_REGISTRY[cls.pandas_name] = cls
        # 注册方法链的方法
        if cls.is_data_method:
            _DATA_METHOD_OPERATION.add(cls.pandas_name)
        return cls

    return decorator


def create_ops(
    pandas_func_name: str,
    pandas_positional_args: List,
    pandas_keyword_args: Dict[str, Any] = None,
    *,
    target_ops: Dict = None,
):
    op_cls = _OPERATION_REGISTRY.get(pandas_func_name)
    if op_cls is None:
        raise ValueError(f"未注册算子: '{pandas_func_name}',\n已经注册的算子有: {_OPERATION_REGISTRY}")
    sig = inspect.signature(op_cls)
    if (
        "pandas_positional_args" in sig.parameters
        and "pandas_keyword_args" in sig.parameters
        and "target_ops" in sig.parameters
    ):
        return op_cls(pandas_positional_args, pandas_keyword_args, target_ops=target_ops)
    elif "pandas_positional_args" in sig.parameters and "pandas_keyword_args" in sig.parameters:
        return op_cls(pandas_positional_args, pandas_keyword_args)
    elif "pandas_positional_args" in sig.parameters:
        return op_cls(pandas_positional_args)
    elif "pandas_keyword_args" in sig.parameters:
        return op_cls(pandas_keyword_args)
    else:
        return op_cls()
