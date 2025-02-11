import os
import pprint
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict

import psutil


# 获取当前进程的内存使用情况（单位：MB）
def get_memory_usage() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)  # 返回单位为MB


@contextmanager
def time_memory_monitor(operation_name: str, result={}):
    start_time = time.time()
    start_memory = get_memory_usage()
    yield  # 执行监控代码块
    end_memory = get_memory_usage()
    elapsed_time = time.time() - start_time
    memory_used = end_memory - start_memory
    print(f"{operation_name} took {memory_used:.4f} MB.")
    print(f"{operation_name} took {elapsed_time:.4f} seconds.")
    result[operation_name] = {"time": elapsed_time, "memory": memory_used}  # 返回执行时间和使用的内存


# 比较函数执行时间和内存使用
def compare_operations(
    operation_name: str,
    a_func: Callable[[Any], Any],
    b_func: Callable[[Any], Any],
    a_input: Any,
    b_input: Any,
    results: Dict[str, Dict[str, float]],
) -> None:
    a_name, b_name = a_func.__name__, b_func.__name__
    # 监控 a 操作
    with time_memory_monitor(a_name, results) as a_monitor:  # noqa
        a_func(a_input)

    # 监控 b 操作
    with time_memory_monitor(b_name, results) as b_monitor:  # noqa
        b_func(b_input)

    pprint.pprint(results)
