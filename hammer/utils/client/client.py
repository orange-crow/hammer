from functools import cached_property
from typing import Any, Dict, Optional

import pandas as pd
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed

from hammer.dataset.table import PandasTable


class ClientBase(object):
    """数据引擎的客户端基类，必须使用连接池

    强制子类实现连接池以优化资源复用
    """

    __slots__ = ("_user", "_password", "_host", "_port", "_database", "_service_name", "_pool")

    def __init__(
        self,
        *,
        user: str,
        password: str,
        host: str,
        port: str,
        database: str = None,
        service_name: Optional[str] = None,
    ) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._service_name = service_name
        self._pool = self._create_pool()

    @cached_property
    def _hash_key(self) -> tuple:
        """缓存哈希键，避免重复计算"""
        return (self._host, self._port, self._service_name)

    def __hash__(self):
        return hash(self._hash_key)

    def __eq__(self, other: Optional["ClientBase"]) -> bool:
        if not isinstance(other, ClientBase):
            return False
        return self._hash_key == other._hash_key

    def to_dict(self) -> Dict[str, str]:
        """转换为字典，避免不必要的内存分配"""
        base_dict = {
            "user": self._user,
            "password": self._password,
            "host": self._host,
            "port": self._port,
            "database": self._database,
        }
        return base_dict if self._service_name is None else {**base_dict, "service_name": self._service_name}

    @classmethod
    def from_dict(cls, config: Dict[str, str]) -> "ClientBase":
        return cls(**config)

    def _create_pool(self) -> Any:
        """创建连接池，必须由子类实现"""
        raise NotImplementedError

    def connect(self) -> Any:
        """从连接池获取连接，必须由子类实现"""
        raise NotImplementedError

    def _read(self, connection: Any, query_or_file_path: str, *args, **kwargs) -> pd.DataFrame:
        """使用连接读取数据，必须由子类实现"""
        raise NotImplementedError

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=lambda exception: isinstance(exception, ConnectionRefusedError),
    )
    def read(self, query_or_file_path: str, *args, **kwargs) -> PandasTable:
        logger.info(f"Read data by \n{query_or_file_path}")
        with self.connect() as connection:
            return PandasTable(self._read(connection, query_or_file_path, *args, **kwargs))

    def __enter__(self) -> "ClientBase":
        """支持上下文管理器，返回自身"""
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """无需显式清理，由连接池管理"""
        pass
