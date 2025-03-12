from typing import Dict, Optional

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from urllib3 import PoolManager

from .client import ClientBase  # 假设 ClientBase 在 base.py 中


class ClickHouseClient(ClientBase):
    """ClickHouse 数据库客户端实现，使用连接池"""

    def __init__(
        self,
        *,
        user: str,
        password: str,
        host: str,
        port: str,
        database: str = None,
        service_name: Optional[str] = None,
        pool_size: int = 5
    ):
        self._database = database
        self._pool_size = pool_size
        super().__init__(
            user=user, password=password, host=host, port=port, database=database, service_name=service_name
        )

    def _create_pool(self):
        """创建 ClickHouse 连接池"""
        pool_mgr = PoolManager(
            num_pools=self._pool_size,  # 设置 5 个连接池
            maxsize=20,  # 每个池最多 20 个连接
            timeout=10.0,  # 超时时间为 10 秒
            retries=3,  # 重试 3 次
            block=True,  # 连接池满时阻塞等待
        )
        return clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            pool_mgr=pool_mgr,
        )

    def connect(self) -> Client:
        """从连接池获取连接"""
        return self._pool

    def _read(self, connection: Client, query_or_file_path: str, *args, **kwargs) -> pd.DataFrame:
        """使用 ClickHouse 连接执行查询并返回 DataFrame"""
        return connection.query_df(query_or_file_path, *args, **kwargs)

    def to_dict(self) -> Dict[str, str]:
        """重写 to_dict，添加 database 参数"""
        base_dict = super().to_dict()
        base_dict["database"] = self._database
        return base_dict
