import io
from typing import Optional

import pandas as pd
from psycopg2 import pool

from .client import ClientBase  # 假设 ClientBase 在 base.py 中


class PostgresClient(ClientBase):
    """PostgreSQL 数据库客户端实现，使用连接池"""

    def __init__(
        self,
        *,
        user: str,
        password: str,
        host: str,
        port: str,
        database: str,
        service_name: Optional[str] = None,
        pool_size: int = 5,
    ):
        self._database = database
        self._pool_size = pool_size
        super().__init__(
            user=user, password=password, host=host, port=port, database=database, service_name=service_name
        )
        self._pool = self._create_pool()

    def _create_pool(self):
        """创建 PostgreSQL 连接池"""
        return pool.SimpleConnectionPool(
            minconn=1,  # 最小连接数
            maxconn=self._pool_size,  # 最大连接数
            user=self._user,
            password=self._password,
            host=self._host,
            port=self._port,
            database=self._database,
        )

    def connect(self):
        """从连接池获取连接"""
        return self._pool.getconn()

    def release(self, connection):
        """释放连接回连接池"""
        self._pool.putconn(connection)

    def _read(self, connection, query_or_file_path: str, *args, **kwargs) -> pd.DataFrame:
        """使用 PostgreSQL 连接执行查询并返回 DataFrame"""
        with connection.cursor() as cursor:
            if kwargs.get("use_copy"):
                output = io.StringIO()
                cursor.copy_expert(f"COPY ({query_or_file_path}) TO STDOUT WITH CSV HEADER", output)
                output.seek(0)
                df = pd.read_csv(output, engine="pyarrow")
            else:
                cursor.execute(query_or_file_path, *args)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
        self.release(connection)
        return df
