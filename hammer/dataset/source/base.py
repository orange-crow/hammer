from abc import ABC, abstractmethod

import pandas as pd
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed


class ClientBase(ABC):
    """负责数据同步和更新, 以及大数据处理"""

    def __init__(self, user, password, host, port, service_name=None) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._service_name = service_name
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    def close(self):
        self.connection.close()
        self._connection = None

    @abstractmethod
    def connect(self): ...

    @abstractmethod
    def _read(self, query_or_file_path: str, *args, **kwargs) -> pd.DataFrame: ...

    @retry(
        stop=stop_after_attempt(3),  # 最多重试3次
        wait=wait_fixed(2),  # 每次重试间隔2秒
        retry=retry_if_exception(ConnectionRefusedError),  # 根据如果连接异常，则多试三次
    )
    def read(self, query_or_file_path: str, *args, **kwargs) -> pd.DataFrame:
        return self._read(query_or_file_path, *args, **kwargs)

    def write(self, *args, **kwargs) -> None:
        raise NotImplementedError("Please implement this method!")
