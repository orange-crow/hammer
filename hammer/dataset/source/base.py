from abc import ABC, abstractmethod

import pandas as pd


class ClientBase(ABC):
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
    def read(self, query_or_file_path: str, *args, **kwargs) -> pd.DataFrame: ...

    def write(self, *args, **kwargs) -> None:
        raise NotImplementedError("Please implement this method!")
