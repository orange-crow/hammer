from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional

import pandas as pd
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed

from ...schema import Datatype, Field, TableSchema
from ..table import PandasTable


class ClientBase(ABC):
    """数据引擎的客户端配置"""

    def __init__(self, *, user, password, host, port, service_name=None) -> None:
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._service_name = service_name
        self._connection = None

    def __hash__(self):
        return hash((self._host, self._port))

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, ClientBase):
            raise TypeError("Comparisons should only involve ClientBase class objects.")

        if self._host != other._host or self._port != other._port or self._service_name != other._service_name:
            return False

        return True

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


class SourceConfig(ABC):
    """
    Configures data sources: database, table. Supports:
    1) Field selection
    2) Conditional data retrieval
    3) Field mapping
    4) Data partitioning
    5) Big data retrieval
    """

    def __init__(
        self,
        *,
        name: str,
        database: str = None,
        table: str = None,
        filter_conditions: str = None,
        field_mapping: Optional[Dict] = None,
        target_fields: List[str] = None,
        partition_by: Optional[str] = None,
    ):
        self.name = name
        self.database = database
        self.table = table
        self.filter_conditions = filter_conditions
        self.field_mapping = field_mapping or {}
        self.target_fields = target_fields or []
        self.partition_by = partition_by
        self._client = None
        self._schema = None
        self._raw_schema = None

    def __hash__(self):
        return hash((self.database, self.table, self.filter_conditions))

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, SourceConfig):
            raise TypeError("Comparisons should only involve SourceConfig class objects.")

        if (
            self.database != other.database
            or self.table != other.table
            or self.filter_conditions != other.filter_conditions
            or self.field_mapping != other.field_mapping
            or self.target_fields != other.target_fields
            or self.partition_by != other.partition_by
        ):
            return False

        return True

    def select_data(self) -> pd.DataFrame:
        sql = f"""
        select * 
        from {self.database}.{self.table}
        """

        if self.target_fields:
            sql = f"""
            select {','.join(self.target_fields)}
            from {self.database}.{self.table}
            """

        if self.field_mapping:
            sql = f"""
            select {','.join([f'{k} as {v}' for k, v in self.field_mapping.items()])}
            from {self.database}.{self.table}
            """

        if self.filter_conditions:
            sql += f"\nwhere {self.filter_conditions}"

        return self.client.read(sql)

    @property
    def raw_schema(self) -> pd.DataFrame:
        if self._raw_schema is None:
            self._raw_schema = self.get_schema()
        assert isinstance(self._raw_schema, pd.DataFrame)
        assert "name" in self._raw_schema.columns
        assert "dtype" in self._raw_schema.columns
        assert self._raw_schema.shape[1] == 2

        # update raw schema with target_fields
        if self.target_fields:
            self._raw_schema = self._raw_schema[self._raw_schema.name.isin(self.target_fields)]

        # update raw schema with field_mapping
        if self.field_mapping:
            self._raw_schema = self._raw_schema[self._raw_schema.name.isin(self.field_mapping.keys())]
        return self._raw_schema

    @property
    def schema(self) -> TableSchema:
        if self._schema is None:
            # convert raw schema to TableSchema
            self._schema = [
                Field(name, self.to_pandas_dtype(raw_dtype)) for i, (name, raw_dtype) in self.raw_schema.iterrows()
            ]
        return self._schema

    @property
    @abstractmethod
    def client(self) -> ClientBase:
        pass

    @abstractmethod
    def get_schema(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def read(self) -> pd.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def to_pandas_dtype(source_dtype: str) -> Callable[[str], Datatype]:
        pass


class DataSource(ABC):
    """数据获取, 并转换为可以处理的格式"""

    def __init__(
        self,
        *,
        name: str,
        time_field: Optional[str] = None,
        created_time_field: Optional[str] = None,
        owner: Optional[str] = None,
        description: Optional[str] = None,
        source_config: Optional[SourceConfig] = None,
    ):
        self.name = name
        self.time_field = time_field or ""
        self.created_time_field = created_time_field or ""
        if self.time_field and self.created_time_field == self.time_field:
            raise ValueError("Please do not use the same column for 'time_field' and 'created_time_field'.")
        self.owner = owner or ""
        self.description = description or ""
        self.source_config = source_config or SourceConfig()

    def __hash__(self):
        return hash((self.name, self.time_field))

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, DataSource):
            raise TypeError("Comparisons should only involve DataSource class objects.")

        if (
            self.name != other.name
            or self.time_field != other.time_field
            or self.created_time_field != other.created_time_field
            or self.description != other.description
            or self.owner != other.owner
            or self.source_config != other.source_config
        ):
            return False

        return True

    def to_pandas(self) -> PandasTable:
        data = self.source_config.read()
        return PandasTable(data, schema=self.source_config.schema)
