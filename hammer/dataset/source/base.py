from typing import Dict, Literal, Optional

from hammer.config import CONF
from hammer.core.protos.source_pb2 import Source as SourceProto
from hammer.dataset.table import PandasTable
from hammer.utils.client.clickhouse import ClickHouseClient
from hammer.utils.client.client import ClientBase


class DataSource(object):
    """创建批数据源, 生成元数据信息"""

    def __init__(
        self,
        name: str,
        version: str,
        table_name: str,
        infra_type: Literal["clickhouse", "oracle"],
        infra_name: str = None,  # 等价于 config，是client的配置参数
        *,
        filter_conditions: str = None,
        field_mapping: Optional[Dict] = None,
        owner: str = None,
        description: str = None,
        config: Dict = None,
    ):
        self.name = name
        self.version = version
        self.table_name = table_name
        self.filter_conditions = filter_conditions
        self.field_mapping = field_mapping
        self.infra_name = infra_name
        self.infra_type = infra_type

        if self.infra_name and self.infra_type:
            self.client_config = CONF.infra.get(self.infra_type).get(self.infra_name)
        else:
            self.client_config = config
        assert self.client_config is not None
        self.database = self.client_config.get("database")
        assert self.database is not None

        self.owner = owner
        self.description = description
        self._client = None
        self._data = None
        self._fetch_data_sql = None

    @property
    def __hash_key__(self):
        return (
            self.name,
            self.version,
            self.table_name,
            self.infra_type,
            self.infra_name,
            self.owner,
            self.description,
        )

    def __hash__(self):
        return hash(self.__hash_key__)

    def __eq__(self, value: "DataSource"):
        if not isinstance(value, DataSource):
            return False
        return self.__hash_key__ == value.__hash_key__

    @property
    def fetch_data_sql(self) -> str:
        raise NotImplementedError

    @property
    def client(self) -> ClientBase:
        if self._client is None:
            if self.infra_type == "clickhouse":
                self._client = ClickHouseClient(**self.client_config)
            else:
                raise NotImplementedError
        return self._client

    @property
    def data(self) -> PandasTable:
        if self._data is None:
            self._data = self.client.read(self.fetch_data_sql)
        return self._data

    def to_dict(self):
        return {
            "name": self.name,
            "version": self.version,
            "table_name": self.table_name,
            "infra_type": self.infra_type,
            "field_mapping": self.field_mapping,
            "owner": self.owner,
            "description": self.description,
            "config": self.client.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: dict) -> "DataSource":
        return cls(**value)

    def to_proto(self) -> SourceProto:
        return SourceProto(**self.to_dict())

    @classmethod
    def from_proto(cls, value: SourceProto) -> "DataSource":
        return cls(
            name=value.name,
            version=value.version,
            table_name=value.table_name,
            infra_type=value.infra_type,
            field_mapping=value.field_mapping,
            owner=value.owner,
            description=value.description,
            config=value.config,
        )
