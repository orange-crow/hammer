from abc import ABC, abstractmethod

from .. import TableBase
from .client import ClientBase


class BatchConfig(ABC):
    """数据引擎和数据读/写配置"""

    def __init__(self, database: str, table: str, filter_condition: str):
        self.database = database
        self.table = table
        self.filter_condition = filter_condition

    @abstractmethod
    @property
    def client(self) -> ClientBase: ...


class BatchSource(object):
    """获取外部数据和数据类型转换成 TableBase"""

    def __init__(self, name: str, batch_config: BatchConfig):
        self.name = name
        self.batch_config = batch_config

    @abstractmethod
    def read(self) -> TableBase:
        pass
