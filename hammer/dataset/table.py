from abc import ABC, abstractmethod
from typing import Dict, List, Union

import pandas as pd

from ..schema import TableSchema
from ..utils.schema import apply_schema, init_schema


class TableBase(ABC):

    @abstractmethod
    def from_csv(self, *args, **kwargs) -> "TableBase":
        pass

    @abstractmethod
    def from_parquet(self, *args, **kwargs) -> "TableBase":
        pass


class PandasTable(pd.DataFrame, TableBase):
    _metadata = ["schema"]  # 保留自定义属性

    def __init__(self, *args, **kwargs):
        schema = kwargs.pop("schema", None)  # 从 kwargs 中取出 schema
        super(PandasTable, self).__init__(*args, **kwargs)
        if schema is not None:
            self.schema = init_schema(schema)
        else:
            self.schema = None

    @classmethod
    def from_csv(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], sep: str = ",", **kwargs
    ) -> "TableBase":
        schema = init_schema(schema)
        df = pd.read_csv(file_path, sep=sep, **kwargs)
        df = apply_schema(df, schema)
        return cls(df, schema=schema)

    @classmethod
    def from_parquet(cls, file_path: str, schema: Union[str, List[Dict], TableSchema], **kwargs) -> "TableBase":
        schema = init_schema(schema)
        df = pd.read_parquet(file_path, **kwargs)
        df = apply_schema(df, schema)
        return cls(df, schema=schema)
