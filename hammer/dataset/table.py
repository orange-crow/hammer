from typing import Dict, List, Union

import pandas as pd

from ..schema import TableSchema
from ..utils.schema import apply_schema_to_pd, init_schema
from .table_base import TableBase


class PandasTable(pd.DataFrame, TableBase):
    _metadata = ["_schema"]  # 保留自定义属性

    def __init__(self, *args, **kwargs):
        schema = kwargs.pop("schema", None)  # 从 kwargs 中取出 schema
        super(PandasTable, self).__init__(*args, **kwargs)
        if schema is not None:
            self._schema = init_schema(schema)
        else:
            self._schema = None

    @classmethod
    def _from_file(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], reader: callable, **kwargs
    ) -> "TableBase":
        schema = init_schema(schema)
        df = reader(file_path, **kwargs)
        ds = apply_schema_to_pd(df, schema)
        return cls(ds, schema=schema)

    @classmethod
    def from_csv(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], sep: str = ",", **kwargs
    ) -> "PandasTable":
        return cls._from_file(file_path, schema, pd.read_csv, sep=sep, **kwargs)

    @classmethod
    def from_parquet(cls, file_path: str, schema: Union[str, List[Dict], TableSchema], **kwargs) -> "PandasTable":
        return cls._from_file(file_path, schema, pd.read_parquet, **kwargs)
