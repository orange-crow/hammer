from typing import Dict, List, Union

from modin import pandas as mpd

from ..schema import TableSchema
from ..utils.schema import apply_schema_to_pd, init_schema
from .table import TableBase


class BigTable(TableBase):
    def __init__(
        self,
        data: mpd.DataFrame,
        schema: Union[str, List[Dict], TableSchema],
    ):
        self._schema = init_schema(schema)
        self._data = apply_schema_to_pd(data, schema)

    @classmethod
    def _from_file(
        cls, file_path: str, schema: Union[str, List[Dict], TableSchema], reader: callable, **kwargs
    ) -> "BigTable":
        schema = init_schema(schema)
        df = reader(file_path, **kwargs)
        df = apply_schema_to_pd(df, schema)
        return cls(df, schema)

    @classmethod
    def from_csv(cls, file_path: str, schema: Union[str, List[Dict], TableSchema], **kwargs) -> "BigTable":
        return cls._from_file(file_path, schema, mpd.read_csv, **kwargs)

    @classmethod
    def from_parquet(cls, file_path: str, schema: Union[str, List[Dict], TableSchema], **kwargs) -> "BigTable":
        return cls._from_file(file_path, schema, mpd.read_parquet, **kwargs)

    def __len__(self):
        return len(self._data)

    # TODO: 其他方法
