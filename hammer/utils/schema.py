from typing import Dict, List, Union

import pandas as pd
from ray.data import Dataset

from ..schema import TableSchema


def init_schema(schema: Union[str, List[Dict], TableSchema]) -> TableSchema:
    if isinstance(schema, list):
        return TableSchema.from_list(schema)
    elif isinstance(schema, str):
        return TableSchema.from_string(schema)
    elif isinstance(schema, TableSchema):
        return schema
    else:
        raise ValueError(f"schema数据类型不符合期望: {type(schema).__name__}, 期望类型是string和list!")


def apply_schema_to_pd(df: pd.DataFrame, schema: TableSchema):
    for f in schema.fields:
        if f.dtype == "datetime":
            df[f.name] = pd.to_datetime(df[f.name])
        else:
            df[f.name] = df[f.name].astype(f.dtype)
    return df


def apply_schema_to_ray(dataset: Dataset, schema: TableSchema) -> Dataset:
    # 定义转换函数
    def convert_row(row: dict) -> dict:
        for f in schema.fields:
            if f.dtype == "datetime":
                row[f.name] = pd.to_datetime(row[f.name])
            else:
                row[f.name] = f"{row[f.name]}" if f.dtype == "str" else row[f.name]
        return row

    # 使用 map_batches 逐批转换数据
    return dataset.map_batches(lambda batch: [convert_row(row) for row in batch])
