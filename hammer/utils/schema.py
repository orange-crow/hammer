from typing import Dict, List, Union

import pandas as pd

from ..schema import Field, TableSchema


def init_schema(schema: Union[str, List[Dict], TableSchema]) -> TableSchema:
    if isinstance(schema, list) and isinstance(schema[0], str):
        return TableSchema.from_list(schema)
    elif isinstance(schema, list) and isinstance(schema[0], Field):
        return TableSchema(schema)
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
