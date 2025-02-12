from dataclasses import dataclass
from typing import Dict, List

from .field import Field


@dataclass
class TableSchema:
    fields: List[Field]

    @property
    def names(self):
        return [f.name for f in self.fields]

    def __getitem__(self, index: int) -> Field:
        """支持通过索引访问字段"""
        return self.fields[index]

    def __len__(self) -> int:
        """支持返回字段数量"""
        return len(self.fields)

    @classmethod
    def from_list(cls, data_schema: List[Dict]) -> "TableSchema":
        assert isinstance(data_schema[0], dict)
        fields = []
        for f in data_schema:
            for field_name, field_dtype in f.items():
                dtype_tag = field_dtype.split(",")
                dtype = field_dtype.split(",")[0].strip()
                tag = None
                if len(dtype_tag) == 2:
                    tag = field_dtype.split(",")[1].strip()
                fields.append(Field(field_name, dtype, tag))
        return cls(fields)

    @classmethod
    def from_string(cls, data_schema: str) -> "TableSchema":
        """从字符形式载入数据表的schema

        Args:
            data_schema (str): 字符形式schema, "Y: int, target; YEARWEEK: datetime, main_time; QUANTITY: int"

        Returns:
            TableSchema: 返回创建好的TableSchema对象
        """
        assert ":" in data_schema
        assert ";" in data_schema
        schema = []
        for f in data_schema.split(";"):
            field_name = f.split(":")[0].strip()
            field_dtype = f.split(":")[1].strip()
            schema.append({field_name: field_dtype})
        return cls.from_list(schema)
