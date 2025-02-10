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
    def from_yaml(cls, data_schema: List[Dict]) -> "TableSchema":
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

    def to_yaml(self):
        raise NotImplementedError("No idea!")
