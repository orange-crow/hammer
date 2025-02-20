from dataclasses import dataclass
from typing import Any, Dict

from .dtype import Datatype


@dataclass
class Field:
    name: str
    dtype: Datatype
    tag: str = None

    def to_numpy(self) -> Any:
        return self.dtype.to_numpy()

    def from_numpy(self, obj) -> Any:
        return self.dtype.from_numpy(obj)

    def to_torch(self) -> Any:
        return self.dtype.to_torch()

    def from_torch(self, obj) -> Any:
        return self.dtype.from_torch(obj)

    @classmethod
    def from_dict(cls, field: Dict) -> "Field":
        """load field from string, like: {'Y': 'int, target'}"""
        assert field, field
        assert isinstance(field, dict)
        assert len(field) == 1
        name, dtype, tag = None, None, None
        for field_name, field_dtype in field.items():
            name = field_name
            dtype_tag: list[str] = field_dtype.split(",")
            dtype = dtype_tag[0].strip()
            tag = None
            if len(dtype_tag) == 2:
                tag = dtype_tag[1].strip()
        return cls(name, dtype, tag)

    @classmethod
    def from_string(cls, field: str) -> "Field":
        """load field_dtype from string, like: 'Y: int, target'"""
        assert field, field
        assert field.count(":") == 1, field
        name = field.split(":")[0].strip()
        field_dtype = field.split(":")[1].strip()
        return cls.from_dict({name: field_dtype})
