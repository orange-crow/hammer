from dataclasses import dataclass, field
from typing import Dict, List

from .field import Field


@dataclass
class TableSchema:
    fields: List[Field]
    _entity: List[str] = field(default_factory=list)
    _time_field: str = None
    _created_time_field: str = None

    @property
    def names(self):
        return [f.name for f in self.fields]

    @property
    def entity(self) -> List[str]:
        if not self._entity:
            self._entity = [f.name for f in self.fields if f.tag.lower() == "entity"]
        return self._entity

    @entity.setter
    def entity(self, names: List[str]):
        assert names, names
        if isinstance(names, str):
            names = [names]
        self._entity = names
        # Make sure that the entity is in schema.
        setting_names_is_in_schema = [True if setting_name in self.names else False for setting_name in names]
        assert all(setting_names_is_in_schema), f"Not all elements of {names} are in schema: {self.fields}"

    @property
    def time_field(self) -> List[str]:
        if not self._time_field:
            self._time_field = [f for f in self.fields if f.tag.lower() == "main_time"]
        return self._time_field

    @property
    def created_time_field(self) -> List[str]:
        if not self._created_time_field:
            self._created_time_field = [f for f in self.fields if f.tag.lower() == "created_time"]
        return self._created_time_field

    def __getitem__(self, index: int) -> Field:
        """支持通过索引访问字段"""
        return self.fields[index]

    def __len__(self) -> int:
        """支持返回字段数量"""
        return len(self.fields)

    @classmethod
    def from_list(cls, data_schema: List[Dict]) -> "TableSchema":
        """load schema from List[Dict], like: [{"Y": "int, target"}, {"YEARWEEK": "datetime, main_time"}]"""
        assert isinstance(data_schema[0], dict), data_schema[0]
        fields = [Field.from_dict(f) for f in data_schema]
        return cls(fields)

    @classmethod
    def from_string(cls, data_schema: str) -> "TableSchema":
        """load schema from string, like: "Y: int, target; YEARWEEK: datetime, main_time; QUANTITY: int" """
        assert ":" in data_schema
        assert ";" in data_schema
        fields = [Field.from_string(f) for f in data_schema.split(";")]
        return cls(fields)

    def to_dict(self) -> dict:
        return {f.name: f.dtype for f in self.fields}

    def __str__(self):
        return f"Table Schema is: {str(self.to_dict())}"
