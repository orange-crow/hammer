import json
from dataclasses import dataclass
from typing import Dict, List

from typeguard import typechecked


@typechecked
@dataclass
class Entity:
    name: str
    join_keys: list[str]

    def __init__(self, *, name: str, join_keys: List[str]):
        self._name = name
        self._join_keys = join_keys

    @property
    def name(self):
        return self._name

    @property
    def join_keys(self):
        return self._join_keys

    def __hash__(self):
        return hash((self.name, ",".join(self.join_keys)))

    def to_dict(self) -> Dict:
        return {"name": self.name, "join_keys": self.join_keys}

    @classmethod
    def from_dict(cls, value: Dict) -> "Entity":
        name = value.get("name")
        assert name is not None, name
        join_keys = value.get("join_keys")
        assert join_keys is not None, join_keys
        return cls(name, join_keys)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, value: str) -> "Entity":
        val = json.loads(value)
        return cls.from_dict(val)
