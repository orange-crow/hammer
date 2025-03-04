from dataclasses import dataclass
from typing import List

from typeguard import typechecked


@typechecked
@dataclass
class Entity:
    name: str
    join_keys: list[str]

    def __init__(self, name: str, join_keys: List[str]):
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
