from dataclasses import dataclass
from typing import Any

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
        raise self.dtype.to_torch()

    def from_torch(self, obj) -> Any:
        raise self.dtype.from_torch(obj)
