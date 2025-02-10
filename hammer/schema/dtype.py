from dataclasses import dataclass
from typing import Any

import numpy as np

pandas_type_mapping = {
    "int": np.int64,
    "int8": np.int8,
    "int16": np.int16,
    "int32": np.int32,
    "str": np.str_,
    "float": np.float64,
    "float32": np.float32,
    "bool": np.bool_,
    "datetime": np.datetime64,
    "timedelta64": np.timedelta64,
    "category": "category",
}

flipped_pandas_type_mapping = {v: k for k, v in pandas_type_mapping.items()}


@dataclass
class Datatype:
    name: str

    def to_numpy(self) -> Any:
        return pandas_type_mapping.get(self.name, np.str_)

    def from_numpy(self, obj) -> Any:
        return flipped_pandas_type_mapping.get(obj, "str")

    def to_torch(self) -> Any:
        raise NotImplementedError("No idea!")

    def from_torch(self, obj) -> Any:
        raise NotImplementedError("No idea!")
