import numpy as np

from hammer.config import Config
from hammer.schema import Datatype, Field, TableSchema


def test_table_schema():
    config = Config()
    schema = config.raw_data_schema
    assert isinstance(schema, TableSchema)
    assert isinstance(schema[0], Field)
    dummy_field = Field("a", Datatype("int"))
    assert dummy_field.to_numpy() == np.int64
    dummy_field = Field("b", Datatype("category"))
    assert dummy_field.to_numpy() == "category"
