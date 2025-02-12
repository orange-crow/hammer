import pandas as pd
import pytest

from hammer.dataset import PandasTable
from hammer.utils.schema import init_schema


@pytest.fixture
def pt():
    return PandasTable()


@pytest.fixture
def csv_path():
    return "tests/data/sample_data.csv"


@pytest.fixture
def parquet_path():
    return "tests/data/sample_data.parquet"


@pytest.fixture
def schema():
    return "id: int, entity_1;category: str, entity_1*1;value: float, target;timestamp: datetime, main_time"


def test_pt_load(csv_path, parquet_path, schema):
    expected = pd.read_csv(csv_path)
    pt = PandasTable.from_csv(csv_path, schema=schema)
    assert isinstance(pt, PandasTable)
    assert len(pt) == len(expected)
    assert not expected.equals(pt)
    assert pt._schema == init_schema(schema)

    pt2 = PandasTable.from_parquet(parquet_path, schema=schema)
    assert len(pt2) == len(expected)
    assert not expected.equals(pt2)
    assert pt2._schema == init_schema(schema)
    print(pt2._schema)
