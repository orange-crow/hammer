import pandas as pd
import pytest

from hammer.table.big_table import BigTable
from hammer.utils.schema import init_schema


@pytest.fixture
def pt():
    return BigTable()


def test_pt_load(csv_path, parquet_path, schema):
    expected = pd.read_csv(csv_path)
    pt = BigTable.from_csv(csv_path, schema=schema)
    print(pt._data.head())
    assert isinstance(pt, BigTable)
    assert len(pt) == len(expected)
    assert not expected.equals(pt)
    assert pt._schema == init_schema(schema)

    pt2 = BigTable.from_parquet(parquet_path, schema=schema)
    print(pt2._data.head())
    assert len(pt2) == len(expected)
    assert not expected.equals(pt2)
    assert pt2._schema == init_schema(schema)
    print(pt2._schema)
