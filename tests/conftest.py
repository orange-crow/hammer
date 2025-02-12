import pytest


@pytest.fixture
def csv_path():
    return "tests/data/sample_data.csv"


@pytest.fixture
def parquet_path():
    return "tests/data/sample_data.parquet"


@pytest.fixture
def schema():
    return "id: int, entity_1;category: str, entity_1*1;value: float, target;timestamp: datetime, main_time"
