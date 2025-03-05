import json

import pytest

from hammer.logical_plan import DataNode


# Test fixtures
@pytest.fixture
def memory_node():
    return DataNode(name="test_memory", data_type="memory")


@pytest.fixture
def io_node():
    return DataNode(name="test_io", data_type="io", source="/path/data.csv")


# Test class initialization
def test_init_basic():
    node = DataNode(name="basic")
    assert node.name == "basic"
    assert node.data_type == ""
    assert node.source == ""


def test_init_with_all_params():
    node = DataNode(name="full", data_type="io", source="test.csv")
    assert node.name == "full"
    assert node.data_type == "io"
    assert node.source == "test.csv"


# Test __repr__
def test_repr(memory_node):
    expected = "DataNode(name=test_memory, data_type=memory, source=)"
    assert str(memory_node) == expected


# Test to_dict
def test_to_dict_memory(memory_node):
    expected = {"name": "test_memory", "data_type": "memory", "source": ""}
    assert memory_node.to_dict() == expected


def test_to_dict_io(io_node):
    expected = {"name": "test_io", "data_type": "io", "source": "/path/data.csv"}
    assert io_node.to_dict() == expected


# Test to_json
def test_to_json_memory(memory_node):
    json_str = memory_node.to_json()
    data = json.loads(json_str)
    assert data["name"] == "test_memory"
    assert data["data_type"] == "memory"
    assert data["source"] == ""


def test_to_json_io(io_node):
    json_str = io_node.to_json()
    data = json.loads(json_str)
    assert data["name"] == "test_io"
    assert data["data_type"] == "io"
    assert data["source"] == "/path/data.csv"


# Test from_json
def test_from_json_roundtrip(memory_node):
    json_str = memory_node.to_json()
    new_node = DataNode.from_json(json_str)
    assert new_node.name == memory_node.name
    assert new_node.data_type == memory_node.data_type
    assert new_node.source == memory_node.source


def test_from_json_invalid():
    invalid_json = "{invalid json}"
    with pytest.raises(json.JSONDecodeError):
        DataNode.from_json(invalid_json)


# Test edge cases
def test_empty_name():
    node = DataNode(name="")
    assert node.name == ""
    assert node.to_dict()["name"] == ""


def test_none_values():
    node = DataNode(name="none_test", data_type=None, source=None)
    assert node.data_type == ""
    assert node.source == ""
