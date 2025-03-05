import json

import pytest

from hammer.logical_plan import LogicalPlan


@pytest.fixture
def dag():
    dag = LogicalPlan()
    # Add data IO node
    dag.add_data_node("input_csv", "io", "data.csv")
    # Add operation node (read CSV and store in memory)
    dag.add_operation_node("read_csv", "pd.read_csv", ["input_csv"], {"sep": ","}, "input_csv")
    return dag


def test_add_node(dag: LogicalPlan):
    assert dag.has_node("input_csv")
    assert dag.has_node("read_csv")
    assert dag.graph.has_edge("input_csv", "read_csv")


def test_dag_to_json(dag: LogicalPlan):
    json_str = dag.to_json()
    data = json.loads(json_str)
    assert "nodes" in data
    assert "input_csv" == data["nodes"][0]["id"]
    assert "read_csv" == data["nodes"][1]["id"]


def test_dag_from_json(dag: LogicalPlan):
    json_str = dag.to_json()
    dag2 = dag.from_json(json_str)
    assert dag == dag2
