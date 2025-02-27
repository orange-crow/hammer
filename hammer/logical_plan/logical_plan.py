from typing import Any, Dict, List, Literal, Union

import networkx as nx

from .data_node import DataNode
from .operations import OperationNode, create_ops


class LogicalPlan(object):
    def __init__(self):
        """Initializes an empty Directed Acyclic Graph (DAG)."""
        self.graph = nx.DiGraph()

    def add_data_node(self, name: str, data_type: Literal["io", "memory", "sql"], source: str = None):
        """Adds a data node to the DAG.

        Args:
            name (str): Name of the data node.
            data_type (str): Type of the data node, either "io" (data IO) or "memory" (in-memory data) or "sql" (use sql to fetch data).
            source (str, optional): Data source (file path or database connection), only applicable for IO nodes.
        """
        self.graph.add_node(name, type="data", obj=DataNode(name, data_type=data_type, source=source))

    def add_operation_node(
        self,
        node_name: str,  # show in dag
        pandas_name: str,
        pandas_positional_args: List = None,
        pandas_keyword_args: Dict[str, Any] = None,
        input_nodes: Union[str, List] = None,
        output_node: str = None,
        *,
        target_ops: dict[str, str] = None,
    ):
        self.graph.add_node(
            node_name,
            type="op",
            obj=create_ops(pandas_name, pandas_positional_args, pandas_keyword_args, target_ops=target_ops),
        )

        if input_nodes:
            if isinstance(input_nodes, str):
                input_nodes = [input_nodes]

            if isinstance(input_nodes, list):
                for input_node in input_nodes:
                    self.graph.add_edge(input_node, node_name)
            else:
                raise ValueError

        if output_node:
            self.graph.add_edge(node_name, output_node)

    def visualize(self):
        """Visualizes the DAG using matplotlib."""
        import matplotlib.pyplot as plt

        pos = nx.spring_layout(self.graph)
        labels = {node: node for node in self.graph.nodes()}
        nx.draw(self.graph, pos, with_labels=True, labels=labels, node_size=2000, node_color="lightblue")
        plt.show()

    def __repr__(self):
        return f"DAG(nodes={list(self.graph.nodes)}, edges={list(self.graph.edges)})"

    def __getitem__(self, node_name: str) -> Dict[str, Any]:
        return self.graph.nodes.get(node_name)

    def has_node(self, node_name: str) -> bool:
        return self.graph.has_node(node_name)

    def node_startswith(self, prefix: str) -> List[str]:
        node_names = [n for n in self.graph.nodes if n.startswith(prefix)]
        return node_names

    def _get_shortest_paths(self, start_node: str, end_node: str) -> List[List[str]]:
        paths = list(nx.all_shortest_paths(self.graph, start_node, end_node, method="dijkstra"))
        return paths

    def _get_datanodes(self, start_node: str, end_node: str) -> List[str]:
        return [n for n in self._get_shortest_paths(start_node, end_node)[0] if self[n]["type"] == "data"]

    def _get_operation_nodes(self, start_node: str, end_node: str) -> List[str]:
        return [n for n in self._get_shortest_paths(start_node, end_node)[0] if self[n]["type"] == "op"]

    def get_input_nodes(self, node_name) -> List[str]:
        if not self.graph.has_node(node_name):
            raise KeyError(f"节点 {node_name} 不存在于图中。")
        return list(self.graph.predecessors(node_name))

    def to_pyspark(self, start_node: str, end_node: str):
        """Converts the DAG to PySpark code."""
        code_content = ""
        oneline_data = ""
        oneline_ops = ""
        for node_name in self._get_shortest_paths(start_node, end_node)[0]:
            node: Union[DataNode, OperationNode] = self[node_name]["obj"]
            if isinstance(node, DataNode):
                if node.data_type == "io":
                    oneline_data = f"\n{node.name} = '{node.source}'"
                    code_content += oneline_data
                elif node.data_type == "sql":
                    raise NotImplementedError
                elif node.data_type == "memory":
                    oneline_data = node.name
                    code_content += f"\n{oneline_data} = {oneline_ops}"
                else:
                    raise ValueError
                oneline_data = ""
                oneline_ops = ""
            elif isinstance(node, OperationNode):
                if oneline_ops:
                    oneline_ops += f".{node.to_pyspark()}"
                else:
                    input_nodes = self.get_input_nodes(node_name)
                    if input_nodes and len(input_nodes) == 1 and self[node_name]["obj"].is_data_method:
                        oneline_ops += f"{input_nodes[0]}.{node.to_pyspark()}"
                    else:
                        oneline_ops += node.to_pyspark()
            else:
                raise ValueError
        return code_content
