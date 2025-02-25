from typing import Any, Dict, List, Literal, Union

import networkx as nx

from .create_ops import create_ops
from .node import Node


class DAG(object):
    def __init__(self):
        """Initializes an empty Directed Acyclic Graph (DAG)."""
        self.graph = nx.DiGraph()

    def add_data_node(self, name: str, data_type: Literal["io", "memory"], source: str = None):
        """Adds a data node to the DAG.

        Args:
            name (str): Name of the data node.
            data_type (str): Type of the data node, either "io" (data IO) or "memory" (in-memory data).
            source (str, optional): Data source (file path or database connection), only applicable for IO nodes.
        """
        self.graph.add_node(name, type="data", obj=Node(name, node_type="data", data_type=data_type, source=source))

    def add_operation_node(
        self,
        pandas_name: str,
        pandas_params: dict,
        input_nodes: Union[str, List, Dict],
        output_node: str,
        *,
        target_ops: dict[str, str] = None,
    ):
        self.graph.add_node(
            pandas_name,
            type="op",
            obj=create_ops(pandas_name, pandas_params, target_ops=target_ops),
        )
        if isinstance(input_nodes, str):
            input_nodes = [input_nodes]

        if isinstance(input_nodes, list):
            for input_node in input_nodes:
                self.graph.add_edge(input_node, pandas_name)  # Connect input data nodes
        elif isinstance(input_nodes, dict):
            for edge_name, input_node in input_nodes.items():
                self.graph.add_edge(input_node, pandas_name)  # Connect input data nodes
                # TODO: 将edge_name附到edge上
        self.graph.add_edge(pandas_name, output_node)  # Connect output data node

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

    def to_pyspark(self):
        """Converts the DAG to PySpark code."""
        raise NotImplementedError
