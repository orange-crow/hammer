from typing import Dict, Literal

import networkx as nx

from .node import Node, Operation


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
        name: str,
        params: dict,
        input_nodes: Dict[str, str],
        output_node: str,
        *,
        target_ops: dict[str, str] = None,
    ):
        """Adds an operation node to the DAG and connects it to input and output data nodes.

        Args:
            name (str): Name of the data node.
            input_nodes (dict[str, str]): Input nodes for the operation.
            output_node (str): Output node name for the operation.
        """
        self.graph.add_node(
            name,
            type="op",
            obj=Node(
                name,
                node_type="op",
                operation=name,
                params=Operation(name, params, input_nodes, output_node, target_ops=target_ops),
            ),
        )
        for input_node in input_nodes:
            self.graph.add_edge(input_node, name)  # Connect input data nodes
        self.graph.add_edge(name, output_node)  # Connect output data node

    def visualize(self):
        """Visualizes the DAG using matplotlib."""
        import matplotlib.pyplot as plt

        pos = nx.spring_layout(self.graph)
        labels = {node: node for node in self.graph.nodes()}
        nx.draw(self.graph, pos, with_labels=True, labels=labels, node_size=2000, node_color="lightblue")
        plt.show()

    def __repr__(self):
        return f"PandasDAG(nodes={list(self.graph.nodes)}, edges={list(self.graph.edges)})"

    def to_pyspark(self):
        """Converts the DAG to PySpark code."""
        raise NotImplementedError
