from dataclasses import field
from typing import Dict, Literal


class Node(object):
    def __init__(
        self,
        name: str,
        *,
        node_type: Literal["data", "op"] = None,
        # data node
        data_type: Literal["io", "memory"] = None,
        source: str = None,
        # operation node
        operation: str = None,
        params: dict = None,
        input_nodes: Dict[str, "Node"] = field(default_factory=dict),
        output_node: "Node" = None,
    ):
        """Represents a data node in the DAG.

        Args:
            name (str): Name of the data node, it is name of variable.
            node_type (str): Type of the node, either "data" or "op" (operation).
            data_type (str): Type of the data node, either "io" (data IO) or "memory" (in-memory data).
            source (str, optional): Data source (file path or database connection), only applicable for IO nodes.
            operation (str): Type of operation (e.g., groupby, merge, filter, apply, join, etc.).
            input_nodes (Dict[str, "Node"], optional): Parameters for the operation.
            output_node(Node, optional): output node for the operation.
        """
        self.name = name
        self.node_type = node_type or ""
        self.data_type = data_type or ""
        self.source = source or ""
        self.operation = operation or ""
        self.params = params or {}
        self.input_nodes = input_nodes or {}
        self.output_node = output_node

    def __repr__(self):
        if self.node_type == "data":
            return f"DataNode(name={self.name}, node_type=data, data_type={self.data_type}, source={self.source})"
        return f"DataNode(name={self.name}, node_type=op, params={self.params}, input_nodes={self.input_nodes}, output_node={self.output_node})"
