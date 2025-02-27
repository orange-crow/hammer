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
        self.graph.add_node(self.rename_node(name), type="data", obj=DataNode(name, data_type=data_type, source=source))

    def add_operation_node(
        self,
        node_name: str,  # show in dag
        pandas_name: str,
        pandas_positional_args: List = None,
        pandas_keyword_args: Dict[str, Any] = None,
        input_nodes: Union[str, List] = None,
        *,
        target_ops: dict[str, str] = None,
    ):
        self.graph.add_node(
            self.rename_node(node_name),
            type="op",
            obj=create_ops(pandas_name, pandas_positional_args, pandas_keyword_args, target_ops=target_ops),
        )

        if input_nodes:
            if isinstance(input_nodes, str):
                input_nodes = [input_nodes]

            if isinstance(input_nodes, list):
                for input_node in input_nodes:
                    self.graph.add_edge(self.get_last_node(input_node), self.get_last_node(node_name))
            else:
                raise ValueError

    def add_edge(self, from_node: str, to_node: str):
        self.graph.add_edge(self.get_last_node(from_node), self.get_last_node(to_node))

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
        """获取节点的输入节点"""
        if not self.graph.has_node(node_name):
            return []
        return list(self.graph.predecessors(node_name))

    def get_out_degree(self, node_name) -> int:
        """获取节点的输出节点个数"""
        return self.graph.out_degree(node_name)

    def rename_node(self, node_name: str) -> str:
        """重新命名节点, 防止因为重名节点造成dag出现环"""
        # 重名名的条件: 节点已经存在，而且 (有输入节点，或者， 有输出节点）
        if self.has_node(node_name) and self.graph.nodes.get(node_name):
            node_type = self[node_name]["type"]
            last_node = self.get_last_node(node_name)
            # 如果node_name 最新重名节点 last_node 还没有使用的话, 就直接返回 last_node
            if (node_type == "data" and len(self.get_input_nodes(last_node)) == 0) or (
                node_type == "op" and self.get_out_degree(last_node) == 0
            ):
                return last_node
            return f"{node_name}_hammer_tag_{len(self.get_duplicated_nodes(node_name))}"
        return node_name

    def get_duplicated_nodes(self, node_name: str) -> List[str]:
        """获取 node_name 同名节点"""
        if self.has_node(node_name):
            nodes = list(set(self.node_startswith(f"{node_name}_hammer_tag_")))
            return sorted([node_name, *nodes])
        return [node_name]

    def get_last_node(self, node_name: str) -> str:
        """获取 node_name 同名节点中最新节点名, 若不存在则返回查询的节点名"""
        return self.get_duplicated_nodes(node_name)[-1]

    def get_second_to_last_node(self, node_name: str) -> str:
        """获取 node_name 同名节点中倒数第二节点名, 若不存在则返回查询的节点名"""
        nodes = self.get_duplicated_nodes(node_name)
        return nodes[-2] if len(nodes) > 1 else node_name

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
