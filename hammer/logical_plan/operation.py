from typing import List

from . import Node


class Operation(object):
    def __init__(self, name: str, params: dict, input_nodes: List[Node], output_node: Node):
        self.name = name  # name in dag.
        self.params = params
        self.input_nodes = input_nodes
        self.output_node = output_node

    def __repr__(self):
        return f"{self.__class__.__name__}(params={self.params})"

    def to_pyspark(self) -> str:
        raise NotImplementedError

    def to_dask(self) -> str:
        raise NotImplementedError
