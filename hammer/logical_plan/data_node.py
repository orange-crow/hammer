from typing import Literal


class DataNode(object):
    def __init__(
        self,
        name: str,
        *,
        data_type: Literal["io", "memory"] = None,
        source: str = None,
    ):
        """Represents a data node in the DAG.

        Args:
            name (str): Name of the data node, it is name of variable.
            node_type (str): Type of the node, either "data" or "op" (operation).
            data_type (str): Type of the data node, either "io" (data IO) or "memory" (in-memory data).
            source (str, optional): Data source (file path or database connection), only applicable for IO nodes.
        """
        self.name = name
        self.data_type = data_type or ""
        self.source = source or ""

    def __repr__(self):
        return f"DataNode(name={self.name}, data_type={self.data_type}, source={self.source})"
