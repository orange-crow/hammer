import json
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

    def __eq__(self, other: "DataNode") -> bool:
        if not isinstance(other, DataNode):
            raise TypeError("Comparison should only involve DataNode class object.")

        if self.name != other.name or self.data_type != other.data_type or self.source != other.source:
            return False
        return True

    def to_dict(self):
        """Convert the DataNode to a dictionary."""
        return {"name": self.name, "data_type": self.data_type, "source": self.source}

    def to_json(self) -> str:
        """Convert the DataNode to a JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, data_node_json: str) -> "DataNode":
        """Create a DataNode instance from a JSON string.

        Args:
            data_node_json (str): JSON string containing DataNode data

        Returns:
            DataNode: A new DataNode instance
        """
        data = json.loads(data_node_json)
        return cls(name=data["name"], data_type=data["data_type"], source=data["source"])
