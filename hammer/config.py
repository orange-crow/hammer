from pathlib import Path
from typing import Dict, List

from .schema import TableSchema
from .utils import yamler


class Config(object):
    def __init__(self, project_dir: str = None):
        self.project_dir = project_dir or Path(__file__).parents[1]
        self.configs: dict = yamler.read(str(self.project_dir / "config.yaml"))
        self.configs2: dict = yamler.read(str(self.project_dir / "config2.yaml"))

    @property
    def raw_data_schema(self) -> List[Dict]:
        schema = self.configs.get("data_schema")
        return TableSchema.from_list(schema)

    @property
    def infra(self) -> Dict:
        infra = self.configs.get("infra")
        infra2 = self.configs2.get("infra")
        infra.update(infra2)
        return infra


CONF = Config()
