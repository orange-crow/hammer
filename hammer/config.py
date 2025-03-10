from pathlib import Path
from typing import Dict, List

from .schema import TableSchema
from .utils import yamler


class Config(object):
    def __init__(self, project_dir: str = None):
        self.project_dir = project_dir or Path(__file__).parents[1]
        self.configs: dict = yamler.read(str(self.project_dir / "config.yaml"))
        self.configs2: dict = yamler.read(str(self.project_dir / "config2.yaml"))
        self._infra = None

    @property
    def raw_data_schema(self) -> List[Dict]:
        schema = self.configs.get("data_schema")
        return TableSchema.from_list(schema)

    @property
    def infra(self) -> Dict:
        if self._infra is None:
            infra = self.configs.get("infra")
            infra2 = self.configs2.get("infra")
            infra.update(infra2)
            current_env = infra.get("current_env")
            current_infra = infra.get(current_env)
            assert current_infra is not None
            # convert list to map, the name is key
            new_format_infra = {}
            for infra_type, infra_list in current_infra.items():
                new_format_infra[infra_type] = {}
                for infra_cong in infra_list:
                    name = infra_cong.pop("name")
                    new_format_infra[infra_type][name] = infra_cong
            self._infra = new_format_infra
        return self._infra


CONF = Config()
