from .base import DataSource


class BatchSource(DataSource):
    @property
    def fetch_data_sql(self) -> str:
        if self._fetch_data_sql is None:
            self._fetch_data_sql = "select *"

            if self.field_mapping:
                self._fetch_data_sql = f"select {','.join([f'{k} as {v}' for k, v in self.field_mapping.items()])}"

            self._fetch_data_sql += (
                f"\nfrom {self.database}.{self.table_name}"
                if self.infra_type != "postgres"
                else f"\nfrom {self.database}.public.{self.table_name}"
            )

            if self.filter_conditions:
                self._fetch_data_sql += f"\nwhere {self.filter_conditions}"

        return self._fetch_data_sql
