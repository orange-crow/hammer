from .base import DataSource


class BatchSource(DataSource):
    @property
    def fetch_data_sql(self) -> str:
        if self._fetch_data_sql is None:
            sql = f"""
            select * 
            from {self.database}.{self.table_name}
            """

            if self.field_mapping:
                sql = f"""
                select {','.join([f'{k} as {v}' for k, v in self.field_mapping.items()])}
                from {self.database}.{self.table_name}
                """

            if self.filter_conditions:
                sql += f"\nwhere {self.filter_conditions}"

        return self._fetch_data_sql
