from typing import List

from hammer.dataset.table import PandasTable

from .base import DataSource


class BatchSource(DataSource):
    _schema: PandasTable = None
    _nulls_count: PandasTable = None

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

    def head(self, n=5) -> PandasTable:
        sql = (
            f"select * from {self.database}.{self.table_name}"
            if self.infra_type != "postgres"
            else f"select * from {self.database}.public.{self.table_name}"
        )
        if self.infra_type == "oracle":
            sql += f"\nfetch rows only {n}"
        else:
            sql += f"\nlimit {n}"
        return self.client.read(sql)

    def _get_postgres_schema(self):
        sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = current_database()
        AND table_name = '{self.table_name}';
        """
        return self.client.read(sql)

    def _get_clickhouse_schema(self):
        sql = f"""
        SELECT name AS column_name, type AS data_type
        FROM system.columns
        WHERE database = currentDatabase()  -- 当前数据库
        AND table = '{self.table_name}';
        """
        return self.client.read(sql)

    def _get_oracle_schema(self):
        sql = f"""
        SELECT  column_name, data_type
        FROM all_tab_columns
        WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')
        AND table_name = '{self.table_name}';
        """
        return self.client.read(sql)

    @property
    def schema(self) -> PandasTable:
        if self._schema is None:
            if self.infra_type == "postgres":
                self._schema = self._get_postgres_schema().copy()
            elif self.infra_type == "clickhouse":
                self._schema = self._get_clickhouse_schema().copy()
            elif self.infra_type == "oracle":
                self._schema = self._get_oracle_schema().copy()
            else:
                raise ValueError
        return self._schema

    @property
    def columns(self) -> List:
        return self.schema["column_name"].tolist()

    def _get_postgres_isnull(self) -> PandasTable:
        counts = [f"COUNT(*) FILTER (WHERE {column} IS NULL) AS {column}" for column in self.columns]
        sql = f"""
        SELECT
            {',\n'.join(counts)},\nCOUNT(*) as nrows
        FROM {self.database}.public.{self.table_name};
        """
        return self.client.read(sql)

    def _get_clickhouse_isnull(self) -> PandasTable:
        counts = [f"COUNTIf({column} IS NULL) AS {column}" for column in self.columns]
        sql = f"""
        SELECT
            {',\n'.join(counts)},\nCOUNT(*) as nrows
        FROM {self.database}.{self.table_name};
        """
        return self.client.read(sql)

    def _get_oracle_isnull(self) -> PandasTable:
        counts = [f"COUNT(CASE WHEN {column} IS NULL THEN 1 END) AS {column}" for column in self.columns]
        sql = f"""
        SELECT
            {',\n'.join(counts)},\nCOUNT(*) as nrows
        FROM {self.database}.{self.table_name};
        """
        return self.client.read(sql)

    @property
    def nulls_count(self) -> PandasTable:
        if self._nulls_count is None:
            if self.infra_type == "postgres":
                self._nulls_count = self._get_postgres_isnull().copy()
            elif self.infra_type == "clickhouse":
                self._nulls_count = self._get_clickhouse_isnull().copy()
            elif self.infra_type == "oracle":
                self._nulls_count = self._get_oracle_isnull().copy()
            else:
                raise ValueError
        return self._nulls_count.T

    def count_nulls(self) -> PandasTable:
        return self.nulls_count
