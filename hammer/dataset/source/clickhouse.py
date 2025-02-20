from typing import Callable

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from loguru import logger

from ...config import CONF
from ...schema import CH2PANDAS, Datatype
from . import ClientBase, SourceConfig


# ref: https://github.com/ClickHouse/clickhouse-connect/blob/main/examples/read_perf.py
class ClickHouseClient(ClientBase):
    def __init__(self, name: str):
        ch_config = CONF.infra.get("clickhouse").get(name)
        assert ch_config is not None, ch_config
        super().__init__(**ch_config)

    def connect(self) -> Client:
        return clickhouse_connect.get_client(
            username=self._user, password=self._password, host=self._host, port=self._port, send_receive_timeout=1200
        )

    def _read(self, query_or_file_path: str, existing_connection: Client = None, *args, **kwargs) -> pd.DataFrame:
        show_log = kwargs.get("show_log", True)
        with existing_connection or self.connect() as connection:
            if show_log:
                logger.info("Got a connection.")
                logger.info(f"Fetch data from clickhouse database: \n {query_or_file_path}")
            df = connection.query_df(query_or_file_path)
            if existing_connection is None:
                connection.close()
        return df


class ClickHouseConfig(SourceConfig):
    @property
    def client(self) -> ClientBase:
        if self._client is None:
            self._client = ClickHouseClient(self.name)
        return self._client

    def get_schema(self) -> pd.DataFrame:
        query = f"""
        SELECT name, type as dtype
        FROM system.columns
        WHERE table = '{self.table}' AND database = '{self.database}'
        ORDER BY position
        """
        schema_df = self.client.read(query)
        return schema_df

    @staticmethod
    def to_pandas_dtype(source_dtype: str) -> Callable[[str], Datatype]:
        if source_dtype.startswith("Nullable"):
            source_dtype = source_dtype[:-1].split("(")[1]
        return CH2PANDAS.get(source_dtype, str)

    def read(self):
        return self.select_data()
