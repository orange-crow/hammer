import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from loguru import logger

from ...config import CONF
from .base import ClientBase


# ref: https://github.com/ClickHouse/clickhouse-connect/blob/main/examples/read_perf.py
class ClickHouseClient(ClientBase):
    def __init__(self):
        ch_config = CONF.infra.get("clickhouse")
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
