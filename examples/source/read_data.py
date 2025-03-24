import fire

from hammer.config import CONF
from hammer.table.source.batch import BatchSource
from hammer.utils.client.clickhouse import ClickHouseClient


def read_version():
    chconf = CONF.infra.get("clickhouse").get("clickhouse1")
    client = ClickHouseClient(**chconf)
    df = client.read("SELECT name, type as dtype FROM system.columns")
    print(df)

    # 上下文管理器示例
    with client:
        df = client.read("SELECT name, type as dtype FROM system.columns")
        print(f"ClickHouse Version: {df}")


def read_batch():
    source = BatchSource("baseline", "0.1.0", "test_format1", "postgres", "baseline")
    source.data.info()
    print(source.data)


if __name__ == "__main__":
    fire.Fire()
