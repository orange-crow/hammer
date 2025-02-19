import fire

from hammer.dataset.source import ClickHouseClient


def read_ch_data(sql: str):
    ch = ClickHouseClient()
    df = ch.read(sql)
    print(df)


if __name__ == "__main__":
    fire.Fire()
