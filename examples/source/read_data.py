import fire

from hammer.dataset.source import BatchSource, ClickHouseConfig


def batchsource_read_data(database: str, table: str):
    source = BatchSource(
        name="clickhouse1", source_config=ClickHouseConfig(name="clickhouse1", database=database, table=table)
    )
    df = source.to_pandas()
    print(df)


def batchsource_read_data2(database: str, table: str):
    source = BatchSource(
        name="clickhouse1",
        source_config=ClickHouseConfig(
            name="clickhouse1",
            database=database,
            table=table,
            target_fields=["item_idnt", "item_desc", "time_to_market", "cb_level3_category_name"],
        ),
    )
    print(source.source_config.target_fields)
    df = source.to_pandas()
    print(df)


if __name__ == "__main__":
    fire.Fire()
