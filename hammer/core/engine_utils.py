from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


class AsyncEngineBase(object):
    """数据库的异步操作: database和表格的创建, 表格条目的增删改查."""

    url: str

    def __init__(self, url: str):
        self.url = url

    def create_engine(self, echo=True, future=True) -> AsyncEngine:
        return create_async_engine(self.url, echo=echo, future=future)

    async def create_database(self, db_config: dict, db_name: str, database_type: str = "postgres"):
        raise NotImplementedError

    async def create_table(
        self,
        table_name: str,
    ):
        raise NotImplementedError
