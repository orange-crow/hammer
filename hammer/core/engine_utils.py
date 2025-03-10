from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .base import Base


class AsyncEngineBase(object):
    """数据库的异步操作: database和表格的创建, 表格条目的增删改查."""

    url: str

    def __init__(
        self,
        url: str,
        *,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None,
    ):
        self.url = url
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

    def create_engine(self, url, *, echo=True, future=True) -> AsyncEngine:
        return create_async_engine(url, echo=echo, future=future)

    async def create_database(self):
        raise NotImplementedError

    async def create_table(self, table_name: str):
        raise NotImplementedError


class Postgres(AsyncEngineBase):
    def __init__(self, *, host: str, port: int, user: str, password: str, database: str):
        url_prefix = f"postgresql+asyncpg://{user}:{password}@{host}:{port}"
        url_with_db = f"{url_prefix}/{database}"
        url_default = f"{url_prefix}/postgres"
        super().__init__(url_default, host=host, port=port, user=user, password=password, database=database)
        self.url_default = url_default
        self.url_with_db = url_with_db

    async def create_database(self, database: str):
        self._database = database
        try:
            async with self.create_engine(self.url_default).connect() as conn:
                # 确保在非事务上下文中执行CREATE DATABASE
                await conn.execution_options(isolation_level="AUTOCOMMIT")
                result = await conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{database}'"))
                # 检查并创建数据库
                if not result.scalar():
                    await conn.execute(text(f"CREATE DATABASE {database}"))
                    print(f"Database '{database}' created successfully.")
                else:
                    print(f"Database '{database}' already exists.")
        except Exception as e:
            print(f"Error creating database: {str(e)}")
            raise

    async def create_table(self, table_name: str = None):
        async with self.create_engine(self.url_with_db).begin() as conn:
            table = Base.metadata.tables.get(table_name)
            if table is not None:
                await conn.run_sync(lambda conn: table.create(conn, checkfirst=True))
                print(f"Table '{table_name}' created successfully.")
            else:
                print(f"Table '{table_name}' not in registry.")
