from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

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
        self._async_engine = None
        self._session_factory = None

    @property
    def async_engine(self):
        if self._async_engine is None:
            self._async_engine = self.create_engine(self.url_with_db)
        return self._async_engine

    @property
    def session_factory(self):
        if self._session_factory is None:
            self._session_factory = sessionmaker(self.async_engine, class_=AsyncSession, expire_on_commit=False)
        return self._session_factory

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

    async def read(self, table_name: str, filters: dict = None):
        """
        异步读取数据库表中的数据。

        Args:
            table_name (str): 要查询的表名
            filters (dict, optional): 查询过滤条件，例如 {"name": "test", "version": "1.0"}

        Returns:
            list: 查询结果列表，每个元素是一个字典

        Raises:
            ValueError: 如果表名不存在于注册表中
        """
        # 获取表对象
        table = Base.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found in metadata registry")

        async with self.create_engine(self.url_with_db).connect() as conn:
            # 构建基本查询
            query = table.select()

            # 添加过滤条件
            if filters:
                for key, value in filters.items():
                    if key in table.columns:
                        query = query.where(table.columns[key] == value)

            # 执行查询
            result = await conn.execute(query)

            # 将结果转换为字典列表
            rows = [dict(row._mapping) for row in result]
            return rows

    async def write(self, table_name: str, data: dict | list[dict], upsert: bool = False):
        """
        异步向数据库表中写入数据。

        Args:
            table_name (str): 要写入的表名
            data (dict | list[dict]): 要写入的数据，可以是单个字典或字典列表
            upsert (bool): 如果为 True，当主键冲突时更新记录，否则插入新记录

        Returns:
            int: 受影响的行数

        Raises:
            ValueError: 如果表名不存在于注册表中或数据格式不正确
        """
        # 获取表对象
        table = Base.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found in metadata registry")

        # 确保数据是列表格式
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError("Data must be a dictionary or list of dictionaries")

        # 检查数据字段是否有效
        valid_columns = set(table.columns.keys())
        for item in data:
            invalid_columns = set(item.keys()) - valid_columns
            if invalid_columns:
                raise ValueError(f"Invalid columns found: {invalid_columns}")

        async with self.create_engine(self.url_with_db).begin() as conn:
            stmt = table.insert()
            if upsert:
                # 使用 PostgreSQL 的 ON CONFLICT 实现 upsert
                stmt = stmt.values(data).on_conflict_do_update(
                    index_elements=[table.primary_key.columns.keys()[0]],  # 使用主键
                    set_={
                        col: stmt.excluded[col] for col in data[0].keys() if col != table.primary_key.columns.keys()[0]
                    },
                )
            else:
                # 普通插入
                stmt = stmt.values(data)

            result = await conn.execute(stmt)
            return result.rowcount

    async def delete(self, table_name: str, filters: dict = None):
        """
        异步从数据库表中删除指定数据。

        Args:
            table_name (str): 要操作的表名
            filters (dict, optional): 删除条件，例如 {"name": "test", "version": "1.0"}
                                    如果为空，则删除表中所有数据

        Returns:
            int: 删除的行数

        Raises:
            ValueError: 如果表名不存在于注册表中
        """
        # 获取表对象
        table = Base.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found in metadata registry")

        async with self.create_engine(self.url_with_db).begin() as conn:
            # 构建删除语句
            stmt = table.delete()

            # 添加过滤条件
            if filters:
                for key, value in filters.items():
                    if key in table.columns:
                        stmt = stmt.where(table.columns[key] == value)
                    else:
                        raise ValueError(f"Invalid filter column: {key}")

            # 执行删除
            result = await conn.execute(stmt)
            return result.rowcount

    @asynccontextmanager
    async def get_db_session(self):
        async_session = self.session_factory()
        try:
            yield async_session
        finally:
            await async_session.close()
