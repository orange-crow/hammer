from typing import List, Literal

import wrapt

from hammer.dataset.entity import Entity
from hammer.dataset.source import BatchSource
from hammer.dataset.table import PandasTable


class Feature(object):
    def __init__(
        self,
        source: List[BatchSource, "Feature"],
        entity: Entity,
        event_timestamp_field: str = None,
        start_event_datetime: str = None,
        end_event_datetime: str = None,
        ttl: int = 0,
        schema: str = None,
        sink: str = None,
        *,
        description: str = None,
        owner: str = None,
    ):
        self.source = source
        self.sink = sink
        self.entity = entity
        self.start_event_datetime = start_event_datetime
        self.end_event_datetime = end_event_datetime
        self.ttl = ttl
        self.event_timestamp_field = event_timestamp_field
        self.schema = schema
        self.description = description
        self.owner = owner
        self._filted_source = None

    @property
    def filted_source(self):
        if self._filted_source is None:
            self._filted_source = self.process_source()
        return self._filted_source

    def process_source(self):
        source = [sr.data if hasattr(sr, "data") else sr for sr in self.source]
        source = [
            sr[
                (sr[self.event_timestamp_field] >= self.start_event_datetime)
                and (sr[self.event_timestamp_field] <= self.end_event_datetime)
            ]
            for sr in source
        ]
        return source

    def process_result(self, result: PandasTable):
        return result[
            (result[self.event_timestamp_field] >= self.start_event_datetime)
            and (result[self.event_timestamp_field] <= self.end_event_datetime)
        ]

    @wrapt.decorator
    def __call__(self, wrapped, instance, args, kwargs):
        # 根据 start_event_datetime, end_event_datetime 过滤需要进行计算的数据
        source = self.process_source()

        result = wrapped(*source, **kwargs)

        # 根据 start_event_datetime, end_event_datetime 过滤计算结果
        result = self.process_result(result)
        return result

    def compute(self, mode: Literal["pandas", "pyspark"] = "pandas"):
        """选择指定计算引擎进行计算"""
        # sql + udf 的方式更加适合hammer的理念：简单易用；
        # udf 可以覆盖复杂特征计算逻辑；
        # sql 可以覆盖简单特征计算逻辑；
        # 好处是：可以最大程度复用spark对不同类型数据源的支持能力；
        # 而相比于SQL来说，使用 pandas api 作为 DSL, 仍然不够简单易用；
        if mode == "pandas":
            return self.__call__(*self.filted_source)
        elif mode == "pyspark":
            pass
        else:
            raise ValueError(f"Unsupported mode: {mode}")
