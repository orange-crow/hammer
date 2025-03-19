from typing import List, Literal

import wrapt

from hammer.dataset.entity import Entity
from hammer.dataset.source import BatchSource


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

    @wrapt.decorator
    def __call__(self, wrapped, instance, args, kwargs):
        # 根据 start_event_datetime, end_event_datetime 过滤需要进行计算的数据
        source = [sr.data if hasattr(sr, "data") else sr for sr in self.source]
        source = [
            sr[
                (sr[self.event_timestamp_field] >= self.start_event_datetime)
                and (sr[self.event_timestamp_field] <= self.end_event_datetime)
            ]
            for sr in source
        ]

        result = wrapped(*source, **kwargs)

        # 根据 start_event_datetime, end_event_datetime 过滤计算结果
        result = result[
            (result[self.event_timestamp_field] >= self.start_event_datetime)
            and (result[self.event_timestamp_field] <= self.end_event_datetime)
        ]
        return result

    def compute(
        self,
        mode: Literal["pandas", "pyspark"] = "pandas",
    ):
        """选择指定计算引擎进行计算"""
        pass
