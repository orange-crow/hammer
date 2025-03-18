from typing import Literal

import wrapt

from hammer.dataset.entity import Entity
from hammer.dataset.source import BatchSource


class Feature(object):
    def __init__(
        self,
        source: BatchSource,
        entity: Entity,
        event_timestamp_field: str = None,
        start_event_datetime: str = None,
        end_event_datetime: str = None,
        mode: Literal["pandas", "pyspark"] = "pandas",
        ttl: int = 0,
        schema: str = None,
        sink: str = None,
        description: str = None,
        owner: str = None,
    ):
        self.source = source
        self.sink = sink
        self.entity = entity
        self.start_event_datetime = start_event_datetime
        self.end_event_datetime = end_event_datetime
        self.mode = mode
        self.ttl = ttl
        self.event_timestamp_field = event_timestamp_field
        self.schema = schema
        self.description = description
        self.owner = owner

    @wrapt.decorator
    def __call__(self, wrapped, instance, args, kwargs):
        # 处理 start_event_datetime, end_event_datetime
        result = wrapped(*args, **kwargs)
        # 处理 start_event_datetime, end_event_datetime
        return result

    def compute(self):
        pass
