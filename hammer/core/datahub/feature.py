from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON

from ..base import Base


class Feature(Base):
    __tablename__ = "feature"

    name = Column(String(256), nullable=False, primary_key=True)
    version = Column(String(256), nullable=False, primary_key=True)
    source = Column(JSON, nullable=False)  # name, version, table_name
    entity = Column(JSON, nullable=False)  # name, version
    event_timestamp_field = Column(String(256))
    start_event_datetime = Column(String(64), default="", primary_key=True)
    end_event_datetime = Column(String(64), default="", primary_key=True)
    ttl = Column(Integer(), default=0)
    schema = Column(JSON, nullable=False)
    sink = Column(JSON, nullable=False)  # hive上 database, table
    transform = Column(String(1024), nullable=False)
    description = Column(String(256), default="")
    owner = Column(String(64), default="")
    status = Column(String(20), default="todo", nullable=False)

    __table_args__ = (UniqueConstraint("name", "version", "start_event_datetime", "end_event_datetime"),)

    def __repr__(self):
        return f"<Feature {self.name}:{self.version}>"
