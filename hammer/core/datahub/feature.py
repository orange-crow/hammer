from sqlalchemy import Column, DateTime, Interval, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON

from ..base import Base


class Feature(Base):
    __tablename__ = "feature"

    name = Column(String(256), nullable=False)
    version = Column(String(256), nullable=False)
    source = Column(JSON)
    entity = Column(JSON)
    event_timestamp_field = Column(String(256))
    start_event_datetime = Column(DateTime)
    end_event_datetime = Column(DateTime)
    ttl = Column(Interval(), nullable=False, default=0)
    schema = Column(JSON)
    sink = Column(JSON)
    transform = Column(String(1024))
    description = Column(String(256))
    owner = Column(String(64))
    status = Column(String(20), default="pending")

    __table_args__ = (UniqueConstraint("name", "version"),)

    def __repr__(self):
        return f"<Feature {self.name}:{self.version}>"
