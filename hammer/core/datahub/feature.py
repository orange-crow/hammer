from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON

from ..base import Base


class FeatureMeta(Base):
    __tablename__ = "feature_meta"

    name = Column(String(256), nullable=False)
    version = Column(String(256), nullable=False)
    lineage_id = Column(String(36))
    parameters = Column(JSON, default=dict)
    start_event_datetime = Column(DateTime)
    end_event_datetime = Column(DateTime)
    status = Column(String(20), default="pending")

    __table_args__ = (UniqueConstraint("name", "version"),)

    def __repr__(self):
        return f"<FeatureMeta {self.name}:{self.version}>"
