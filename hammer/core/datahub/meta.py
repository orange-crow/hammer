from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON

from ..base import Base


class Metadata(Base):
    __tablename__ = "hammer_meta"

    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    version = Column(String(256), nullable=False)
    lineage_id = Column(String(36))
    parameters = Column(JSON, default=dict)
    status = Column(String(20), default="pending")

    __table_args__ = (UniqueConstraint("name", "version"),)

    def __repr__(self):
        return f"<Metadata {self.name}:{self.version}>"
