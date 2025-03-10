from sqlalchemy import JSON, Column, Integer, String, UniqueConstraint

from ..base import Base


class Entity(Base):
    __tablename__ = "entity"

    id = Column(Integer, primary_key=True)
    name = Column(String(256), nullable=False)
    join_keys = Column(JSON)  # List

    __table_args__ = (UniqueConstraint("name"),)

    def __repr__(self):
        return f"<Metadata {self.name}:{self.join_keys}>"
