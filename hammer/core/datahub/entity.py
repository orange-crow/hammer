from sqlalchemy import JSON, Column, String, UniqueConstraint

from ..base import Base


class Entity(Base):
    __tablename__ = "entity"

    name = Column(String(256), nullable=False, primary_key=True)
    join_keys = Column(JSON)  # List

    __table_args__ = (UniqueConstraint("name"),)

    def __repr__(self):
        return f"<Entity {self.name}:{self.join_keys}>"
