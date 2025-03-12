from sqlalchemy import JSON, Column, String, UniqueConstraint

from ..base import Base


class Source(Base):
    __tablename__ = "source"

    name = Column(String(256), nullable=False, primary_key=True)
    version = Column(String(256), nullable=False, primary_key=True)
    table_name = Column(String(256), nullable=False)
    infra_type = Column(String(256), nullable=False)
    field_mapping = Column(JSON, default=dict)
    owner = Column(String(256))
    description = Column(String(256))
    config = Column(JSON, default=dict)  # 数据源的配置信息

    __table_args__ = (UniqueConstraint("name", "version"),)

    def __repr__(self):
        return f"<Source {self.name}:{self.version}:{self.table_name}>"
