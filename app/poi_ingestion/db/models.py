# app/models.py

from sqlalchemy import (
    Column,
    String,
    Text,
    Float,
    Integer,
    BigInteger,
    ForeignKey,
    DateTime,
    JSON,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class POI(Base):
    __tablename__ = "pois"

    poi_uuid = Column(String, primary_key=True)
    country_code = Column(String(2), index=True)
    title = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    raw_json = Column(JSON)
    payload_hash = Column(Text)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now())

    connections = relationship(
        "Connection",
        back_populates="poi",
        cascade="all, delete-orphan",
    )


class Connection(Base):
    __tablename__ = "connections"

    id = Column(BigInteger, primary_key=True)
    poi_uuid = Column(String, ForeignKey("pois.poi_uuid", ondelete="CASCADE"), index=True)

    connection_id = Column(BigInteger)
    connection_type = Column(Text)
    power_kw = Column(Float)
    voltage = Column(Integer)
    amperage = Column(Integer)
    raw_json = Column(JSON)
    payload_hash = Column(Text)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now())

    poi = relationship("POI", back_populates="connections")
