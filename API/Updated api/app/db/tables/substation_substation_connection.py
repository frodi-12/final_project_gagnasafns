from sqlalchemy import Column, ForeignKey, Integer
from app.db.base import Base


class SubstationSubstationConnection(Base):
    __tablename__ = "substation_substation_connection"
    __table_args__ = {"schema": "public"}

    sending_station_id = Column(Integer, ForeignKey("public.substation.id"), primary_key=True)
    receiving_station_id = Column(Integer, ForeignKey("public.substation.id"), primary_key=True)
