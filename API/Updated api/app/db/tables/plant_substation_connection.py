from sqlalchemy import Column, Float, ForeignKey, Integer
from app.db.base import Base


class PlantSubstationConnection(Base):
    __tablename__ = "plant_substation_connection"
    __table_args__ = {"schema": "public"}

    plant_id = Column(Integer, ForeignKey("public.pwr_plant.id"), primary_key=True)
    substation_id = Column(Integer, ForeignKey("public.substation.id"), primary_key=True)
    distance = Column(Float, nullable=False)
