from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from app.db.base import Base


class PlantSubMeasurements(Base):
    __tablename__ = "plant_sub_measurements"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    plant_id = Column(Integer, ForeignKey("public.pwr_plant.id"), nullable=False)
    substation_id = Column(Integer, ForeignKey("public.substation.id"), nullable=False)
    time = Column(DateTime, nullable=False)
    pwr_measurement_kwh = Column(Float, nullable=False)
    type = Column(String, nullable=False)
