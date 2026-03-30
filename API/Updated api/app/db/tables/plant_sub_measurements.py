from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, PrimaryKeyConstraint
from app.db.base import Base


class PlantSubMeasurements(Base):
    __tablename__ = "plant_sub_measurements"
    __table_args__ = (
        PrimaryKeyConstraint("substation_id", "plant_id", "time", name="pk_plant_sub_measurements"),
        {"schema": "public"},
    )

    substation_id = Column(Integer, ForeignKey("public.substation.id"), nullable=False)
    plant_id = Column(Integer, ForeignKey("public.pwr_plant.id"), nullable=False)
    time = Column(DateTime, nullable=False)
    generated_pwr = Column(Float, nullable=False)
    received_pwr = Column(Float, nullable=False)
