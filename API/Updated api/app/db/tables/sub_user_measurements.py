from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer
from app.db.base import Base


class SubUserMeasurements(Base):
    __tablename__ = "sub_user_measurements"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    substation_id = Column(Integer, ForeignKey("public.substation.id"), nullable=False)
    energy_user_id = Column(Integer, ForeignKey("public.energy_user.id"), nullable=False)
    time = Column(DateTime, nullable=False)
    pwr_measurement_kwh = Column(Float, nullable=False)
