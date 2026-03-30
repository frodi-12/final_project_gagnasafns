from sqlalchemy import Column, Date, Float, Integer, String
from app.db.base import Base


class EnergyUnit(Base):
    __tablename__ = "energy_unit"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    owner = Column(String, nullable=False)
    construction_date = Column(Date, nullable=False)
    x_cords = Column(Float, nullable=False)
    y_cords = Column(Float, nullable=False)
