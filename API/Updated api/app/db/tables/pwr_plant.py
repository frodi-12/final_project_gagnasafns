from sqlalchemy import Column, ForeignKey, Integer, String
from app.db.base import Base


class PowerPlant(Base):
    __tablename__ = "pwr_plant"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, ForeignKey("public.energy_unit.id"), primary_key=True)
    type = Column(String, nullable=False)
