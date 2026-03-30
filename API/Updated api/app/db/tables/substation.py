from sqlalchemy import Column, ForeignKey, Integer
from app.db.base import Base


class Substation(Base):
    __tablename__ = "substation"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, ForeignKey("public.energy_unit.id"), primary_key=True)
