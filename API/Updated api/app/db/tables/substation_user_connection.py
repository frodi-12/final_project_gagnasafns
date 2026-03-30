from sqlalchemy import Column, ForeignKey, Integer
from app.db.base import Base


class SubstationUserConnection(Base):
    __tablename__ = "substation_user_connection"
    __table_args__ = {"schema": "public"}

    substation_id = Column(Integer, ForeignKey("public.substation.id"), primary_key=True)
    energy_user_id = Column(Integer, ForeignKey("public.energy_user.id"), primary_key=True)
