from sqlalchemy import Column, Float, Integer, String
from app.db.base import Base


class EnergyUser(Base):
    __tablename__ = "energy_user"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    kennitala = Column(String, unique=True, nullable=False)
    x_cords = Column(Float, nullable=False)
    y_cords = Column(Float, nullable=False)
