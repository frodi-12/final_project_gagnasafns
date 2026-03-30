from sqlalchemy import Column, ForeignKey, Integer, String
from app.db.base import Base


class UserInfo(Base):
    __tablename__ = "user_info"
    __table_args__ = {"schema": "public"}

    kennitala = Column(String, ForeignKey("public.energy_user.kennitala"), primary_key=True)
    founding_year = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    owner = Column(String, nullable=False)
