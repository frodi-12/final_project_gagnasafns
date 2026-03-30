from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, PrimaryKeyConstraint
from app.db.base import Base


class SubUserMeasurements(Base):
    __tablename__ = "sub_user_measurements"
    __table_args__ = (
        PrimaryKeyConstraint("substation_id", "energy_user_id", "time", name="pk_sub_user_measurements"),
        {"schema": "public"},
    )

    substation_id = Column(Integer, ForeignKey("public.substation.id"), nullable=False)
    energy_user_id = Column(Integer, ForeignKey("public.energy_user.id"), nullable=False)
    time = Column(DateTime, nullable=False)
    sent_pwr = Column(Float, nullable=False)
    received_pwr = Column(Float, nullable=False)
