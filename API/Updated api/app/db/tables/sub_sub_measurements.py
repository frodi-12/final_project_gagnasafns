from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, PrimaryKeyConstraint
from app.db.base import Base


class SubSubMeasurements(Base):
    __tablename__ = "sub_sub_measurements"
    __table_args__ = (
        PrimaryKeyConstraint("sending_station_id", "receiving_station_id", "time", name="pk_sub_sub_measurements"),
        {"schema": "public"},
    )

    sending_station_id = Column(Integer, ForeignKey("public.substation.id"), nullable=False)
    receiving_station_id = Column(Integer, ForeignKey("public.substation.id"), nullable=False)
    time = Column(DateTime, nullable=False)
    sent_pwr = Column(Float, nullable=False)
    received_pwr = Column(Float, nullable=False)
