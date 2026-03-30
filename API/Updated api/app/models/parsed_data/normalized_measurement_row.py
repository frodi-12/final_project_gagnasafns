from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NormalizedMeasurementRow:
    plant_name: str
    measurement_type: str
    sender_name: str
    timestamp: datetime
    value_kwh: float
    customer_name: Optional[str]
