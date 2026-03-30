import csv
from datetime import datetime
from io import StringIO
from typing import List

from app.models.parsed_data.normalized_measurement_row import (
    NormalizedMeasurementRow,
)


def parse_measurements_csv(raw_text: str) -> List[NormalizedMeasurementRow]:
    rows: List[NormalizedMeasurementRow] = []
    reader = csv.DictReader(StringIO(raw_text))

    for csv_row in reader:
        try:
            plant_name = (csv_row.get("eining_heiti") or "").strip()
            measurement_type = (csv_row.get("tegund_maelingar") or "").strip()
            sender_name = (csv_row.get("sendandi_maelingar") or "").strip()
            timestamp = datetime.fromisoformat(
                (csv_row.get("timi") or "").strip()
            )
            value = float((csv_row.get("gildi_kwh") or "0").strip())
            customer_name = (csv_row.get("notandi_heiti") or "").strip() or None

            if not plant_name or not measurement_type or not sender_name:
                continue

            rows.append(
                NormalizedMeasurementRow(
                    plant_name=plant_name,
                    measurement_type=measurement_type,
                    sender_name=sender_name,
                    timestamp=timestamp,
                    value_kwh=value,
                    customer_name=customer_name,
                )
            )
        except Exception:
            continue

    return rows
