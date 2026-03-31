from datetime import datetime
from typing import Dict, List, Tuple

from fastapi import HTTPException, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.tables.energy_unit import EnergyUnit
from app.db.tables.energy_user import EnergyUser
from app.db.tables.pwr_plant import PowerPlant
from app.db.tables.plant_sub_measurements import PlantSubMeasurements
from app.db.tables.plant_substation_connection import PlantSubstationConnection
from app.db.tables.sub_user_measurements import SubUserMeasurements
from app.db.tables.substation import Substation
from app.db.tables.substation_user_connection import SubstationUserConnection
from app.db.tables.user_info import UserInfo
from app.models.monthly_company_usage_model import MonthlyCompanyUsageModel
from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel
from app.models.monthly_plant_loss_ratios import MonthlyPlantLossRatiosModel
from app.models.parsed_data.normalized_measurement_row import (
    NormalizedMeasurementRow,
)
from app.parsers.parse_measurements_csv import parse_measurements_csv
from app.utils.validate_file_type import validate_file_type


def _load_names(db: Session) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
    """Sækjum einföld kort yfir nöfn -> id."""
    plant_names: Dict[str, int] = {}
    for plant_id, name in (
        db.query(PowerPlant.id, EnergyUnit.name)
        .join(EnergyUnit, PowerPlant.id == EnergyUnit.id)
        .all()
    ):
        plant_names[name] = plant_id

    substation_names: Dict[str, int] = {}
    for sub_id, name in (
        db.query(Substation.id, EnergyUnit.name)
        .join(EnergyUnit, Substation.id == EnergyUnit.id)
        .all()
    ):
        substation_names[name] = sub_id

    customer_names: Dict[str, int] = {}
    for user, info in (
        db.query(EnergyUser, UserInfo).join(
            UserInfo, EnergyUser.kennitala == UserInfo.kennitala
        )
    ):
        customer_names[info.name] = user.id

    return plant_names, substation_names, customer_names


def _load_links(
    db: Session,
) -> Tuple[Dict[int, List[int]], List[Tuple[int, int]], List[Tuple[int, int]]]:
    """Lesum upp tengitöflur á einfaldan hátt."""
    plant_to_subs: Dict[int, List[int]] = {}
    plant_pairs: List[Tuple[int, int]] = []
    for link in db.query(PlantSubstationConnection).all():
        plant_pairs.append((link.plant_id, link.substation_id))
        plant_to_subs.setdefault(link.plant_id, []).append(link.substation_id)

    sub_user_pairs: List[Tuple[int, int]] = []
    for link in db.query(SubstationUserConnection).all():
        sub_user_pairs.append((link.substation_id, link.energy_user_id))

    return plant_to_subs, plant_pairs, sub_user_pairs


def _find_substation_id(
    plant_id: int,
    sender_name: str,
    substation_names: Dict[str, int],
    plant_pairs: List[Tuple[int, int]],
    plant_to_subs: Dict[int, List[int]],
    force_sender: bool,
) -> int:
    """Veljum einfaldlega fyrsta substation sem passar."""
    sub_id = substation_names.get(sender_name)
    if sub_id and (plant_id, sub_id) in plant_pairs:
        return sub_id

    if force_sender:
        raise HTTPException(
            status_code=400,
            detail=f"Tengivirki '{sender_name}' fannst ekki fyrir virkjun {plant_id}",
        )

    defaults = plant_to_subs.get(plant_id)
    if defaults:
        return defaults[0]

    raise HTTPException(
        status_code=400,
        detail=f"Vantar tengingu milli virkjunar {plant_id} og nokkurs tengivirkis",
    )


def _group_plant_rows(
    rows: List[NormalizedMeasurementRow],
    plant_names: Dict[str, int],
    substation_names: Dict[str, int],
    plant_pairs: List[Tuple[int, int]],
    plant_to_subs: Dict[int, List[int]],
) -> Dict[Tuple[int, int, datetime], Dict[str, float]]:
    """Safna framleiðslu og innmötun saman."""
    result: Dict[Tuple[int, int, datetime], Dict[str, float]] = {}

    for row in rows:
        plant_id = plant_names.get(row.plant_name)
        if plant_id is None:
            raise HTTPException(
                status_code=400,
                detail=f"Virkjun '{row.plant_name}' fannst ekki",
            )

        key = (plant_id, 0, row.timestamp)
        measure = row.measurement_type.lower()

        if "fram" in measure:
            sub_id = _find_substation_id(
                plant_id, row.sender_name, substation_names, plant_pairs, plant_to_subs, False
            )
            key = (plant_id, sub_id, row.timestamp)
            result.setdefault(key, {"generated": 0.0, "received": 0.0})
            result[key]["generated"] += row.value_kwh

        elif "inn" in measure:
            sub_id = _find_substation_id(
                plant_id, row.sender_name, substation_names, plant_pairs, plant_to_subs, True
            )
            key = (plant_id, sub_id, row.timestamp)
            result.setdefault(key, {"generated": 0.0, "received": 0.0})
            result[key]["received"] += row.value_kwh

    return result


def _group_user_rows(
    rows: List[NormalizedMeasurementRow],
    plant_names: Dict[str, int],
    substation_names: Dict[str, int],
    customer_names: Dict[str, int],
    plant_pairs: List[Tuple[int, int]],
    sub_user_pairs: List[Tuple[int, int]],
) -> Dict[Tuple[int, int, datetime], float]:
    """Safna úttektum per substation og notanda."""
    result: Dict[Tuple[int, int, datetime], float] = {}

    for row in rows:
        measure = row.measurement_type.lower()
        if "útt" not in measure and "utt" not in measure:
            continue

        plant_id = plant_names.get(row.plant_name)
        sub_id = substation_names.get(row.sender_name)
        user_id = customer_names.get(row.customer_name or "")

        if plant_id is None or sub_id is None or user_id is None:
            raise HTTPException(
                status_code=400,
                detail="Gat ekki fundið virkjun, tengivirki eða notanda fyrir úttekt",
            )

        if (plant_id, sub_id) not in plant_pairs:
            raise HTTPException(
                status_code=400,
                detail=f"Tengivirki '{row.sender_name}' er ekki tengt við '{row.plant_name}'",
            )

        if (sub_id, user_id) not in sub_user_pairs:
            raise HTTPException(
                status_code=400,
                detail=f"Notandi '{row.customer_name}' er ekki tengdur við '{row.sender_name}'",
            )

        key = (sub_id, user_id, row.timestamp)
        result[key] = result.get(key, 0.0) + row.value_kwh

    return result


# Task C5
def get_monthly_energy_flow_data(
    from_date: datetime,
    to_date: datetime,
    db: Session,
) -> List[MonthlyPlantEnergyFlowModel]:
    query = text(
        """
            WITH monthly_measurements AS (
                SELECT
                    eu.name AS power_plant_source,
                    DATE_TRUNC('month', psm.time) AS month_start,
                    'Framleiðsla' AS measurement_type,
                    SUM(psm.generated_pwr) AS total_kwh
                FROM public.plant_sub_measurements AS psm
                JOIN public.pwr_plant AS pp ON psm.plant_id = pp.id
                JOIN public.energy_unit AS eu ON eu.id = pp.id
                WHERE psm.time >= :from_date
                  AND psm.time < :to_date
                GROUP BY eu.name, month_start

                UNION ALL

                SELECT
                    eu.name AS power_plant_source,
                    DATE_TRUNC('month', psm.time) AS month_start,
                    'Innmötun' AS measurement_type,
                    SUM(psm.received_pwr) AS total_kwh
                FROM public.plant_sub_measurements AS psm
                JOIN public.pwr_plant AS pp ON psm.plant_id = pp.id
                JOIN public.energy_unit AS eu ON eu.id = pp.id
                WHERE psm.time >= :from_date
                  AND psm.time < :to_date
                GROUP BY eu.name, month_start

                UNION ALL

                SELECT
                    eu.name AS power_plant_source,
                    DATE_TRUNC('month', sumu.time) AS month_start,
                    'Úttekt' AS measurement_type,
                    SUM(sumu.received_pwr) AS total_kwh
                FROM public.sub_user_measurements AS sumu
                JOIN public.plant_substation_connection AS psc
                    ON sumu.substation_id = psc.substation_id
                JOIN public.pwr_plant AS pp ON psc.plant_id = pp.id
                JOIN public.energy_unit AS eu ON eu.id = pp.id
                WHERE sumu.time >= :from_date
                  AND sumu.time < :to_date
                GROUP BY eu.name, month_start
            )
            SELECT
                power_plant_source,
                EXTRACT(YEAR FROM month_start)::int AS year,
                EXTRACT(MONTH FROM month_start)::int AS month,
                measurement_type,
                total_kwh
            FROM monthly_measurements
            ORDER BY
                power_plant_source,
                year,
                month,
                total_kwh DESC
        """
    )

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    return [
        MonthlyPlantEnergyFlowModel(**row._mapping)  # type: ignore[arg-type]
        for row in result
    ]


def get_monthly_company_usage_data(
    from_date: datetime,
    to_date: datetime,
    db: Session,
) -> List[MonthlyCompanyUsageModel]:
    query = text(
        """
            SELECT
                plant_unit.name AS power_plant_source,
                ui.name AS customer_name,
                EXTRACT(YEAR FROM sumu.time)::int AS year,
                EXTRACT(MONTH FROM sumu.time)::int AS month,
                SUM(sumu.received_pwr) AS total_kwh
            FROM public.sub_user_measurements AS sumu
            JOIN public.energy_user AS eu ON sumu.energy_user_id = eu.id
            JOIN public.user_info AS ui ON eu.kennitala = ui.kennitala
            JOIN public.plant_substation_connection AS psc
                ON sumu.substation_id = psc.substation_id
            JOIN public.energy_unit AS plant_unit ON plant_unit.id = psc.plant_id
            WHERE sumu.time >= :from_date
              AND sumu.time < :to_date
            GROUP BY plant_unit.name, ui.name, year, month
            ORDER BY plant_unit.name, year, month, customer_name
        """
    )

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    return [
        MonthlyCompanyUsageModel(**row._mapping)  # type: ignore[arg-type]
        for row in result
    ]


def get_monthly_plant_loss_ratios_data(
    from_date: datetime,
    to_date: datetime,
    db: Session,
) -> List[MonthlyPlantLossRatiosModel]:
    query = text(
        """
            WITH plant_generation AS (
                SELECT
                    psm.plant_id,
                    SUM(psm.generated_pwr) AS total_generated,
                    SUM(psm.received_pwr) AS total_received
                FROM public.plant_sub_measurements AS psm
                WHERE psm.time >= :from_date
                  AND psm.time < :to_date
                GROUP BY psm.plant_id
            ),
            user_consumption AS (
                SELECT
                    psc.plant_id,
                    SUM(sumu.received_pwr) AS total_user_received
                FROM public.sub_user_measurements AS sumu
                JOIN public.plant_substation_connection AS psc
                    ON sumu.substation_id = psc.substation_id
                WHERE sumu.time >= :from_date
                  AND sumu.time < :to_date
                GROUP BY psc.plant_id
            )
            SELECT
                eu.name AS power_plant_source,
                CASE
                    WHEN pg.total_generated = 0 THEN 0
                    ELSE (pg.total_generated - pg.total_received) / pg.total_generated
                END AS plant_to_substation_loss_ratio,
                CASE
                    WHEN pg.total_generated = 0 THEN 0
                    ELSE (pg.total_generated - COALESCE(uc.total_user_received, 0)) / pg.total_generated
                END AS total_system_loss_ratio
            FROM plant_generation AS pg
            JOIN public.energy_unit AS eu ON eu.id = pg.plant_id
            LEFT JOIN user_consumption AS uc ON uc.plant_id = pg.plant_id
            ORDER BY eu.name
        """
    )

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    return [
        MonthlyPlantLossRatiosModel(**row._mapping)  # type: ignore[arg-type]
        for row in result
    ]


# Task E1
async def insert_measurements_data(
    file: UploadFile,
    db: Session,
):
    validate_file_type(file, [".csv"])
    raw_bytes = await file.read()
    raw_text = raw_bytes.decode("utf-8-sig")

    parsed_rows = parse_measurements_csv(raw_text)
    if not parsed_rows:
        raise HTTPException(
            status_code=400,
            detail="CSV skráin innihélt engar nothæfar línur",
        )

    plant_names, substation_names, customer_names = _load_names(db)
    plant_to_subs, plant_pairs, sub_user_pairs = _load_links(db)

    plant_measurements = _group_plant_rows(
        parsed_rows,
        plant_names,
        substation_names,
        plant_pairs,
        plant_to_subs,
    )
    user_measurements = _group_user_rows(
        parsed_rows,
        plant_names,
        substation_names,
        customer_names,
        plant_pairs,
        sub_user_pairs,
    )

    plant_payload = [
        {
            "plant_id": plant_id,
            "substation_id": substation_id,
            "time": timestamp,
            "generated_pwr": values["generated"],
            "received_pwr": values["received"],
        }
        for (plant_id, substation_id, timestamp), values in plant_measurements.items()
        if values["generated"] or values["received"]
    ]

    user_payload = [
        {
            "substation_id": substation_id,
            "energy_user_id": energy_user_id,
            "time": timestamp,
            "sent_pwr": values["sent"],
            "received_pwr": values["received"],
        }
        for (substation_id, energy_user_id, timestamp), values in user_measurements.items()
        if values["sent"] or values["received"]
    ]

    if not plant_payload and not user_payload:
        raise HTTPException(
            status_code=400,
            detail="Engar línur voru settar inn í gagnagrunninn",
        )

    try:
        if plant_payload:
            db.bulk_insert_mappings(PlantSubMeasurements, plant_payload)
        if user_payload:
            db.bulk_insert_mappings(SubUserMeasurements, user_payload)
        db.commit()
    except Exception as exc:  # pragma: no cover
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "status": 200,
        "rows_processed": len(parsed_rows),
        "plant_measurements_inserted": len(plant_payload),
        "user_measurements_inserted": len(user_payload),
    }


def get_substations_gridflow_data():
    raise NotImplementedError
