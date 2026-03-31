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

"""
python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

Swagger UI: http://localhost:8000/docs
"""

# Fasti fyrir viewin okkar
TASK_C_DROP_STATEMENTS = [
    "DROP VIEW IF EXISTS public.substation_plant_map CASCADE",
    "DROP VIEW IF EXISTS public.energy_flow CASCADE",
    "DROP VIEW IF EXISTS public.monthly_company_usage_view CASCADE",
    "DROP VIEW IF EXISTS public.energy_delivered CASCADE",
    "DROP VIEW IF EXISTS public.pwr_plant_monthly_totals CASCADE",
]

TASK_C_VIEW_STATEMENTS = [
    """
    CREATE OR REPLACE VIEW public.substation_plant_map AS
    WITH RECURSIVE sub_map AS (
        SELECT
            psc.substation_id,
            psc.plant_id
        FROM public.plant_substation_connection AS psc
        UNION ALL
        SELECT
            ssc.receiving_station_id AS substation_id,
            sub_map.plant_id
        FROM public.substation_substation_connection AS ssc
        JOIN sub_map ON ssc.sending_station_id = sub_map.substation_id
    )
    SELECT DISTINCT substation_id, plant_id
    FROM sub_map
    """,
    """
    CREATE OR REPLACE VIEW public.pwr_plant_monthly_totals AS
    SELECT
        eu.id AS plant_id,
        eu.name,
        EXTRACT(YEAR FROM psm.time)::int AS year,
        EXTRACT(MONTH FROM psm.time)::int AS month,
        SUM(
            CASE WHEN psm."type" ILIKE 'Framlei%' THEN psm.pwr_measurement_kwh ELSE 0 END
        ) AS total_production_kwh,
        SUM(
            CASE WHEN psm."type" ILIKE 'Innm%' THEN psm.pwr_measurement_kwh ELSE 0 END
        ) AS total_substation_pwr_kwh
    FROM public.plant_sub_measurements AS psm
    JOIN public.pwr_plant AS pp ON psm.plant_id = pp.id
    JOIN public.energy_unit AS eu ON eu.id = pp.id
    GROUP BY
        eu.id,
        eu.name,
        EXTRACT(YEAR FROM psm.time)::int,
        EXTRACT(MONTH FROM psm.time)::int
    """,
    """
    CREATE OR REPLACE VIEW public.energy_delivered AS
    SELECT
        spm.plant_id AS pwr_plant_id,
        EXTRACT(YEAR FROM sumu.time)::int AS year,
        EXTRACT(MONTH FROM sumu.time)::int AS month,
        SUM(sumu.pwr_measurement_kwh) AS delivered_pwr
    FROM public.sub_user_measurements AS sumu
    JOIN public.substation_plant_map AS spm
        ON sumu.substation_id = spm.substation_id
    GROUP BY
        spm.plant_id,
        EXTRACT(YEAR FROM sumu.time)::int,
        EXTRACT(MONTH FROM sumu.time)::int
    """,
    """
    CREATE OR REPLACE VIEW public.energy_flow AS
    SELECT
        ppt.plant_id,
        ppt.name,
        ppt.year,
        ppt.month,
        ppt.total_production_kwh,
        ppt.total_substation_pwr_kwh,
        CASE WHEN ed.delivered_pwr IS NULL THEN 0 ELSE ed.delivered_pwr END AS delivered_pwr
    FROM public.pwr_plant_monthly_totals AS ppt
    LEFT JOIN public.energy_delivered AS ed
           ON ed.pwr_plant_id = ppt.plant_id
          AND ed.year = ppt.year
          AND ed.month = ppt.month
    ORDER BY ppt.name, ppt.year, ppt.month
    """,
    """
    CREATE OR REPLACE VIEW public.monthly_company_usage_view AS
    SELECT
        plants.name AS power_plant_source,
        ui.name AS customer_name,
        EXTRACT(YEAR FROM sumu.time)::int AS year,
        EXTRACT(MONTH FROM sumu.time)::int AS month,
        SUM(sumu.pwr_measurement_kwh) AS total_kwh
    FROM public.sub_user_measurements AS sumu
    JOIN public.energy_user AS eu ON sumu.energy_user_id = eu.id
    JOIN public.user_info AS ui ON eu.kennitala = ui.kennitala
    JOIN public.substation_plant_map AS spm
        ON sumu.substation_id = spm.substation_id
    JOIN public.energy_unit AS plants ON plants.id = spm.plant_id
    GROUP BY
        plants.name,
        ui.name,
        EXTRACT(YEAR FROM sumu.time)::int,
        EXTRACT(MONTH FROM sumu.time)::int
    """,
]

_TASK_C_VIEWS_CREATED = False # Create pointer for views


def ensure_task_c_views(db: Session) -> None:
    """Keyrum view skipanirnar einu sinni"""
    global _TASK_C_VIEWS_CREATED
    if _TASK_C_VIEWS_CREATED:
        return

    for statement in TASK_C_DROP_STATEMENTS:
        db.execute(text(statement))

    for statement in TASK_C_VIEW_STATEMENTS:
        db.execute(text(statement))

    db.commit()
    _TASK_C_VIEWS_CREATED = True # View created verður true ef það tókst að gera þau


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


def _build_plant_measurements(
    rows: List[NormalizedMeasurementRow],
    plant_names: Dict[str, int],
    substation_names: Dict[str, int],
    plant_pairs: List[Tuple[int, int]],
    plant_to_subs: Dict[int, List[int]],
) -> List[Dict[str, object]]:
    """Búum til línur fyrir plant_sub_measurements."""
    payload: List[Dict[str, object]] = []

    for row in rows:
        measure = row.measurement_type.lower()
        if "fram" not in measure and "inn" not in measure:
            continue

        plant_id = plant_names.get(row.plant_name)
        if plant_id is None:
            raise HTTPException(
                status_code=400,
                detail=f"Virkjun '{row.plant_name}' fannst ekki",
            )

        force_sender = "inn" in measure
        sub_id = _find_substation_id(
            plant_id,
            row.sender_name,
            substation_names,
            plant_pairs,
            plant_to_subs,
            force_sender,
        )

        payload.append(
            {
                "plant_id": plant_id,
                "substation_id": sub_id,
                "time": row.timestamp,
                "pwr_measurement_kwh": row.value_kwh,
                "type": row.measurement_type,
            }
        )

    return payload


def _build_user_measurements(
    rows: List[NormalizedMeasurementRow],
    plant_names: Dict[str, int],
    substation_names: Dict[str, int],
    customer_names: Dict[str, int],
    plant_pairs: List[Tuple[int, int]],
    sub_user_pairs: List[Tuple[int, int]],
) -> List[Dict[str, object]]:
    """Búum til línur fyrir sub_user_measurements."""
    payload: List[Dict[str, object]] = []

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

        payload.append(
            {
                "substation_id": sub_id,
                "energy_user_id": user_id,
                "time": row.timestamp,
                "pwr_measurement_kwh": row.value_kwh,
            }
        )

    return payload


# Task C5
def get_monthly_energy_flow_data(
    from_date: datetime,
    to_date: datetime,
    db: Session,
) -> List[MonthlyPlantEnergyFlowModel]:
    # Sækjum gögn úr energy_flow view til að mata Query A1 endpoint
    ensure_task_c_views(db)

    query = text(
        """
            SELECT
                name AS power_plant_source,
                year,
                month,
                'Framleiðsla' AS measurement_type,
                CASE
                    WHEN total_production_kwh IS NULL THEN 0
                    ELSE total_production_kwh
                END AS total_kwh
            FROM public.energy_flow
            WHERE MAKE_DATE(year, month, 1) >= :from_date
              AND MAKE_DATE(year, month, 1) < :to_date

            UNION ALL

            SELECT
                name AS power_plant_source,
                year,
                month,
                'Innmötun' AS measurement_type,
                CASE
                    WHEN total_substation_pwr_kwh IS NULL THEN 0
                    ELSE total_substation_pwr_kwh
                END AS total_kwh
            FROM public.energy_flow
            WHERE MAKE_DATE(year, month, 1) >= :from_date
              AND MAKE_DATE(year, month, 1) < :to_date

            UNION ALL

            SELECT
                name AS power_plant_source,
                year,
                month,
                'Úttekt' AS measurement_type,
                CASE
                    WHEN delivered_pwr IS NULL THEN 0
                    ELSE delivered_pwr
                END AS total_kwh
            FROM public.energy_flow
            WHERE MAKE_DATE(year, month, 1) >= :from_date
              AND MAKE_DATE(year, month, 1) < :to_date

            ORDER BY power_plant_source, year, month, total_kwh DESC
        """
    )

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    items: List[MonthlyPlantEnergyFlowModel] = []
    for row in result:
        items.append(MonthlyPlantEnergyFlowModel(**row._mapping))  # type: ignore[arg-type]
    return items


def get_monthly_company_usage_data(
    from_date: datetime,
    to_date: datetime,
    db: Session,
) -> List[MonthlyCompanyUsageModel]:
    # Notum samnefnt view til að spegla Query A2 niðurstöður
    ensure_task_c_views(db)

    query = text(
        """
            SELECT
                power_plant_source,
                customer_name,
                year,
                month,
                total_kwh
            FROM public.monthly_company_usage_view
            WHERE MAKE_DATE(year, month, 1) >= :from_date
              AND MAKE_DATE(year, month, 1) < :to_date
            ORDER BY power_plant_source, year, month, customer_name
        """
    )

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    items: List[MonthlyCompanyUsageModel] = []
    for row in result:
        items.append(MonthlyCompanyUsageModel(**row._mapping))  # type: ignore[arg-type]
    return items


def get_monthly_plant_loss_ratios_data(
    from_date: datetime,
    to_date: datetime,
    db: Session,
) -> List[MonthlyPlantLossRatiosModel]:
    # Einfalt tapmat með því að summa energy_flow línur
    ensure_task_c_views(db)

    query = text(
        """
            WITH plant_totals AS (
                SELECT
                    name AS power_plant_source,
                    SUM(total_production_kwh) AS total_production,
                    SUM(total_substation_pwr_kwh) AS total_to_substations,
                    SUM(delivered_pwr) AS total_to_users
                FROM public.energy_flow
                WHERE MAKE_DATE(year, month, 1) >= :from_date
                  AND MAKE_DATE(year, month, 1) < :to_date
                GROUP BY name
            )
            SELECT
                power_plant_source,
                CASE
                    WHEN total_production = 0 THEN 0
                    ELSE (total_production - total_to_substations) / total_production
                END AS plant_to_substation_loss_ratio,
                CASE
                    WHEN total_production = 0 THEN 0
                    ELSE (
                        total_production
                        - CASE WHEN total_to_users IS NULL THEN 0 ELSE total_to_users END
                    ) / total_production
                END AS total_system_loss_ratio
            FROM plant_totals
            ORDER BY power_plant_source
        """
    )

    result = db.execute(query, {"from_date": from_date, "to_date": to_date})

    items: List[MonthlyPlantLossRatiosModel] = []
    for row in result:
        items.append(MonthlyPlantLossRatiosModel(**row._mapping))  # type: ignore[arg-type]
    return items


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

    plant_measurements = _build_plant_measurements(
        parsed_rows,
        plant_names,
        substation_names,
        plant_pairs,
        plant_to_subs,
    )
    user_measurements = _build_user_measurements(
        parsed_rows,
        plant_names,
        substation_names,
        customer_names,
        plant_pairs,
        sub_user_pairs,
    )

    if not plant_measurements and not user_measurements:
        raise HTTPException(
            status_code=400,
            detail="Engar línur voru settar inn í gagnagrunninn",
        )

    try:
        if plant_measurements:
            db.bulk_insert_mappings(PlantSubMeasurements, plant_measurements)
        if user_measurements:
            db.bulk_insert_mappings(SubUserMeasurements, user_measurements)
        db.commit()
    except Exception as exc:  # pragma: no cover
        db.rollback()
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "status": 200,
        "rows_processed": len(parsed_rows),
        "plant_measurements_inserted": len(plant_measurements),
        "user_measurements_inserted": len(user_measurements),
    }


def get_substations_gridflow_data():
    raise NotImplementedError
