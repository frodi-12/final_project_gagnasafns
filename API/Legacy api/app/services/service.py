from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
from app.db.tables.orku_einingar import OrkuEiningar
from app.models.orku_einingar_model import OrkuEiningarModel
from app.db.tables.notendur_skraning import NotendurSkraning
from app.models.notendur_skraning_model import NotendurSkraningModel
from app.db.tables.orku_maelingar import OrkuMaelingar
from app.models.orku_maelingar_model import OrkuMaelingarModel
from app.db.tables.test_measurement import TestMeasurement
from app.models.parsed_data.test_measurement_data import TestMeasurementData
from app.parsers.parse_test_measurment_csv import parse_test_measurement_csv
from app.utils.validate_file_type import validate_file_type
from datetime import datetime
from sqlalchemy import text # import text for queries

'''
Services already in place
'''
def get_orku_einingar_data(
    db: Session
):
    rows = db.query(OrkuEiningar).all()

    return [
        OrkuEiningarModel(
            id=row.id,
            heiti=row.heiti,
            tegund=row.tegund,
            tegund_stod=row.tegund_stod,
            eigandi=row.eigandi,
            ar_uppsett=row.ar_uppsett,
            manudir_uppsett=row.manudir_uppsett,
            dagur_uppsett=row.dagur_uppsett,
            X_HNIT=row.X_HNIT,
            Y_HNIT=row.Y_HNIT,
            tengd_stod=row.tengd_stod,
        ) 
        for row in rows
    ]

def get_notendur_skraning_data(
    db: Session
):
    rows = db.query(NotendurSkraning).all()

    return [
        NotendurSkraningModel(
            id=row.id,
            heiti=row.heiti,
            kennitala=row.kennitala,
            eigandi=row.eigandi,
            ar_stofnad=row.ar_stofnad,
            X_HNIT=row.X_HNIT,
            Y_HNIT=row.Y_HNIT,
        ) 
        for row in rows
    ]

def get_orku_maelingar_data(
    from_date: datetime,
    to_date: datetime,
    limit: int,
    offset: int,
    db: Session,
    eining: str | None = None,
    tegund: str | None = None,
):
    query = db.query(OrkuMaelingar).filter(
        OrkuMaelingar.timi >= from_date,
        OrkuMaelingar.timi <= to_date
    )

    if eining:
        query = query.filter(OrkuMaelingar.eining_heiti == eining)
    if tegund:
        query = query.filter(OrkuMaelingar.tegund_maelingar == tegund)

    rows = (
        query
        .order_by(OrkuMaelingar.timi)
        .limit(limit)
        .offset(offset)
        .all()
    )

    return [
        OrkuMaelingarModel(
            id=row.id,
            eining_heiti=row.eining_heiti,
            tegund_maelingar=row.tegund_maelingar,
            sendandi_maelingar=row.sendandi_maelingar,
            timi=row.timi,
            gildi_kwh=row.gildi_kwh,
            notandi_heiti=row.notandi_heiti
        )
        for row in rows
    ]

async def insert_test_measurement_data(
    file: UploadFile,
    db: Session,
    mode: str = "bulk"
):
    validate_file_type(
        file, 
        allowed_extensions=[".csv"]
    )

    raw_data = await file.read()
    raw_text = raw_data.decode()

    parsed_rows: list[TestMeasurementData]
    parsed_rows = parse_test_measurement_csv(raw_text)

    if not parsed_rows:
        raise HTTPException(status_code=400, detail="No valid rows found")

    try:
        if mode == "single":
            for row in parsed_rows:
                db.add(
                    TestMeasurement(
                        timi=row.timi,
                        value=row.value
                    )
                )
            db.commit()

        elif mode == "bulk":
            insert_data = [
                {
                    "timi": row.timi,
                    "value": row.value
                }
                for row in parsed_rows
            ]
            db.bulk_insert_mappings(TestMeasurement, insert_data)
            db.commit()

        elif mode == "fallback":
            for row in parsed_rows:
                try:
                    db.add(
                        TestMeasurement(
                            timi=row.timi,
                            value=row.value
                        )
                    )
                    db.flush()
                except Exception:
                    db.rollback()
                    continue
            db.commit()
        else:
            raise HTTPException(status_code=400, detail="Invalid mode")

        return {
            "status": 200,
            "rows_processed": len(parsed_rows),
            "mode": mode
        }
    
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# Task B2

# Query A1
def get_monthly_energy_flow_data(from_date, to_date, db):
    query = text("""
        SELECT
            om.eining_heiti AS power_plant_source,
            EXTRACT(YEAR FROM om.timi)::int AS year,
            EXTRACT(MONTH FROM om.timi)::int AS month,
            om.tegund_maelingar AS measurement_type,
            SUM(om.gildi_kwh) AS total_kwh
        FROM raforka_legacy.orku_maelingar om
        JOIN raforka_legacy.orku_einingar oe
            ON om.eining_heiti = oe.heiti
        WHERE oe.tegund = 'virkjun'
          AND om.timi >= :from_date
          AND om.timi < :to_date
        GROUP BY
            om.eining_heiti,
            EXTRACT(YEAR FROM om.timi),
            EXTRACT(MONTH FROM om.timi),
            om.tegund_maelingar
        ORDER BY
            om.eining_heiti,
            year ASC,
            month ASC,
            total_kwh DESC
    """)

    result = db.execute(query, {
        "from_date": from_date,
        "to_date": to_date
    })

    return [dict(row._mapping) for row in result]

# Query A2
def get_monthly_company_usage_data(from_date, to_date, db):
    query = text("""
        SELECT
            eining_heiti AS power_plant_source,
            EXTRACT(YEAR FROM timi)::int AS year,
            EXTRACT(MONTH FROM timi)::int AS month,
            notandi_heiti AS customer_name,
            SUM(gildi_kwh) AS total_kwh
        FROM raforka_legacy.orku_maelingar
        WHERE timi >= :from_date
          AND timi < :to_date
          AND tegund_maelingar = 'Úttekt'
        GROUP BY
            eining_heiti,
            EXTRACT(YEAR FROM timi),
            EXTRACT(MONTH FROM timi),
            notandi_heiti
        ORDER BY
            power_plant_source,
            year ASC,
            month ASC,
            customer_name ASC
    """)

    result = db.execute(query, {
        "from_date": from_date,
        "to_date": to_date
    })

    return [dict(row._mapping) for row in result]

# Query A3
def get_monthly_plant_loss_ratios_data(from_date, to_date, db):
    query = text("""
        WITH monthly_plant_data AS (
            SELECT
                om.eining_heiti AS power_plant_source,
                EXTRACT(YEAR FROM om.timi)::int AS year,
                EXTRACT(MONTH FROM om.timi)::int AS month,
                SUM(CASE
                    WHEN om.tegund_maelingar ILIKE 'Framleiðsla' THEN om.gildi_kwh
                    ELSE 0
                END) AS framleidsla_kwh,
                SUM(CASE
                    WHEN om.tegund_maelingar ILIKE 'Innmötun' THEN om.gildi_kwh
                    ELSE 0
                END) AS innmotun_kwh,
                SUM(CASE
                    WHEN om.tegund_maelingar ILIKE 'Úttekt' THEN om.gildi_kwh
                    ELSE 0
                END) AS uttekt_kwh
            FROM raforka_legacy.orku_maelingar om
            JOIN raforka_legacy.orku_einingar oe
                ON om.eining_heiti = oe.heiti
            WHERE oe.tegund = 'virkjun'
              AND om.timi >= :from_date
              AND om.timi < :to_date
            GROUP BY
                om.eining_heiti,
                EXTRACT(YEAR FROM om.timi),
                EXTRACT(MONTH FROM om.timi)
        )
        SELECT
            power_plant_source,
            AVG(
                CASE
                    WHEN framleidsla_kwh > 0
                    THEN (framleidsla_kwh - innmotun_kwh) / framleidsla_kwh
                    ELSE NULL
                END
            ) AS plant_to_substation_loss_ratio,
            AVG(
                CASE
                    WHEN framleidsla_kwh > 0
                    THEN (framleidsla_kwh - uttekt_kwh) / framleidsla_kwh
                    ELSE NULL
                END
            ) AS total_system_loss_ratio
        FROM monthly_plant_data
        GROUP BY power_plant_source
        ORDER BY power_plant_source
    """)

    result = db.execute(query, {
        "from_date": from_date,
        "to_date": to_date
    })

    return [dict(row._mapping) for row in result]