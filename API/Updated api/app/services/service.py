from datetime import datetime
from typing import List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel


# Task C5
def get_monthly_energy_flow_data(
    from_date: datetime,
    to_date: datetime,
    db: Session
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

    result = db.execute(query, {
        "from_date": from_date,
        "to_date": to_date
    })

    return [
        MonthlyPlantEnergyFlowModel(**row._mapping)
        for row in result
    ]


# Placeholder stubs for later tasks
def get_monthly_company_usage_data():
    raise NotImplementedError


def get_monthly_plant_loss_ratios_data():
    raise NotImplementedError


def insert_measurements_data():
    raise NotImplementedError


def get_substations_gridflow_data():
    raise NotImplementedError
