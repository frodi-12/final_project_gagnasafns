from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, File, UploadFile
from sqlalchemy.orm import Session

from app.db.session import get_orkuflaedi_session
from app.models.monthly_company_usage_model import MonthlyCompanyUsageModel
from app.models.monthly_energy_flow_model import MonthlyPlantEnergyFlowModel
from app.models.monthly_plant_loss_ratios import MonthlyPlantLossRatiosModel
from app.services.service import (
    get_monthly_company_usage_data,
    get_monthly_energy_flow_data,
    get_monthly_plant_loss_ratios_data,
    insert_measurements_data,
)
from app.utils.validate_date_range import validate_date_range_helper

router = APIRouter()
db_name = "UpdatedOrkuFlaediIsland"


@router.get(
    "/monthly-energy-flow",
    response_model=List[MonthlyPlantEnergyFlowModel],
    summary="Query monthly energy flow for each plant and measurement type",
)
def get_monthly_energy_flow_endpoint(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session),
):
    print(f"Calling [GET] /{db_name}/monthly-energy-flow")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0),
    )

    return get_monthly_energy_flow_data(from_date, to_date, db)


@router.get(
    "/monthly-company-usage",
    response_model=List[MonthlyCompanyUsageModel],
    summary="Query monthly consumption per customer grouped by plant",
)
def get_monthly_company_usage_endpoint(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session),
):
    print(f"Calling [GET] /{db_name}/monthly-company-usage")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0),
    )

    return get_monthly_company_usage_data(from_date, to_date, db)


@router.get(
    "/plant-loss-ratios",
    response_model=List[MonthlyPlantLossRatiosModel],
    summary="Get loss ratios from plants to substations and end-users",
)
def get_monthly_plant_loss_ratios_endpoint(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    db: Session = Depends(get_orkuflaedi_session),
):
    print(f"Calling [GET] /{db_name}/plant-loss-ratios")

    from_date, to_date = validate_date_range_helper(
        from_date,
        to_date,
        datetime(2025, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 0),
    )

    return get_monthly_plant_loss_ratios_data(from_date, to_date, db)


@router.post(
    "/measurements-data",
    summary="Upload normalized measurement CSV and insert into database",
)
async def upload_measurements_data(
    file: UploadFile = File(...),
    db: Session = Depends(get_orkuflaedi_session),
):
    print(f"Calling [POST] /{db_name}/measurements-data")
    return await insert_measurements_data(file, db)
