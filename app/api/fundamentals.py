from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
import app.schemas as schemas
import app.crud.fundamental as fundamental_crud
from app.services.fundamental import FundamentalService
from app.services.fundamental_analysis import FundamentalAnalysisEngine

router = APIRouter()
service = FundamentalService()
analyzer = FundamentalAnalysisEngine()


@router.get(
    "/stocks/{stock_id}/fundamentals",
    response_model=dict,
)
def get_latest_fundamentals(
    stock_id: int,
    investment_style: str = Query(
        "value",
        regex="^(growth|value|income|quality)$",
        description="Investor style to emphasize in the composite narrative",
    ),
    db: Session = Depends(get_db),
):
    record = fundamental_crud.latest(db, stock_id)
    if not record:
        raise HTTPException(status_code=404, detail="No fundamentals found for stock")
    return analyzer.analyze(record, investment_style=investment_style)


@router.post(
    "/stocks/{stock_id}/fundamentals/refresh",
    response_model=schemas.FundamentalIndicator,
)
def refresh_fundamentals(
    stock_id: int,
    force_refresh: bool = Query(
        False,
        description="Set true to bypass the 24h cache window and pull fresh metrics",
    ),
    db: Session = Depends(get_db),
):
    return service.refresh_for_stock(db, stock_id, force_refresh=force_refresh)


@router.post("/stocks/fundamentals/refresh")
def refresh_all_fundamentals(
    force_refresh: bool = Query(
        False,
        description="Set true to refresh even if recent fundamentals exist",
    ),
    db: Session = Depends(get_db),
):
    return service.refresh_all(db, force_refresh=force_refresh)
