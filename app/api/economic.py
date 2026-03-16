from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from sqlalchemy.orm import Session
import os

from database import get_db
from services.economic_service import EconomicService
import app.crud.economic_snapshot as economic_crud
import app.schemas as schemas
from app.services.sector_economic_rating import SectorEconomicRatingService
from config import get_settings

router = APIRouter()

settings = get_settings()
economic_service = EconomicService(api_key=settings.fred_api_key)
sector_rating_service = SectorEconomicRatingService()


def _r2(val):
    return (
        round(float(val), 2)
        if isinstance(val, (int, float)) and val is not None
        else val
    )


def _normalize_snapshot(snapshot) -> Dict[str, Any]:
    # Combine indicator value/score/trend/previous into a single object per indicator
    indicators = snapshot.indicators or {}
    context = snapshot.indicator_context or {}
    normalized_indicators = {}
    for name, value in indicators.items():
        ctx = context.get(name, {}) if isinstance(context, dict) else {}
        normalized_indicators[name] = {
            "value": _r2(value),
            "score": _r2(ctx.get("score")),
            "trend": ctx.get("trend"),
            "previous": _r2(ctx.get("previous")),
        }

    components = snapshot.components or {}
    components = {k: _r2(v) for k, v in components.items()}

    return {
        "id": snapshot.id if hasattr(snapshot, "id") else None,
        "economic_score": _r2(snapshot.economic_score),
        "components": components,
        "indicators": normalized_indicators,
        "analysis": snapshot.analysis,
        "data_source": snapshot.data_source,
        "created_at": snapshot.created_at,
    }


@router.get("/", response_model=Dict)
def get_economic_environment(db: Session = Depends(get_db)):
    try:
        snapshot = economic_crud.get_latest_snapshot(db)
        if not snapshot:
            snapshot = _refresh_and_store(db)
        return _normalize_snapshot(snapshot)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching economic data: {str(e)}"
        )


@router.get("/indicators", response_model=Dict)
def get_economic_indicators(db: Session = Depends(get_db)):
    try:
        snapshot = economic_crud.get_latest_snapshot(db)
        if not snapshot:
            snapshot = _refresh_and_store(db)
        normalized = _normalize_snapshot(snapshot)
        return {
            "indicators": normalized["indicators"],
            "data_source": normalized["data_source"],
            "timestamp": normalized["created_at"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching indicators: {str(e)}"
        )


@router.post("/refresh", response_model=Dict)
def refresh_economic_data(db: Session = Depends(get_db)):
    try:
        snapshot = _refresh_and_store(db)
        return _normalize_snapshot(snapshot)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error refreshing economic data: {str(e)}"
        )


@router.get("/health", response_model=schemas.EconomicHealth)
def check_economic_service():
    has_api_key = bool(os.getenv("FRED_API_KEY"))
    if not has_api_key:
        return {
            "status": "degraded",
            "message": "FRED_API_KEY not configured - economic scores default to 5.0",
            "recommendation": "Add FRED_API_KEY to enable economic analysis",
        }
    try:
        test_data = economic_service._fetch_latest_value("fed_funds_rate")
        if test_data:
            return {
                "status": "healthy",
                "message": "FRED API connected successfully",
                "sample_data": f"Federal Funds Rate: {test_data}%",
            }
        return {
            "status": "warning",
            "message": "FRED API connected but no data returned",
            "recommendation": "Check FRED API status",
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"FRED API connection failed: {str(e)}",
            "recommendation": "Verify FRED_API_KEY is valid",
        }


def _refresh_and_store(db: Session):
    economic_data = economic_service.calculate_economic_score()
    snapshot = economic_crud.save_snapshot(db, economic_data)
    sector_rating_service.rate_all_sectors(db, snapshot)
    return snapshot
