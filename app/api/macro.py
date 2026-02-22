from fastapi import APIRouter, HTTPException, Depends
from typing import Dict
from sqlalchemy.orm import Session
import os

from database import get_db
from services.macro_service import MacroeconomicService
import app.crud.macro_snapshot as macro_crud
from config import get_settings

router = APIRouter()

settings = get_settings()
macro_service = MacroeconomicService(api_key=settings.fred_api_key)


@router.get("/", response_model=Dict)
def get_macro_environment(db: Session = Depends(get_db)):
    try:
        snapshot = macro_crud.get_latest_snapshot(db)
        if not snapshot:
            snapshot = _refresh_and_store(db)
        return {
            "macro_score": snapshot.macro_score,
            "components": snapshot.components,
            "indicators": snapshot.indicators,
            "indicator_context": snapshot.indicator_context,
            "indicator_meta": snapshot.indicator_meta,
            "analysis": snapshot.analysis,
            "data_source": snapshot.data_source,
            "created_at": snapshot.created_at,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching macro data: {str(e)}"
        )


@router.get("/indicators", response_model=Dict)
def get_macro_indicators(db: Session = Depends(get_db)):
    try:
        snapshot = macro_crud.get_latest_snapshot(db)
        if not snapshot:
            snapshot = _refresh_and_store(db)
        return {
            "indicators": snapshot.indicators or {},
            "indicator_context": snapshot.indicator_context or {},
            "indicator_meta": snapshot.indicator_meta or {},
            "data_source": snapshot.data_source,
            "timestamp": snapshot.created_at,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching indicators: {str(e)}"
        )


@router.post("/refresh", response_model=Dict)
def refresh_macro_data(db: Session = Depends(get_db)):
    try:
        snapshot = _refresh_and_store(db)
        return {
            "id": snapshot.id,
            "created_at": snapshot.created_at,
            "macro_score": snapshot.macro_score,
            "components": snapshot.components,
            "indicators": snapshot.indicators,
            "indicator_context": snapshot.indicator_context,
            "indicator_meta": snapshot.indicator_meta,
            "analysis": snapshot.analysis,
            "data_source": snapshot.data_source,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error refreshing macro data: {str(e)}"
        )


@router.get("/health")
def check_macro_service():
    has_api_key = bool(os.getenv("FRED_API_KEY"))
    if not has_api_key:
        return {
            "status": "degraded",
            "message": "FRED_API_KEY not configured - macro scores default to 5.0",
            "recommendation": "Add FRED_API_KEY to enable macro analysis",
        }
    try:
        test_data = macro_service._fetch_latest_value("fed_funds_rate")
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
    macro_data = macro_service.calculate_macro_score()
    return macro_crud.save_snapshot(db, macro_data)
