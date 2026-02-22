from fastapi import APIRouter, HTTPException, Depends
from typing import Dict
from sqlalchemy.orm import Session
from services.macro_service import MacroeconomicService
import os
from config import get_settings
from database import get_db
from repositories import macro_repository

router = APIRouter()

# Get API keys from .env file
settings = get_settings()

FRED_API_KEY = settings.fred_api_key
macro_service = MacroeconomicService(api_key=FRED_API_KEY)


@router.get("/", response_model=Dict)
def get_macro_environment(db: Session = Depends(get_db)):
    """
    Get current macroeconomic environment analysis

    Returns comprehensive macro score with component breakdown,
    indicator values, and analysis text.

    Requires FRED_API_KEY environment variable.
    Data is real-time from Federal Reserve Economic Data (FRED).
    """
    try:
        snapshot = macro_repository.get_latest_snapshot(db)

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
    """
    Get just the raw macroeconomic indicators without scoring

    Useful for displaying current economic conditions:
    - Interest rates (Fed Funds, Treasury yields)
    - Inflation (CPI year-over-year)
    - GDP growth
    - Unemployment rate
    - Consumer sentiment
    """
    try:
        snapshot = macro_repository.get_latest_snapshot(db)
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
    """
    Force refresh of macroeconomic data from external sources and persist to DB.
    """
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
    """
    Health check for macroeconomic service

    Returns status of FRED API connection and whether
    macro scoring is available.
    """
    has_api_key = bool(os.getenv("FRED_API_KEY"))

    if not has_api_key:
        return {
            "status": "degraded",
            "message": "FRED_API_KEY not configured - macro scores will default to 5.0",
            "recommendation": "Add FRED_API_KEY to enable macro analysis",
        }

    # Test API connection
    try:
        test_data = macro_service._fetch_latest_value("fed_funds_rate")
        if test_data:
            return {
                "status": "healthy",
                "message": "FRED API connected successfully",
                "sample_data": f"Federal Funds Rate: {test_data}%",
            }
        else:
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
    """
    Internal helper to fetch fresh macro data and persist snapshot.
    """
    macro_data = macro_service.calculate_macro_score()
    return macro_repository.save_snapshot(db, macro_data)
