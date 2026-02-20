from fastapi import APIRouter, HTTPException
from typing import Dict
from services.macro_service import MacroeconomicService
import os
from config import get_settings

router = APIRouter()

# Get API keys from .env file
settings = get_settings()

FRED_API_KEY = settings.fred_api_key
macro_service = MacroeconomicService(api_key=FRED_API_KEY)


@router.get("/", response_model=Dict)
def get_macro_environment():
    """
    Get current macroeconomic environment analysis

    Returns comprehensive macro score with component breakdown,
    indicator values, and analysis text.

    Requires FRED_API_KEY environment variable.
    Data is real-time from Federal Reserve Economic Data (FRED).
    """
    try:
        macro_data = macro_service.calculate_macro_score()
        return macro_data
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching macro data: {str(e)}"
        )


@router.get("/indicators", response_model=Dict)
def get_macro_indicators():
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
        macro_data = macro_service.calculate_macro_score()
        return {
            "indicators": macro_data.get("indicators", {}),
            "data_source": macro_data.get("data_source", "FRED"),
            "timestamp": "real-time",
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching indicators: {str(e)}"
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
