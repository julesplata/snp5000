from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
import app.schemas as schemas
import app.crud.fundamental as fundamental_crud
import app.crud.fundamental_analysis as fundamental_analysis_crud
from app.services.fundamental import FundamentalService

router = APIRouter()
service = FundamentalService()


def _build_slim_response(row, investment_style: str) -> dict:
    slim_scores = {
        metric: {
            "raw_value": data.get("raw_value"),
            "normalized_score": data.get("normalized_score"),
            "status": data.get("status"),
        }
        for metric, data in (row.normalized_scores or {}).items()
    }
    composite_scores = row.composite_scores or {}
    composite_score = composite_scores.get(investment_style, {}).get("overall_score")
    narrative = row.narrative or {}
    slim_narrative = {
        "strengths": narrative.get("strengths", []),
        "weaknesses": narrative.get("weaknesses", []),
        "verdict": narrative.get("verdict"),
        "risk_rating": narrative.get("risk_rating"),
        "confidence": narrative.get("confidence"),
        "summary": narrative.get("summary"),
    }
    return {
        "stock_id": row.stock_id,
        "investment_style": investment_style,
        "analyzed_at": row.analyzed_at,
        "normalized_scores": slim_scores,
        "composite_score": composite_score,
        "narrative": slim_narrative,
        "valuation_score": row.valuation_score,
        "profitability_score": row.profitability_score,
        "growth_score": row.growth_score,
        "health_score": row.health_score,
        "cashflow_score": row.cashflow_score,
        "efficiency_score": row.efficiency_score,
        "overall_fundamental_rating": row.overall_fundamental_rating,
        "peer_cca": (row.narrative or {}).get("peer_cca"),
    }


@router.get(
    "/stocks/{stock_id}/fundamentals",
    response_model=schemas.FundamentalAnalysisSlimResponse,
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
    cutoff = datetime.utcnow() - timedelta(hours=24)
    cached = fundamental_analysis_crud.latest(db, stock_id, since=cutoff)

    if cached:
        return _build_slim_response(cached, investment_style)

    raw = fundamental_crud.latest(db, stock_id)
    if not raw:
        raise HTTPException(status_code=404, detail="No fundamentals found for stock")

    analysis_row = service._analyze_and_store(db, raw)
    return _build_slim_response(analysis_row, investment_style)


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
