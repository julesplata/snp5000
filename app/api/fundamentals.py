from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
import app.schemas as schemas
import app.crud.fundamental as fundamental_crud
import app.crud.fundamental_analysis as fundamental_analysis_crud
from app.services.fundamental import FundamentalService
from app.services.fundamental_analysis import FundamentalAnalysisEngine

router = APIRouter()
service = FundamentalService()
analyzer = FundamentalAnalysisEngine()


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

    full = analyzer.analyze(raw, investment_style=investment_style)
    payload = schemas.FundamentalAnalysisCreate(
        stock_id=stock_id,
        fundamental_indicator_id=raw.id,
        normalized_scores=full["normalized_scores"],
        composite_scores=full["composite_scores"],
        anomalies=full["anomalies"],
        risk_rating=full["narrative"]["risk_rating"],
        confidence=full["narrative"]["confidence"],
        narrative=full["narrative"],
        analyzed_at=datetime.utcnow(),
    )
    analysis_row = fundamental_analysis_crud.upsert(db, payload)
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
