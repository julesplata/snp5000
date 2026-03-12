from datetime import datetime, timedelta
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

import app.crud.fundamental as fundamental_crud
import app.crud.fundamental_analysis as fundamental_analysis_crud
import app.models as models
import app.schemas as schemas
from app.services.fundamental_analysis import FundamentalAnalysisEngine
from app.utils.rating_utils import FinnhubClient
from config import get_settings


class FundamentalService:
    def __init__(self, finnhub_api_key: Optional[str] = None, ttl_hours: int = 24):
        settings = get_settings()
        self.client = FinnhubClient(
            finnhub_api_key or settings.finnhub_api_key, max_per_minute=55
        )
        self.ttl_hours = ttl_hours

    def refresh_for_stock(
        self, db: Session, stock_id: int, force_refresh: bool = False
    ) -> models.FundamentalIndicator:
        stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")

        cutoff = datetime.utcnow() - timedelta(hours=self.ttl_hours)
        if not force_refresh:
            cached = fundamental_crud.latest(db, stock.id, since=cutoff)
            if cached:
                return cached

        payload = self._fetch_metrics_payload(stock.symbol, stock.id)
        new_record = fundamental_crud.create(db, payload)
        self._store_analysis(db, new_record)
        return new_record

    def refresh_all(self, db: Session, force_refresh: bool = False) -> dict:
        updated = 0
        stocks = db.query(models.Stock).all()
        for stock in stocks:
            try:
                self.refresh_for_stock(db, stock.id, force_refresh=force_refresh)
                updated += 1
            except Exception:
                db.rollback()
                continue
        return {"updated": updated, "timestamp": datetime.utcnow()}

    def _fetch_metrics_payload(self, symbol: str, stock_id: int) -> dict:
        data = self.client.get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if not data or not data.get("metric"):
            raise HTTPException(
                status_code=503, detail="Fundamental metrics unavailable right now"
            )
        metrics = data["metric"] or {}
        return {
            "stock_id": stock_id,
            "pe_ratio": self._first_metric(metrics, ["peBasicExclExtraTTM", "peTTM"]),
            "pb_ratio": self._first_metric(metrics, ["pbAnnual", "pbQuarterly"]),
            "debt_to_equity": self._first_metric(
                metrics,
                [
                    "totalDebt/totalEquityAnnual",
                    "totalDebt/totalEquityQuarterly",
                    "totalDebtToEquityAnnual",
                    "totalDebtToEquityQuarterly",
                ],
            ),
            "profit_margin": self._first_metric(
                metrics, ["netProfitMarginTTM", "netProfitMarginAnnual"]
            ),
            "dividend_yield": self._first_metric(
                metrics, ["dividendYieldIndicatedAnnual", "dividendYieldTTM"]
            ),
            "raw_metrics": metrics,
            "data_source": "finnhub",
            "fetched_at": datetime.utcnow(),
        }

    def _store_analysis(
        self, db: Session, raw_record: models.FundamentalIndicator
    ) -> None:
        analyzer = FundamentalAnalysisEngine()
        result = analyzer.analyze(raw_record)
        payload = schemas.FundamentalAnalysisCreate(
            stock_id=raw_record.stock_id,
            fundamental_indicator_id=raw_record.id,
            normalized_scores=result["normalized_scores"],
            composite_scores=result["composite_scores"],
            anomalies=result["anomalies"],
            risk_rating=result["narrative"]["risk_rating"],
            confidence=result["narrative"]["confidence"],
            narrative=result["narrative"],
            analyzed_at=datetime.utcnow(),
        )
        fundamental_analysis_crud.upsert(db, payload)

    @staticmethod
    def _first_metric(metrics: dict, keys) -> Optional[float]:
        for key in keys:
            if key in metrics and metrics[key] is not None:
                try:
                    return float(metrics[key])
                except Exception:
                    continue
        return None
