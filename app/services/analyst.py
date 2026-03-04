from datetime import datetime, date
from typing import Optional
from fastapi import HTTPException
from sqlalchemy.orm import Session

import app.models as models
import app.crud.analyst as analyst_crud
from app.utils.rating_utils import FinnhubClient
from config import get_settings


class AnalystService:
    def __init__(self, finnhub_api_key: Optional[str] = None):
        settings = get_settings()
        self.client = FinnhubClient(
            finnhub_api_key or settings.finnhub_api_key, max_per_minute=55
        )

    def refresh_for_stock(self, db: Session, stock_id: int) -> dict:
        stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")

        recommendation = self._latest_recommendation(stock.symbol)

        if not recommendation:
            raise HTTPException(status_code=503, detail="Analyst data unavailable")

        published_at = self._published_at(recommendation, None)

        rating_payload = {
            "stock_id": stock_id,
            "source": "finnhub",
            "rating": self._rating_label(recommendation),
            "published_at": published_at,
        }
        consensus_payload = {
            "stock_id": stock_id,
            "strong_buy": recommendation.get("strongBuy") if recommendation else None,
            "buy": recommendation.get("buy") if recommendation else None,
            "hold": recommendation.get("hold") if recommendation else None,
            "sell": recommendation.get("sell") if recommendation else None,
            "strong_sell": recommendation.get("strongSell") if recommendation else None,
            "target_mean": None,
            "last_updated": published_at,
        }

        rating = analyst_crud.upsert_rating(db, rating_payload)
        consensus = analyst_crud.upsert_consensus(db, consensus_payload)

        return {"rating": rating, "consensus": consensus}

    def refresh_all(self, db: Session) -> dict:
        updated = 0
        stocks = db.query(models.Stock).all()
        for stock in stocks:
            try:
                result = self.refresh_for_stock(db, stock.id)
                if result:
                    updated += 1
            except Exception:
                db.rollback()
                continue
        return {"updated": updated, "timestamp": datetime.utcnow()}

    def _latest_recommendation(self, symbol: str) -> Optional[dict]:
        data = self.client.get("/stock/recommendation", {"symbol": symbol}) or []
        if not data:
            return None
        return sorted(data, key=lambda item: item.get("period") or "", reverse=True)[0]

    @staticmethod
    def _parse_period(period_str: Optional[str]) -> Optional[date]:
        if not period_str:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m"):
            try:
                return datetime.strptime(period_str, fmt).date()
            except ValueError:
                continue
        return None

    def _published_at(self, recommendation: Optional[dict], fallback: Optional[datetime]) -> datetime:
        period = self._parse_period(recommendation.get("period")) if recommendation else None
        if isinstance(period, date):
            return datetime.combine(period, datetime.min.time())
        return datetime.utcnow()

    @staticmethod
    def _rating_label(recommendation: Optional[dict]) -> str:
        if not recommendation:
            return "unknown"
        strong_buy = recommendation.get("strongBuy", 0) or 0
        buy = recommendation.get("buy", 0) or 0
        hold = recommendation.get("hold", 0) or 0
        sell = recommendation.get("sell", 0) or 0
        strong_sell = recommendation.get("strongSell", 0) or 0

        score = 2 * strong_buy + buy - sell - 2 * strong_sell
        total = strong_buy + buy + hold + sell + strong_sell
        if total == 0:
            return "unknown"
        sentiment = score / total
        if sentiment >= 0.5:
            return "buy"
        if sentiment <= -0.5:
            return "sell"
        return "hold"
