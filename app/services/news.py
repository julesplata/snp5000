from datetime import datetime, timedelta
from fastapi import HTTPException
from sqlalchemy.orm import Session

import app.models as models
import app.crud.news as news_crud
from app.utils.rating_utils import FinnhubClient
from config import get_settings


class NewsService:
    def __init__(self, finnhub_api_key: str | None = None):
        settings = get_settings()
        self.client = FinnhubClient(
            finnhub_api_key or settings.finnhub_api_key, max_per_minute=55
        )

    def fetch_and_store_company_news(
        self, db: Session, stock_id: int, lookback_hours: int = 12
    ):
        stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")

        articles = self._fetch_company_news_data(stock.symbol, lookback_hours)
        return news_crud.upsert_articles(db, stock_id, articles)

    def fetch_and_store_all_company_news(
        self, db: Session, lookback_hours: int = 12
    ) -> dict:
        stocks = db.query(models.Stock).all()
        total_inserted = 0

        for stock in stocks:
            articles = self._fetch_company_news_data(stock.symbol, lookback_hours)
            total_inserted += news_crud.upsert_articles(db, stock.id, articles)

        return {"stocks_processed": len(stocks), "inserted": total_inserted}

    def _fetch_company_news_data(self, symbol: str, lookback_hours: int):
        # Finnhub company-news requires date range; free tier supports recent 30 days.
        now = datetime.utcnow()
        to_date = now.date()
        from_date = (now - timedelta(hours=lookback_hours)).date()

        data = self.client.get(
            "/company-news",
            {
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
            },
        )
        if not data:
            return []

        articles = []
        for item in data:
            published = datetime.utcfromtimestamp(item["datetime"])
            articles.append(
                {
                    "title": item.get("headline"),
                    "summary": item.get("summary"),
                    "content": None,
                    "url": item.get("url"),
                    "source": item.get("source"),
                    "author": None,
                    "published_at": published,
                    "sentiment_score": None,
                    "sentiment_label": None,
                    "category": self._normalize_category(item.get("category")),
                }
            )
        return articles

    @staticmethod
    def _normalize_category(category: str | None) -> str | None:
        """Map provider categories into our allowed set to avoid response validation errors."""
        if not category:
            return None
        allowed = {"earnings", "merger", "product", "guidance", "general", "company"}
        value = category.lower()
        if value in allowed:
            return value
        # Default any unknown bucket to "general" so responses stay valid
        return "general"
