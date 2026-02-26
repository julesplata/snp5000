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

    def fetch_and_store_company_news(self, db: Session, stock_id: int):
        stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")

        symbol = stock.symbol
        # Finnhub company-news requires date range; free tier supports recent 30 days.
        to_date = datetime.utcnow().date()
        from_date = to_date - timedelta(days=3)

        data = self.client.get(
            "/company-news",
            {
                "symbol": symbol,
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
            },
        )
        if not data:
            return 0

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
                    "category": item.get("category"),
                }
            )

        return news_crud.upsert_articles(db, stock_id, articles)
