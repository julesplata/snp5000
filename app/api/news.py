from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from database import get_db
import app.schemas as schemas
import app.crud.news as news_crud
from app.services.news import NewsService

router = APIRouter()
service = NewsService()


@router.get("/stocks/{stock_id}/news", response_model=List[schemas.NewsArticle])
def list_news(
    stock_id: int,
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    articles = news_crud.list_news(db, stock_id, skip=skip, limit=limit)
    return articles


@router.get("/stocks/{stock_id}/news/summary", response_model=schemas.NewsSummary)
def news_summary(stock_id: int, limit: int = 20, db: Session = Depends(get_db)):
    return news_crud.summarize_news(db, stock_id, limit)


@router.post("/stocks/news/refresh")
def refresh_all_news(lookback_hours: int = 12, db: Session = Depends(get_db)):
    """Refresh news for all stocks; trims any previously stored articles not in the latest fetch."""
    return service.fetch_and_store_all_company_news(db, lookback_hours=lookback_hours)
