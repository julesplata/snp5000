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
def list_news(stock_id: int, limit: int = 50, db: Session = Depends(get_db)):
    articles = news_crud.list_news(db, stock_id, limit)
    return articles


@router.get("/stocks/{stock_id}/news/summary")
def news_summary(stock_id: int, limit: int = 20, db: Session = Depends(get_db)):
    return news_crud.summarize_news(db, stock_id, limit)


@router.post("/stocks/{stock_id}/news/refresh")
def refresh_news(stock_id: int, db: Session = Depends(get_db)):
    inserted = service.fetch_and_store_company_news(db, stock_id)
    return {"inserted": inserted}
