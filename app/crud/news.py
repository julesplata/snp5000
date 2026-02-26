from datetime import datetime
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import desc

import app.models as models
import app.schemas as schemas


def _r2(val):
    return (
        round(float(val), 2)
        if isinstance(val, (int, float)) and val is not None
        else val
    )


def list_news(
    db: Session, stock_id: int, skip: int = 0, limit: int = 50
) -> List[models.NewsArticle]:
    return (
        db.query(models.NewsArticle)
        .filter(models.NewsArticle.stock_id == stock_id)
        .order_by(desc(models.NewsArticle.published_at))
        .offset(skip)
        .limit(limit)
        .all()
    )


def upsert_articles(db: Session, stock_id: int, articles: List[dict]) -> int:
    inserted = 0
    for art in articles:
        if (
            db.query(models.NewsArticle)
            .filter(models.NewsArticle.url == art["url"])
            .first()
        ):
            continue  # skip duplicates by URL
        db_article = models.NewsArticle(
            stock_id=stock_id,
            title=art["title"],
            summary=art.get("summary"),
            content=art.get("content"),
            url=art["url"],
            source=art.get("source"),
            author=art.get("author"),
            published_at=art.get("published_at"),
            sentiment_score=_r2(art.get("sentiment_score")),
            sentiment_label=art.get("sentiment_label"),
            category=art.get("category"),
            fetched_at=datetime.utcnow(),
        )
        db.add(db_article)
        inserted += 1
    db.commit()
    return inserted


def summarize_news(db: Session, stock_id: int, limit: int = 20) -> dict:
    articles = list_news(db, stock_id, limit)
    headlines = [a.title for a in articles]
    sources = {}
    for a in articles:
        sources[a.source] = sources.get(a.source, 0) + 1
    return {
        "count": len(articles),
        "headlines": headlines,
        "sources": sources,
        "latest_published_at": articles[0].published_at if articles else None,
    }
