from typing import List, Optional
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc

import models
import schemas
from utils.rating_utils import RatingService as RatingCalculator


def list_ratings(
    db: Session, skip: int, limit: int, stock_id: Optional[int]
) -> List[models.Rating]:
    query = db.query(models.Rating)

    if stock_id:
        query = query.filter(models.Rating.stock_id == stock_id)

    return (
        query.order_by(desc(models.Rating.rating_date)).offset(skip).limit(limit).all()
    )


def get_rating(db: Session, rating_id: int) -> models.Rating:
    rating = db.query(models.Rating).filter(models.Rating.id == rating_id).first()
    if not rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    return rating


def calculate_and_save_rating(db: Session, stock_id: int) -> models.Rating:
    stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    service = RatingCalculator(db_session=db)
    rating_data = service.calculate_rating(stock.symbol, db=db)
    if not rating_data:
        raise HTTPException(
            status_code=503, detail="Unable to calculate rating at this time"
        )

    db_rating = models.Rating(
        stock_id=stock_id,
        overall_rating=rating_data["overall_rating"],
        technical_score=rating_data.get("technical_score"),
        analyst_score=rating_data.get("analyst_score"),
        fundamental_score=rating_data.get("fundamental_score"),
        macro_score=rating_data.get("macro_score"),
        data_sources=rating_data.get("data_sources"),
        notes="Auto-generated rating (Finnhub)",
    )

    db.add(db_rating)
    db.commit()
    db.refresh(db_rating)
    return db_rating


# Backwards-compat export for callers expecting RatingService class
RatingService = RatingCalculator
