from fastapi import HTTPException
from sqlalchemy import desc
from sqlalchemy.orm import Session
from typing import List, Optional

import app.models as models
import app.schemas as schemas


def list_ratings(
    db: Session, skip: int = 0, limit: int = 100, stock_id: Optional[int] = None
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


def create_rating(db: Session, payload: schemas.RatingCreate) -> models.Rating:
    rating = models.Rating(**payload.model_dump())
    db.add(rating)
    db.commit()
    db.refresh(rating)
    return rating
