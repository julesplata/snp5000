from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import List
from database import get_db
import models
import schemas

router = APIRouter()


@router.get("/", response_model=List[schemas.Rating])
def get_ratings(
    skip: int = 0, limit: int = 100, stock_id: int = None, db: Session = Depends(get_db)
):
    query = db.query(models.Rating)

    if stock_id:
        query = query.filter(models.Rating.stock_id == stock_id)

    ratings = (
        query.order_by(desc(models.Rating.rating_date)).offset(skip).limit(limit).all()
    )
    return ratings


@router.get("/{rating_id}", response_model=schemas.Rating)
def get_rating(rating_id: int, db: Session = Depends(get_db)):
    rating = db.query(models.Rating).filter(models.Rating.id == rating_id).first()
    if not rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    return rating


@router.post("/calculate/{stock_id}", response_model=schemas.Rating)
def calculate_and_save_rating(stock_id: int, db: Session = Depends(get_db)):
    stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    from services.rating_service import RatingService

    service = RatingService(db_session=db)
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
