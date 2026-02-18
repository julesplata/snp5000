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


@router.post("/", response_model=schemas.Rating)
def create_rating(rating: schemas.RatingCreate, db: Session = Depends(get_db)):
    # Verify stock exists
    stock = db.query(models.Stock).filter(models.Stock.id == rating.stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    db_rating = models.Rating(**rating.dict())
    db.add(db_rating)
    db.commit()
    db.refresh(db_rating)
    return db_rating


@router.delete("/{rating_id}")
def delete_rating(rating_id: int, db: Session = Depends(get_db)):
    """Delete a rating"""
    db_rating = db.query(models.Rating).filter(models.Rating.id == rating_id).first()
    if not db_rating:
        raise HTTPException(status_code=404, detail="Rating not found")

    db.delete(db_rating)
    db.commit()
    return {"message": "Rating deleted successfully"}


@router.post("/calculate/{stock_id}", response_model=schemas.Rating)
def calculate_and_save_rating(stock_id: int, db: Session = Depends(get_db)):
    stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    # Placeholder calculation - replace with actual logic
    # You would fetch data from yfinance, analyst APIs, etc.
    technical_score = 7.5  # Calculate from technical indicators
    analyst_score = 8.0  # Calculate from analyst ratings
    fundamental_score = 6.5  # Calculate from fundamentals
    momentum_score = 7.0  # Calculate from price momentum

    # Weighted average
    overall_rating = (
        technical_score * 0.3
        + analyst_score * 0.3
        + fundamental_score * 0.25
        + momentum_score * 0.15
    )

    db_rating = models.Rating(
        stock_id=stock_id,
        overall_rating=round(overall_rating, 2),
        technical_score=technical_score,
        analyst_score=analyst_score,
        fundamental_score=fundamental_score,
        momentum_score=momentum_score,
        data_sources={
            "technical": "calculated",
            "analyst": "placeholder",
            "fundamental": "placeholder",
            "momentum": "calculated",
        },
        notes="Auto-generated rating",
    )

    db.add(db_rating)
    db.commit()
    db.refresh(db_rating)
    return db_rating
