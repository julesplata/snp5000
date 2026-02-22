from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import schemas
from services import rating_service

router = APIRouter()


@router.get("/", response_model=List[schemas.Rating])
def get_ratings(
    skip: int = 0, limit: int = 100, stock_id: int = None, db: Session = Depends(get_db)
):
    return rating_service.list_ratings(db, skip, limit, stock_id)


@router.get("/{rating_id}", response_model=schemas.Rating)
def get_rating(rating_id: int, db: Session = Depends(get_db)):
    return rating_service.get_rating(db, rating_id)


@router.post("/calculate/{stock_id}", response_model=schemas.Rating)
def calculate_and_save_rating(stock_id: int, db: Session = Depends(get_db)):
    return rating_service.calculate_and_save_rating(db, stock_id)
