from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from database import get_db
import app.schemas as schemas
import app.crud.rating as rating_crud
import app.services.rating as rating_service

router = APIRouter()


@router.get("/", response_model=List[schemas.Rating])
def list_ratings(
    skip: int = 0, limit: int = 100, stock_id: Optional[int] = None, db: Session = Depends(get_db)
):
    return rating_crud.list_ratings(db, skip=skip, limit=limit, stock_id=stock_id)


@router.get("/{rating_id}", response_model=schemas.Rating)
def get_rating(rating_id: int, db: Session = Depends(get_db)):
    rating = rating_crud.get_rating(db, rating_id)
    if not rating:
        raise HTTPException(status_code=404, detail="Rating not found")
    return rating


@router.post("/calculate/{stock_id}", response_model=schemas.Rating)
def calculate_and_save_rating(stock_id: int, db: Session = Depends(get_db)):
    return rating_service.calculate_and_store_rating(db, stock_id)
