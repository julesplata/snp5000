from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List, Optional

from database import get_db
import app.schemas as schemas
import app.crud.stock as stock_crud

router = APIRouter()


@router.get("/", response_model=List[schemas.StockWithLatestRating])
def list_stocks(
    page: int = 1,
    page_size: int = 10,
    sector_id: Optional[int] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
):
    return stock_crud.list_stocks(
        db=db,
        page=page,
        page_size=page_size,
        sector_id=sector_id,
        min_rating=min_rating,
        max_rating=max_rating,
        search=search,
    )


@router.get("/{stock_id}", response_model=schemas.StockWithLatestRating)
def get_stock(stock_id: int, db: Session = Depends(get_db)):
    return stock_crud.get_stock(db, stock_id)


@router.post("/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    return stock_crud.create_stock(db, stock)


@router.get("/{stock_id}/history", response_model=schemas.RatingHistoryResponse)
def get_stock_rating_history(stock_id: int, db: Session = Depends(get_db)):
    return stock_crud.get_rating_history(db, stock_id)
