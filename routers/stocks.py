from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from database import get_db
import schemas
from services import stock_service

router = APIRouter()


@router.get("/", response_model=List[schemas.StockWithLatestRating])
def get_stocks(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    sector_id: Optional[int] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Get all stocks with optional filtering and pagination"""
    return stock_service.list_stocks(
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
    return stock_service.get_stock(db, stock_id)


@router.post("/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    """Create a new stock"""
    return stock_service.create_stock(db, stock)


@router.put("/{stock_id}", response_model=schemas.Stock)
def update_stock(
    stock_id: int, stock: schemas.StockUpdate, db: Session = Depends(get_db)
):
    return stock_service.update_stock(db, stock_id, stock)


@router.delete("/{stock_id}")
def delete_stock(stock_id: int, db: Session = Depends(get_db)):
    """Delete a stock"""
    stock_service.delete_stock(db, stock_id)
    return {"message": "Stock deleted successfully"}


@router.get("/{stock_id}/history", response_model=schemas.RatingHistoryResponse)
def get_stock_rating_history(stock_id: int, db: Session = Depends(get_db)):
    """Get rating history for a stock"""
    return stock_service.get_rating_history(db, stock_id)
