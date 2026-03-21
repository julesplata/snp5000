from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Literal, Optional

from database import get_db
import app.schemas as schemas
import app.crud.stock as stock_crud

router = APIRouter()


@router.get(
    "/",
    response_model=List[schemas.StockWithLatestRating],
    response_model_exclude={"latest_rating": {"data_sources"}},
)
def list_stocks(
    skip: int = 0,
    limit: int = 10,
    sector_id: Optional[int] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    search: Optional[str] = None,
    sort_by: Literal["name", "symbol", "market_cap", "rating", "created_at"] = Query(
        "rating", description="Field to sort by"
    ),
    sort_dir: Literal["asc", "desc"] = Query("desc", description="Sort direction"),
    db: Session = Depends(get_db),
):
    return stock_crud.list_stocks(
        db=db,
        skip=skip,
        limit=limit,
        sector_id=sector_id,
        min_rating=min_rating,
        max_rating=max_rating,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.get(
    "/{stock_id}",
    response_model=schemas.StockWithLatestRating,
    response_model_exclude={"latest_rating": {"data_sources"}},
)
def get_stock(stock_id: int, db: Session = Depends(get_db)):
    return stock_crud.get_stock(db, stock_id)


@router.post("/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    return stock_crud.create_stock(db, stock)


@router.get(
    "/{stock_id}/history",
    response_model=schemas.RatingHistoryResponse,
    response_model_exclude={"ratings": {"__all__": {"data_sources"}}},
)
def get_stock_rating_history(stock_id: int, db: Session = Depends(get_db)):
    return stock_crud.get_rating_history(db, stock_id)
