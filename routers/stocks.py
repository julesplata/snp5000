from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import desc, or_
from typing import List, Optional
from database import get_db
import models
import schemas

router = APIRouter()


@router.get("/", response_model=List[schemas.StockWithLatestRating])
def get_stocks(
    page: int = Query(1, ge=10),
    page_size: int = Query(10, ge=1, le=100),
    sector_id: Optional[int] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Get all stocks with optional filtering"""
    query = db.query(models.Stock).options(
        joinedload(models.Stock.sector), joinedload(models.Stock.ratings)
    )

    # Apply filters
    if sector_id:
        query = query.filter(models.Stock.sector_id == sector_id)

    if search:
        query = query.filter(
            or_(
                models.Stock.symbol.ilike(f"%{search}%"),
                models.Stock.name.ilike(f"%{search}%"),
            )
        )

    stocks = query.offset(page).limit(page_size).all()

    # Add latest rating and trend to each stock
    result = []
    for stock in stocks:
        stock_dict = schemas.Stock.from_orm(stock).dict()

        if stock.ratings:
            # Get latest rating
            latest_rating = max(stock.ratings, key=lambda r: r.rating_date)
            stock_dict["latest_rating"] = schemas.Rating.model_validate(latest_rating).model_dump()

            # Calculate trend
            if len(stock.ratings) >= 2:
                sorted_ratings = sorted(
                    stock.ratings, key=lambda r: r.rating_date, reverse=True
                )
                if sorted_ratings[0].overall_rating > sorted_ratings[1].overall_rating:
                    stock_dict["rating_trend"] = "up"
                elif (
                    sorted_ratings[0].overall_rating < sorted_ratings[1].overall_rating
                ):
                    stock_dict["rating_trend"] = "down"
                else:
                    stock_dict["rating_trend"] = "stable"
            else:
                stock_dict["rating_trend"] = "new"

            # Apply rating filters
            if min_rating and latest_rating.overall_rating < min_rating:
                continue
            if max_rating and latest_rating.overall_rating > max_rating:
                continue
        else:
            stock_dict["latest_rating"] = None
            stock_dict["rating_trend"] = None

        result.append(schemas.StockWithLatestRating(**stock_dict))

    return result


@router.get("/{stock_id}", response_model=schemas.StockWithLatestRating)
def get_stock(stock_id: int, db: Session = Depends(get_db)):
    stock = (
        db.query(models.Stock)
        .options(joinedload(models.Stock.sector), joinedload(models.Stock.ratings))
        .filter(models.Stock.id == stock_id)
        .first()
    )

    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    stock_dict = schemas.Stock.from_orm(stock).dict()

    if stock.ratings:
        latest_rating = max(stock.ratings, key=lambda r: r.rating_date)
        stock_dict["latest_rating"] = schemas.Rating.from_orm(latest_rating).dict()

        if len(stock.ratings) >= 2:
            sorted_ratings = sorted(
                stock.ratings, key=lambda r: r.rating_date, reverse=True
            )
            if sorted_ratings[0].overall_rating > sorted_ratings[1].overall_rating:
                stock_dict["rating_trend"] = "up"
            elif sorted_ratings[0].overall_rating < sorted_ratings[1].overall_rating:
                stock_dict["rating_trend"] = "down"
            else:
                stock_dict["rating_trend"] = "stable"
        else:
            stock_dict["rating_trend"] = "new"
    else:
        stock_dict["latest_rating"] = None
        stock_dict["rating_trend"] = None

    return schemas.StockWithLatestRating(**stock_dict)


@router.post("/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    """Create a new stock"""
    # Check if stock already exists
    existing_stock = (
        db.query(models.Stock).filter(models.Stock.symbol == stock.symbol).first()
    )
    if existing_stock:
        raise HTTPException(
            status_code=400, detail="Stock with this symbol already exists"
        )

    db_stock = models.Stock(**stock.dict())
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock


@router.put("/{stock_id}", response_model=schemas.Stock)
def update_stock(
    stock_id: int, stock: schemas.StockUpdate, db: Session = Depends(get_db)
):
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    update_data = stock.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_stock, field, value)

    db.commit()
    db.refresh(db_stock)
    return db_stock


@router.delete("/{stock_id}")
def delete_stock(stock_id: int, db: Session = Depends(get_db)):
    """Delete a stock"""
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    db.delete(db_stock)
    db.commit()
    return {"message": "Stock deleted successfully"}


@router.get("/{stock_id}/history", response_model=schemas.RatingHistoryResponse)
def get_stock_rating_history(stock_id: int, db: Session = Depends(get_db)):
    """Get rating history for a stock"""
    stock = (
        db.query(models.Stock)
        .options(joinedload(models.Stock.sector))
        .filter(models.Stock.id == stock_id)
        .first()
    )

    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    ratings = (
        db.query(models.Rating)
        .filter(models.Rating.stock_id == stock_id)
        .order_by(desc(models.Rating.rating_date))
        .all()
    )

    return {"stock": stock, "ratings": ratings}
