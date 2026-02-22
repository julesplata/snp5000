from fastapi import HTTPException
from sqlalchemy import desc, or_
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional

import models
import schemas


def list_stocks(
    db: Session,
    page: int,
    page_size: int,
    sector_id: Optional[int],
    min_rating: Optional[float],
    max_rating: Optional[float],
    search: Optional[str],
) -> List[schemas.StockWithLatestRating]:
    query = db.query(models.Stock).options(
        joinedload(models.Stock.sector), joinedload(models.Stock.ratings)
    )

    if sector_id:
        query = query.filter(models.Stock.sector_id == sector_id)

    if search:
        query = query.filter(
            or_(
                models.Stock.symbol.ilike(f"%{search}%"),
                models.Stock.name.ilike(f"%{search}%"),
            )
        )

    offset_value = (page - 1) * page_size
    stocks = query.offset(offset_value).limit(page_size).all()

    result = []
    for stock in stocks:
        stock_dict = schemas.Stock.model_validate(stock).model_dump()

        if stock.ratings:
            sorted_ratings = sorted(
                stock.ratings, key=lambda r: r.rating_date, reverse=True
            )
            latest_rating = sorted_ratings[0]

            if min_rating and latest_rating.overall_rating < min_rating:
                continue
            if max_rating and latest_rating.overall_rating > max_rating:
                continue

            stock_dict["latest_rating"] = schemas.Rating.model_validate(
                latest_rating
            ).model_dump()

            if len(sorted_ratings) >= 2:
                curr, prev = (
                    sorted_ratings[0].overall_rating,
                    sorted_ratings[1].overall_rating,
                )
                if curr > prev:
                    stock_dict["rating_trend"] = "up"
                elif curr < prev:
                    stock_dict["rating_trend"] = "down"
                else:
                    stock_dict["rating_trend"] = "stable"
            else:
                stock_dict["rating_trend"] = "new"
        else:
            if min_rating or max_rating:
                continue
            stock_dict["latest_rating"] = None
            stock_dict["rating_trend"] = None

        result.append(schemas.StockWithLatestRating(**stock_dict))

    return result


def get_stock(db: Session, stock_id: int) -> schemas.StockWithLatestRating:
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


def create_stock(db: Session, stock: schemas.StockCreate) -> models.Stock:
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


def update_stock(db: Session, stock_id: int, stock: schemas.StockUpdate) -> models.Stock:
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    update_data = stock.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_stock, field, value)

    db.commit()
    db.refresh(db_stock)
    return db_stock


def delete_stock(db: Session, stock_id: int) -> None:
    db_stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not db_stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    db.delete(db_stock)
    db.commit()


def get_rating_history(db: Session, stock_id: int) -> schemas.RatingHistoryResponse:
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

    return schemas.RatingHistoryResponse(stock=stock, ratings=ratings)
