from fastapi import HTTPException
from sqlalchemy.orm import Session

import app.crud.rating as rating_crud
import app.schemas as schemas
from app.utils.rating_utils import RatingService as RatingEngine
import app.models as models


def calculate_and_store_rating(db: Session, stock_id: int):
    stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    engine = RatingEngine(db_session=db)
    data = engine.calculate_rating(stock.symbol, db=db)
    if not data:
        raise HTTPException(status_code=503, detail="Unable to calculate rating now")

    payload = schemas.RatingCreate(
        stock_id=stock.id,
        overall_rating=data["overall_rating"],
        technical_score=data.get("technical_score"),
        analyst_score=data.get("analyst_score"),
        fundamental_score=data.get("fundamental_score"),
        macro_score=data.get("macro_score"),
        data_sources=data.get("data_sources"),
        notes="Auto-generated rating (Finnhub)",
    )
    return rating_crud.create_rating(db, payload)
