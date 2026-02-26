from typing import List
from sqlalchemy.orm import Session
import app.models as models


def update_quote(db: Session, stock: models.Stock, price: float, market_cap: float):
    stock.current_price = price
    stock.market_cap = round(float(market_cap), 2) if market_cap is not None else None
    db.add(stock)
    db.commit()
    db.refresh(stock)
    return stock


def list_stocks(db: Session) -> List[models.Stock]:
    return db.query(models.Stock).all()
