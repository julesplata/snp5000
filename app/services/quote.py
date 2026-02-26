from typing import Optional
from sqlalchemy.orm import Session
from datetime import datetime
import app.crud.quote as quote_crud
import app.models as models
from app.utils.rating_utils import FinnhubClient
from config import get_settings


class QuoteService:
    def __init__(self, finnhub_api_key: Optional[str] = None):
        settings = get_settings()
        self.client = FinnhubClient(
            finnhub_api_key or settings.finnhub_api_key, max_per_minute=55
        )

    def refresh_all_quotes(self, db: Session):
        updated = 0
        stocks = quote_crud.list_stocks(db)
        for stock in stocks:
            price, market_cap = self._fetch_quote_and_cap(stock.symbol)
            if price is None and market_cap is None:
                continue
            quote_crud.update_quote(db, stock, price, market_cap)
            updated += 1
        return {"updated": updated, "timestamp": datetime.utcnow()}

    def refresh_quote(self, db: Session, stock_id: int):
        stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
        if not stock:
            return None
        price, market_cap = self._fetch_quote_and_cap(stock.symbol)
        if price is None and market_cap is None:
            return None
        return quote_crud.update_quote(db, stock, price, market_cap)

    def _fetch_quote_and_cap(self, symbol: str):
        quote = self.client.get("/quote", {"symbol": symbol}) or {}
        price = quote.get("c")
        # Finnhub profile2 has market cap
        profile = self.client.get("/stock/profile2", {"symbol": symbol}) or {}
        market_cap = profile.get("marketCapitalization")
        if market_cap is not None:
            market_cap = round(float(market_cap), 2)
        return price, market_cap
