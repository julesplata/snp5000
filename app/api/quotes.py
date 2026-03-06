from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
import app.schemas as schemas
from app.services.quote import QuoteService

router = APIRouter()
service = QuoteService()


@router.post("/stocks/{stock_id}/refresh-quote", response_model=schemas.Stock)
def refresh_quote(stock_id: int, db: Session = Depends(get_db)):
    updated = service.refresh_quote(db, stock_id)
    if not updated:
        raise HTTPException(
            status_code=404, detail="Stock not found or quote unavailable"
        )
    return updated
