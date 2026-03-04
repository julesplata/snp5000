from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from database import get_db
import app.schemas as schemas
import app.crud.analyst as analyst_crud
from app.services.analyst import AnalystService

router = APIRouter()
service = AnalystService()


@router.get(
    "/stocks/{stock_id}/analyst-ratings",
    response_model=List[schemas.AnalystRating],
)
def list_analyst_ratings(
    stock_id: int, skip: int = 0, limit: int = 50, db: Session = Depends(get_db)
):
    return analyst_crud.list_ratings(db, stock_id, skip=skip, limit=limit)


@router.get(
    "/stocks/{stock_id}/analyst-consensus",
    response_model=Optional[schemas.AnalystConsensus],
)
def get_analyst_consensus(stock_id: int, db: Session = Depends(get_db)):
    consensus = analyst_crud.latest_consensus(db, stock_id)
    if not consensus:
        raise HTTPException(status_code=404, detail="Consensus not found")
    return consensus


@router.post(
    "/stocks/{stock_id}/analyst-ratings/refresh",
    response_model=schemas.AnalystRefreshResponse,
)
def refresh_analyst(stock_id: int, db: Session = Depends(get_db)):
    result = service.refresh_for_stock(db, stock_id)
    return schemas.AnalystRefreshResponse.model_validate(result)


@router.post("/stocks/analyst-consensus/refresh")
def refresh_all_analyst(db: Session = Depends(get_db)):
    result = service.refresh_all(db)
    if result.get("updated", 0) == 0:
        raise HTTPException(status_code=503, detail="No analyst data updated")
    return result
