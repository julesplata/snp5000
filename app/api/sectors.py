from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List

from database import get_db
import app.models as models
import app.schemas as schemas
import app.crud.sector as sector_crud

router = APIRouter()


@router.get("/", response_model=List[schemas.Sector])
def list_sectors(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return sector_crud.list_sectors(db, skip, limit)


@router.get("/{sector_id}", response_model=schemas.Sector)
def get_sector(sector_id: int, db: Session = Depends(get_db)):
    return sector_crud.get_sector(db, sector_id)


@router.get(
    "/{sector_id}/economic-ratings",
    response_model=List[schemas.SectorEconomicRatingResponse],
)
def list_sector_economic_ratings(
    sector_id: int,
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    return (
        db.query(models.SectorEconomicRating)
        .filter(models.SectorEconomicRating.sector_id == sector_id)
        .order_by(models.SectorEconomicRating.rated_at.desc())
        .limit(limit)
        .all()
    )


@router.get(
    "/{sector_id}/economic-ratings/latest",
    response_model=schemas.SectorEconomicRatingResponse,
)
def get_latest_sector_economic_rating(
    sector_id: int,
    db: Session = Depends(get_db),
):
    row = (
        db.query(models.SectorEconomicRating)
        .filter(models.SectorEconomicRating.sector_id == sector_id)
        .order_by(models.SectorEconomicRating.rated_at.desc())
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="No economic rating found for sector")
    return row
