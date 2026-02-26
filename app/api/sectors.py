from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List

from database import get_db
import app.schemas as schemas
import app.crud.sector as sector_crud

router = APIRouter()


@router.get("/", response_model=List[schemas.Sector])
def list_sectors(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return sector_crud.list_sectors(db, skip, limit)


@router.get("/{sector_id}", response_model=schemas.Sector)
def get_sector(sector_id: int, db: Session = Depends(get_db)):
    return sector_crud.get_sector(db, sector_id)

