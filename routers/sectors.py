from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import schemas
from services import sector_service

router = APIRouter()


@router.get("/", response_model=List[schemas.Sector])
def get_sectors(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return sector_service.list_sectors(db, skip, limit)


@router.get("/{sector_id}", response_model=schemas.Sector)
def get_sector(sector_id: int, db: Session = Depends(get_db)):
    return sector_service.get_sector(db, sector_id)


@router.post("/", response_model=schemas.Sector)
def create_sector(sector: schemas.SectorCreate, db: Session = Depends(get_db)):
    return sector_service.create_sector(db, sector)


@router.delete("/{sector_id}")
def delete_sector(sector_id: int, db: Session = Depends(get_db)):
    sector_service.delete_sector(db, sector_id)
    return {"message": "Sector deleted successfully"}
