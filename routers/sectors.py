from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from database import get_db
import models
import schemas

router = APIRouter()


@router.get("/", response_model=List[schemas.Sector])
def get_sectors(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    sectors = db.query(models.Sector).offset(skip).limit(limit).all()
    return sectors


@router.get("/{sector_id}", response_model=schemas.Sector)
def get_sector(sector_id: int, db: Session = Depends(get_db)):
    sector = db.query(models.Sector).filter(models.Sector.id == sector_id).first()
    if not sector:
        raise HTTPException(status_code=404, detail="Sector not found")
    return sector


@router.post("/", response_model=schemas.Sector)
def create_sector(sector: schemas.SectorCreate, db: Session = Depends(get_db)):
    # Check if sector already exists
    existing_sector = (
        db.query(models.Sector).filter(models.Sector.name == sector.name).first()
    )
    if existing_sector:
        raise HTTPException(
            status_code=400, detail="Sector with this name already exists"
        )

    db_sector = models.Sector(**sector.dict())
    db.add(db_sector)
    db.commit()
    db.refresh(db_sector)
    return db_sector


@router.delete("/{sector_id}")
def delete_sector(sector_id: int, db: Session = Depends(get_db)):
    db_sector = db.query(models.Sector).filter(models.Sector.id == sector_id).first()
    if not db_sector:
        raise HTTPException(status_code=404, detail="Sector not found")

    # Check if sector has associated stocks
    if db_sector.stocks:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete sector with associated stocks. Remove stocks first.",
        )

    db.delete(db_sector)
    db.commit()
    return {"message": "Sector deleted successfully"}
