from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List

import models
import schemas


def list_sectors(db: Session, skip: int, limit: int) -> List[models.Sector]:
    return db.query(models.Sector).offset(skip).limit(limit).all()


def get_sector(db: Session, sector_id: int) -> models.Sector:
    sector = db.query(models.Sector).filter(models.Sector.id == sector_id).first()
    if not sector:
        raise HTTPException(status_code=404, detail="Sector not found")
    return sector


def create_sector(db: Session, sector: schemas.SectorCreate) -> models.Sector:
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


def delete_sector(db: Session, sector_id: int) -> None:
    db_sector = db.query(models.Sector).filter(models.Sector.id == sector_id).first()
    if not db_sector:
        raise HTTPException(status_code=404, detail="Sector not found")

    if db_sector.stocks:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete sector with associated stocks. Remove stocks first.",
        )

    db.delete(db_sector)
    db.commit()
