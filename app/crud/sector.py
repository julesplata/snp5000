from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List

import app.models as models
import app.schemas as schemas


def list_sectors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sector]:
    return db.query(models.Sector).offset(skip).limit(limit).all()


def get_sector(db: Session, sector_id: int) -> models.Sector:
    sector = db.query(models.Sector).filter(models.Sector.id == sector_id).first()
    if not sector:
        raise HTTPException(status_code=404, detail="Sector not found")
    return sector

