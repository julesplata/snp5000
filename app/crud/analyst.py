from typing import List, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy import desc

import app.models as models
import app.schemas as schemas


def _r2(val):
    return round(float(val), 2) if isinstance(val, (int, float)) and val is not None else val


# Granular analyst ratings
def list_ratings(
    db: Session, stock_id: int, skip: int = 0, limit: int = 50
) -> List[models.AnalystRating]:
    return (
        db.query(models.AnalystRating)
        .filter(models.AnalystRating.stock_id == stock_id)
        .order_by(desc(models.AnalystRating.published_at))
        .offset(skip)
        .limit(limit)
        .all()
    )


def upsert_rating(
    db: Session, payload: Union[schemas.AnalystRatingCreate, dict]
) -> models.AnalystRating:
    data = payload.model_dump() if isinstance(payload, schemas.AnalystRatingCreate) else dict(payload)
    stock_id = data["stock_id"]
    published_at = data.get("published_at")
    source = data.get("source")

    query = db.query(models.AnalystRating).filter(models.AnalystRating.stock_id == stock_id)
    if published_at:
        query = query.filter(models.AnalystRating.published_at == published_at)
    if source:
        query = query.filter(models.AnalystRating.source == source)

    existing = query.first()
    if existing:
        for key, value in data.items():
            if hasattr(existing, key) and value is not None:
                setattr(existing, key, _r2(value) if isinstance(value, float) else value)
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return existing

    data["target_price"] = _r2(data.get("target_price"))
    record = models.AnalystRating(**data)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


# Consensus snapshots
def latest_consensus(db: Session, stock_id: int) -> Optional[models.AnalystConsensus]:
    return (
        db.query(models.AnalystConsensus)
        .filter(models.AnalystConsensus.stock_id == stock_id)
        .order_by(desc(models.AnalystConsensus.last_updated))
        .first()
    )


def upsert_consensus(
    db: Session, payload: Union[schemas.AnalystConsensusCreate, dict]
) -> models.AnalystConsensus:
    data = payload.model_dump() if isinstance(payload, schemas.AnalystConsensusCreate) else dict(payload)
    stock_id = data["stock_id"]
    last_updated = data.get("last_updated")

    query = db.query(models.AnalystConsensus).filter(models.AnalystConsensus.stock_id == stock_id)
    if last_updated:
        query = query.filter(models.AnalystConsensus.last_updated == last_updated)

    existing = query.first()
    if existing:
        for key, value in data.items():
            if hasattr(existing, key) and value is not None:
                setattr(existing, key, _r2(value) if isinstance(value, float) else value)
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return existing

    data["target_mean"] = _r2(data.get("target_mean"))
    record = models.AnalystConsensus(**data)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record
