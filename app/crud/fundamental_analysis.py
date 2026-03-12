from datetime import datetime
from typing import Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

import app.models as models
import app.schemas as schemas


def latest(
    db: Session, stock_id: int, since: Optional[datetime] = None
) -> Optional[models.FundamentalAnalysis]:
    query = db.query(models.FundamentalAnalysis).filter(
        models.FundamentalAnalysis.stock_id == stock_id
    )
    if since:
        query = query.filter(models.FundamentalAnalysis.analyzed_at >= since)
    return query.order_by(desc(models.FundamentalAnalysis.analyzed_at)).first()


def upsert(
    db: Session, payload: schemas.FundamentalAnalysisCreate
) -> models.FundamentalAnalysis:
    data = payload.model_dump()
    existing = (
        db.query(models.FundamentalAnalysis)
        .filter(models.FundamentalAnalysis.stock_id == data["stock_id"])
        .first()
    )
    if existing:
        for k, v in data.items():
            setattr(existing, k, v)
        db.commit()
        db.refresh(existing)
        return existing
    record = models.FundamentalAnalysis(**data)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record
