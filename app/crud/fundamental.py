from datetime import datetime
from typing import Optional, Union

from sqlalchemy import desc
from sqlalchemy.orm import Session

import app.models as models
import app.schemas as schemas


def latest(
    db: Session, stock_id: int, since: Optional[datetime] = None
) -> Optional[models.FundamentalIndicator]:
    query = db.query(models.FundamentalIndicator).filter(
        models.FundamentalIndicator.stock_id == stock_id
    )
    if since:
        query = query.filter(models.FundamentalIndicator.fetched_at >= since)
    return query.order_by(desc(models.FundamentalIndicator.fetched_at)).first()


def create(
    db: Session, payload: Union[schemas.FundamentalIndicatorCreate, dict]
) -> models.FundamentalIndicator:
    data = (
        payload.model_dump()
        if isinstance(payload, schemas.FundamentalIndicatorCreate)
        else dict(payload)
    )
    record = models.FundamentalIndicator(**data)
    db.add(record)
    db.commit()
    db.refresh(record)
    return record
