from sqlalchemy.orm import Session
from typing import Optional, Dict

import app.models as models


def save_snapshot(db: Session, data: Dict) -> models.EconomicSnapshot:
    snapshot = models.EconomicSnapshot(
        economic_score=data.get("economic_score"),
        components=data.get("components"),
        indicators=data.get("indicators"),
        indicator_context=data.get("indicator_context"),
        indicator_meta=data.get("indicator_meta"),
        analysis=data.get("analysis"),
        data_source=data.get("data_source"),
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)
    return snapshot


def get_latest_snapshot(db: Session) -> Optional[models.EconomicSnapshot]:
    return (
        db.query(models.EconomicSnapshot)
        .order_by(models.EconomicSnapshot.created_at.desc())
        .first()
    )


def clear_snapshots(db: Session) -> None:
    db.query(models.EconomicSnapshot).delete()
    db.commit()
