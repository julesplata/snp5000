from sqlalchemy.orm import Session
from models import MacroSnapshot
from typing import Optional, Dict


def save_snapshot(db: Session, data: Dict) -> MacroSnapshot:
    snapshot = MacroSnapshot(
        macro_score=data.get("macro_score"),
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


def get_latest_snapshot(db: Session) -> Optional[MacroSnapshot]:
    return db.query(MacroSnapshot).order_by(MacroSnapshot.created_at.desc()).first()


def clear_snapshots(db: Session):
    db.query(MacroSnapshot).delete()
    db.commit()
