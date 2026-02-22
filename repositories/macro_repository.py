from typing import Dict, Optional
from sqlalchemy.orm import Session

from models import MacroSnapshot


class MacroSnapshotRepository:
    """Persistence adapter for macro snapshots."""

    def save_snapshot(self, db: Session, data: Dict) -> MacroSnapshot:
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

    def get_latest_snapshot(self, db: Session) -> Optional[MacroSnapshot]:
        return db.query(MacroSnapshot).order_by(MacroSnapshot.created_at.desc()).first()

    def clear_snapshots(self, db: Session) -> None:
        db.query(MacroSnapshot).delete()
        db.commit()


macro_repository = MacroSnapshotRepository()
