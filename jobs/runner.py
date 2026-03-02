"""
Lightweight job runner for scheduled tasks (ratings refresh, macro snapshots).

Examples:
  # Recalculate ratings for all stocks (safe on a cron)
  python jobs/runner.py recalc-ratings

  # Only recalc first 50 stocks
  python jobs/runner.py recalc-ratings --limit 50

  # Refresh macro snapshot
  python jobs/runner.py refresh-macro

This script is idempotent-ish: it upserts new ratings and macro snapshots
without deleting historical data. It also writes a simple job_runs log table
for observability; the table is created if missing.
"""

import argparse
import datetime as dt
import sys
from typing import Optional

from sqlalchemy import text

import app.models as models
from app.utils.rating_utils import RatingService
from config import get_settings
from database import SessionLocal, engine
import app.crud.macro_snapshot as macro_crud
from services.macro_service import MacroeconomicService


def ensure_job_runs_table():
    """Create job_runs table if it doesn't exist (safe to run each time)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS job_runs (
        id SERIAL PRIMARY KEY,
        job_name TEXT NOT NULL,
        started_at TIMESTAMPTZ DEFAULT now(),
        finished_at TIMESTAMPTZ,
        status TEXT,
        processed INTEGER,
        error TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def log_job_start(job_name: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                "INSERT INTO job_runs (job_name, status, processed) "
                "VALUES (:job_name, 'running', 0) RETURNING id"
            ),
            {"job_name": job_name},
        )
        return result.scalar_one()


def log_job_end(job_id: int, status: str, processed: int, error: Optional[str] = None):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE job_runs "
                "SET status = :status, processed = :processed, error = :error, "
                "finished_at = now() "
                "WHERE id = :job_id"
            ),
            {
                "status": status,
                "processed": processed,
                "error": error,
                "job_id": job_id,
            },
        )


def task_refresh_macro() -> int:
    db = SessionLocal()
    try:
        svc = MacroeconomicService()
        data = svc.calculate_macro_score()
        macro_crud.save_snapshot(db, data)
        return 1
    finally:
        db.close()


def task_recalc_ratings(
    limit: Optional[int] = None, symbol: Optional[str] = None
) -> int:
    db = SessionLocal()
    processed = 0
    try:
        query = db.query(models.Stock)
        if symbol:
            query = query.filter(models.Stock.symbol == symbol)
        if limit:
            query = query.limit(limit)

        stocks = query.all()
        if not stocks:
            print("No stocks found to process.")
            return 0

        rater = RatingService(db_session=db)
        for stock in stocks:
            rating_data = rater.calculate_rating(stock.symbol, db=db)
            if not rating_data:
                print(f"✗ {stock.symbol}: rating failed")
                continue

            rating = models.Rating(
                stock_id=stock.id,
                overall_rating=rating_data["overall_rating"],
                technical_score=rating_data.get("technical_score"),
                analyst_score=rating_data.get("analyst_score"),
                fundamental_score=rating_data.get("fundamental_score"),
                macro_score=rating_data.get("macro_score"),
                notes="Automated refresh",
                rating_date=dt.datetime.utcnow(),
                data_sources=rating_data.get("data_sources"),
            )
            db.add(rating)
            db.commit()
            processed += 1
            print(f"✓ {stock.symbol}: {rating.overall_rating}/10")
        return processed
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Run maintenance jobs.")
    sub = parser.add_subparsers(dest="command", required=True)

    recalc = sub.add_parser("recalc-ratings", help="Recalculate ratings for stocks")
    recalc.add_argument("--limit", type=int, help="Limit number of stocks")
    recalc.add_argument("--symbol", type=str, help="Only process one symbol")

    sub.add_parser("refresh-macro", help="Fetch and store macro snapshot")

    args = parser.parse_args()

    ensure_job_runs_table()
    job_id = log_job_start(args.command)

    try:
        if args.command == "recalc-ratings":
            processed = task_recalc_ratings(limit=args.limit, symbol=args.symbol)
        elif args.command == "refresh-macro":
            processed = task_refresh_macro()
        else:
            raise ValueError(f"Unknown command {args.command}")
        log_job_end(job_id, status="success", processed=processed)
        print(f"Job {args.command} finished. Processed: {processed}")
    except Exception as e:
        log_job_end(job_id, status="error", processed=0, error=str(e))
        print(f"Job {args.command} failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
