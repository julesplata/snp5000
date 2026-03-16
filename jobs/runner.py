"""
Lightweight job runner for scheduled tasks (ratings refresh, economic snapshots).

Examples:
  # Recalculate ratings for all stocks (safe on a cron)
  python jobs/runner.py recalc-ratings

  # Only recalc first 50 stocks
  python jobs/runner.py recalc-ratings --limit 50

  # Refresh economic snapshot
  python jobs/runner.py refresh-economic

  # Refresh all quotes
  python jobs/runner.py refresh-quotes

  # Refresh news for every stock (and prune stale articles)
  python jobs/runner.py refresh-news --lookback-hours 12

This script is idempotent-ish: it upserts new ratings and economic snapshots
without deleting historical data. It also writes a simple job_runs log table
for observability; the table is created if missing.
"""

import argparse
import datetime as dt
import logging
import sys
from pathlib import Path
from typing import Optional

from sqlalchemy import text

# Ensure project root is on PYTHONPATH when invoked directly (e.g., from cron).
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app.models as models
from app.utils.rating_utils import RatingService
from config import get_settings
from database import SessionLocal, engine
import app.crud.economic_snapshot as economic_crud
from services.economic_service import EconomicService
from app.services.sector_economic_rating import SectorEconomicRatingService
from app.services.quote import QuoteService
from app.services.news import NewsService


settings = get_settings()
logging.basicConfig(
    level=getattr(logging, str(settings.log_level).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("job_runner")


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
    logger.info("Starting job %s", job_name)
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
    if status == "success":
        logger.info("Job finished with status=%s processed=%s", status, processed)
    else:
        logger.error("Job finished with status=%s processed=%s error=%s", status, processed, error)
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


def task_refresh_economic() -> int:
    db = SessionLocal()
    try:
        svc = EconomicService()
        data = svc.calculate_economic_score()
        snapshot = economic_crud.save_snapshot(db, data)
        SectorEconomicRatingService().rate_all_sectors(db, snapshot)
        return 1
    finally:
        db.close()


def task_refresh_quotes() -> int:
    db = SessionLocal()
    try:
        svc = QuoteService()
        result = svc.refresh_all_quotes(db)
        logger.info("Quotes refreshed: %s stocks updated", result.get("updated", 0))
        return result.get("updated", 0)
    finally:
        db.close()


def task_refresh_news(lookback_hours: int = 12) -> int:
    db = SessionLocal()
    try:
        svc = NewsService()
        result = svc.fetch_and_store_all_company_news(
            db, lookback_hours=lookback_hours
        )
        logger.info(
            "News refreshed: %s stocks processed, %s articles inserted",
            result.get("stocks_processed"),
            result.get("inserted"),
        )
        return result.get("inserted", 0)
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
            logger.warning("No stocks found to process.")
            return 0

        rater = RatingService(db_session=db)
        for stock in stocks:
            rating_data = rater.calculate_rating(stock.symbol, db=db)
            if not rating_data:
                logger.warning("%s: rating failed", stock.symbol)
                continue

            rating = models.Rating(
                stock_id=stock.id,
                overall_rating=rating_data["overall_rating"],
                technical_score=rating_data.get("technical_score"),
                analyst_score=rating_data.get("analyst_score"),
                fundamental_score=rating_data.get("fundamental_score"),
                economic_score=rating_data.get("economic_score"),
                notes="Automated refresh",
                rating_date=dt.datetime.utcnow(),
                data_sources=rating_data.get("data_sources"),
            )
            db.add(rating)
            db.commit()
            processed += 1
            logger.info("%s: %.1f/10", stock.symbol, rating.overall_rating)
        return processed
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Run maintenance jobs.")
    sub = parser.add_subparsers(dest="command", required=True)

    recalc = sub.add_parser("recalc-ratings", help="Recalculate ratings for stocks")
    recalc.add_argument("--limit", type=int, help="Limit number of stocks")
    recalc.add_argument("--symbol", type=str, help="Only process one symbol")

    sub.add_parser("refresh-economic", help="Fetch and store economic snapshot")
    sub.add_parser("refresh-quotes", help="Refresh quotes for all stocks")

    refresh_news = sub.add_parser(
        "refresh-news", help="Refresh news for all stocks and prune stale entries"
    )
    refresh_news.add_argument(
        "--lookback-hours",
        type=int,
        default=12,
        help="How far back to fetch company news per stock",
    )

    args = parser.parse_args()

    ensure_job_runs_table()
    job_id = log_job_start(args.command)

    try:
        logger.info("Executing command %s", args.command)
        if args.command == "recalc-ratings":
            processed = task_recalc_ratings(limit=args.limit, symbol=args.symbol)
        elif args.command == "refresh-economic":
            processed = task_refresh_economic()
        elif args.command == "refresh-quotes":
            processed = task_refresh_quotes()
        elif args.command == "refresh-news":
            processed = task_refresh_news(lookback_hours=args.lookback_hours)
        else:
            raise ValueError(f"Unknown command {args.command}")
        log_job_end(job_id, status="success", processed=processed)
        logger.info("Job %s finished. Processed: %s", args.command, processed)
    except Exception as e:
        log_job_end(job_id, status="error", processed=0, error=str(e))
        logger.exception("Job %s failed: %s", args.command, e)
        sys.exit(1)


if __name__ == "__main__":
    main()
