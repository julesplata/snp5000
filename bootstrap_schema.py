"""
One-time schema bootstrap for all SQLAlchemy models.

Usage (requires DATABASE_URL env var to point at your DB):
    python bootstrap_schema.py

Safe to re-run; it only creates missing tables.
"""

from database import Base, engine  # uses DATABASE_URL from config/settings
import app.models  # noqa: F401 ensures models are imported and registered


def main():
    print("Creating tables if missing...")
    Base.metadata.create_all(bind=engine)
    print("Done.")


if __name__ == "__main__":
    main()
