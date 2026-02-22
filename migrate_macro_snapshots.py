"""
Migration: create macro_snapshots table to store cached macroeconomic data pulls.
Run this once against your DATABASE_URL.
"""

from sqlalchemy import create_engine, text
from database import DATABASE_URL
import sys


def migrate_database():
    try:
        engine = create_engine(DATABASE_URL)

        print("=" * 60)
        print("Database Migration: Adding macro_snapshots table")
        print("=" * 60)

        with engine.connect() as connection:
            # Check if table exists
            result = connection.execute(text("""
                SELECT to_regclass('public.macro_snapshots')
            """))
            exists = result.fetchone()[0]

            if exists:
                print("\n✓ macro_snapshots table already exists")
                return

            print("\n→ Creating macro_snapshots table...")
            connection.execute(text("""
                CREATE TABLE macro_snapshots (
                    id SERIAL PRIMARY KEY,
                    macro_score FLOAT,
                    components JSON,
                    indicators JSON,
                    indicator_context JSON,
                    indicator_meta JSON,
                    analysis TEXT,
                    data_source VARCHAR,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
                )
                """))
            connection.commit()

            print("✓ Table created successfully")
            print("\nNext steps:")
            print("- Restart FastAPI server")
            print("- Hit POST /api/macro/refresh to populate first snapshot")

    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("This will add the macro_snapshots table to your database.")
    resp = input("Continue? (y/n): ")
    if resp.lower().startswith("y"):
        migrate_database()
    else:
        print("Migration cancelled.")
