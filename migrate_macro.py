"""
Database migration script to add macro_score column to existing databases

Run this if you already have a populated database and want to add the macro feature
"""

from sqlalchemy import create_engine, text
from database import DATABASE_URL
import sys


def migrate_database():
    """Add macro_score column to ratings table"""
    try:
        engine = create_engine(DATABASE_URL)

        print("=" * 60)
        print("Database Migration: Adding Macro Score Support")
        print("=" * 60)

        with engine.connect() as connection:
            # Check if column already exists
            result = connection.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='ratings' AND column_name='macro_score'
            """))

            if result.fetchone():
                print("\n✓ macro_score column already exists!")
                print("  No migration needed.")
                return

            # Add the column
            print("\n→ Adding macro_score column to ratings table...")
            connection.execute(text("""
                ALTER TABLE ratings 
                ADD COLUMN macro_score FLOAT
            """))
            connection.commit()

            print("✓ Successfully added macro_score column!")

            # Get count of existing ratings
            result = connection.execute(text("SELECT COUNT(*) FROM ratings"))
            count = result.fetchone()[0]

            print(f"\n✓ Migration complete!")
            print(f"  Found {count} existing ratings")
            print(f"  Existing ratings will have macro_score = NULL")
            print(f"  New ratings will include macro_score automatically")

            print("\n" + "=" * 60)
            print("Next Steps:")
            print("=" * 60)
            print("1. Set up FRED API key in .env (optional)")
            print("2. Restart your FastAPI server")
            print("3. Calculate new ratings to get macro scores")
            print("\nExample:")
            print("  curl -X POST http://localhost:8000/api/ratings/calculate/1")
            print("=" * 60)

    except Exception as e:
        print(f"\n✗ Migration failed: {e}")
        print("\nTroubleshooting:")
        print("- Ensure PostgreSQL is running")
        print("- Check DATABASE_URL in .env")
        print("- Verify you have ALTER TABLE permissions")
        sys.exit(1)


if __name__ == "__main__":
    print("\nThis will add the macro_score column to your database.")
    response = input("Continue? (y/n): ")

    if response.lower() == "y":
        migrate_database()
    else:
        print("Migration cancelled.")
