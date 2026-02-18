import sys
import time
import os
from sqlalchemy.orm import Session
from database import SessionLocal, engine
import models
from services.rating_service_alpaca import AlpacaRatingService
from datetime import datetime
from config import get_settings

# Get API keys from .env file
settings = get_settings()

ALPACA_API_KEY = settings.alpaca_key
ALPACA_API_SECRET = settings.alpaca_secret

if not ALPACA_API_KEY or not ALPACA_API_SECRET:
    print("\n" + "=" * 60)
    print("ERROR: Alpaca API credentials not found!")
    print("\n" + "=" * 60)
    sys.exit(1)


def create_sectors(db: Session):
    """Create sample sectors"""
    sectors = [
        {"name": "Technology", "description": "Technology and software companies"},
        {
            "name": "Healthcare",
            "description": "Healthcare and pharmaceutical companies",
        },
        {"name": "Finance", "description": "Banks and financial institutions"},
        {"name": "Consumer", "description": "Consumer goods and retail"},
        {"name": "Energy", "description": "Energy and utilities"},
        {"name": "Industrial", "description": "Industrial and manufacturing"},
        {"name": "Real Estate", "description": "Real estate and REITs"},
    ]

    created_sectors = []
    for sector_data in sectors:
        # Check if sector already exists
        existing = (
            db.query(models.Sector)
            .filter(models.Sector.name == sector_data["name"])
            .first()
        )
        if not existing:
            sector = models.Sector(**sector_data)
            db.add(sector)
            created_sectors.append(sector)
            print(f"Created sector: {sector_data['name']}")
        else:
            created_sectors.append(existing)
            print(f"Sector already exists: {sector_data['name']}")

    db.commit()
    return created_sectors


def create_sample_stocks(
    db: Session, sectors: list, rating_service: AlpacaRatingService
):
    """Create sample stocks"""

    # Map sector names to sector objects
    sector_map = {sector.name: sector for sector in sectors}

    sample_stocks = [
        {"symbol": "AAPL", "sector": "Technology"},
        {"symbol": "MSFT", "sector": "Technology"},
        {"symbol": "GOOGL", "sector": "Technology"},
        {"symbol": "AMZN", "sector": "Consumer"},
        {"symbol": "TSLA", "sector": "Consumer"},
        {"symbol": "JNJ", "sector": "Healthcare"},
        {"symbol": "JPM", "sector": "Finance"},
        {"symbol": "V", "sector": "Finance"},
        {"symbol": "NVDA", "sector": "Technology"},
        {"symbol": "META", "sector": "Technology"},
    ]

    created_stocks = []
    for i, stock_data in enumerate(sample_stocks):
        # Check if stock already exists
        existing = (
            db.query(models.Stock)
            .filter(models.Stock.symbol == stock_data["symbol"])
            .first()
        )
        if existing:
            print(f"Stock already exists: {stock_data['symbol']}")
            created_stocks.append(existing)
            continue

        print(
            f"\n[{i+1}/{len(sample_stocks)}] Fetching data for {stock_data['symbol']}..."
        )

        # Retry logic with exponential backoff for rate limiting
        stock_info = None
        for attempt in range(3):
            stock_info = rating_service.get_stock_info(stock_data["symbol"])
            if stock_info:
                break
            wait = 10 * (attempt + 1)
            print(
                f"  Rate limited or error — retrying in {wait}s... (attempt {attempt + 1}/3)"
            )
            time.sleep(wait)

        if stock_info:
            sector = sector_map.get(stock_data["sector"])
            stock = models.Stock(
                symbol=stock_info["symbol"],
                name=stock_info.get("name", stock_info["symbol"]),
                sector_id=sector.id if sector else None,
                market_cap=stock_info.get("market_cap"),
                current_price=stock_info.get("current_price"),
            )
            db.add(stock)
            db.commit()
            db.refresh(stock)
            created_stocks.append(stock)
            print(f"✓ Created stock: {stock.symbol}")

            # Calculate and save rating
            print(f"  Calculating rating for {stock.symbol}...")
            rating_data = rating_service.calculate_rating(stock.symbol)

            if rating_data:
                rating = models.Rating(
                    stock_id=stock.id,
                    overall_rating=rating_data["overall_rating"],
                    technical_score=rating_data.get("technical_score"),
                    analyst_score=rating_data.get("analyst_score"),
                    fundamental_score=rating_data.get("fundamental_score"),
                    momentum_score=rating_data.get("momentum_score"),
                    data_sources=rating_data.get("data_sources"),
                    notes="Initial automated rating (Alpaca)",
                )
                db.add(rating)
                db.commit()
                print(f"  ✓ Rating saved: {rating_data['overall_rating']}/10")
        else:
            print(
                f"✗ Skipping {stock_data['symbol']} — could not fetch after 3 attempts"
            )

        # Pause between each stock to avoid rate limiting (skip pause after last stock)
        if i < len(sample_stocks) - 1:
            print("  Waiting 2s before next request...")
            time.sleep(2)

    return created_stocks


def main():
    """Main function to populate database"""
    print("=" * 60)
    print("Stock Rating App - Database Population (Alpaca)")
    print("=" * 60)

    # Initialize Alpaca rating service
    print("\nInitializing Alpaca API connection...")
    rating_service = AlpacaRatingService(ALPACA_API_KEY, ALPACA_API_SECRET)
    print("✓ Connected to Alpaca API")

    # Create database tables
    print("\nCreating database tables...")
    models.Base.metadata.create_all(bind=engine)
    print("✓ Tables created successfully!")

    # Create database session
    db = SessionLocal()

    try:
        # Create sectors
        print("\n" + "=" * 60)
        print("Creating Sectors")
        print("=" * 60)
        sectors = create_sectors(db)
        print(f"\n✓ Total sectors: {len(sectors)}")

        # Create sample stocks
        print("\n" + "=" * 60)
        print("Creating Sample Stocks")
        print("=" * 60)
        stocks = create_sample_stocks(db, sectors, rating_service)
        print(f"\n✓ Total stocks created/updated: {len(stocks)}")

        print("\n" + "=" * 60)
        print("✓ Database population completed successfully!")
        print("=" * 60)
        print("\nYou can now:")
        print("1. Start the FastAPI server: python main.py")
        print("2. Start the React frontend: cd ../frontend && npm run dev")
        print("3. Visit http://localhost:3000 to view the app")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()
        db.rollback()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
