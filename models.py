from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship
from database import Base
from datetime import datetime


class Sector(Base):
    __tablename__ = "sectors"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    description = Column(Text, nullable=True)

    stocks = relationship("Stock", back_populates="sector")


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, unique=True, index=True)
    name = Column(String, index=True)
    sector_id = Column(Integer, ForeignKey("sectors.id"), nullable=True)
    market_cap = Column(Float, nullable=True)
    current_price = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sector = relationship("Sector", back_populates="stocks")
    ratings = relationship(
        "Rating", back_populates="stock", cascade="all, delete-orphan"
    )


class Rating(Base):
    __tablename__ = "ratings"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))

    # Overall rating (1-10)
    overall_rating = Column(Float)

    # Component ratings
    technical_score = Column(Float, nullable=True)  # Technical indicators
    analyst_score = Column(Float, nullable=True)  # Analyst ratings
    fundamental_score = Column(Float, nullable=True)  # P/E, P/B, etc.
    macro_score = Column(Float, nullable=True)  # Macroeconomic environment

    # Rating metadata
    rating_date = Column(DateTime, default=datetime.utcnow, index=True)
    data_sources = Column(JSON, nullable=True)  # Track which sources were used
    notes = Column(Text, nullable=True)

    stock = relationship("Stock", back_populates="ratings")


class TechnicalIndicator(Base):
    __tablename__ = "technical_indicators"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))

    # Moving averages
    sma_50 = Column(Float, nullable=True)
    sma_200 = Column(Float, nullable=True)
    ema_12 = Column(Float, nullable=True)
    ema_26 = Column(Float, nullable=True)


    # Volatility
    bollinger_upper = Column(Float, nullable=True)
    bollinger_lower = Column(Float, nullable=True)

    calculated_at = Column(DateTime, default=datetime.utcnow)


class AnalystRating(Base):
    __tablename__ = "analyst_ratings"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))

    source = Column(String)  # e.g., "Goldman Sachs", "Morgan Stanley"
    rating = Column(String)  # e.g., "Buy", "Hold", "Sell"
    target_price = Column(Float, nullable=True)
    analyst_name = Column(String, nullable=True)

    published_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
