from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, JSON, Index
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import TSVECTOR
from datetime import datetime

from database import Base


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
    technical_indicators = relationship(
        "TechnicalIndicator", cascade="all, delete-orphan"
    )
    fundamental_indicators = relationship(
        "FundamentalIndicator", cascade="all, delete-orphan"
    )
    news_articles = relationship(
        "NewsArticle", back_populates="stock", cascade="all, delete-orphan"
    )


class Rating(Base):
    __tablename__ = "ratings"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))

    overall_rating = Column(Float)
    technical_score = Column(Float, nullable=True)
    analyst_score = Column(Float, nullable=True)
    fundamental_score = Column(Float, nullable=True)
    macro_score = Column(Float, nullable=True)

    rating_date = Column(DateTime, default=datetime.utcnow, index=True)
    data_sources = Column(JSON, nullable=True)
    notes = Column(Text, nullable=True)

    stock = relationship("Stock", back_populates="ratings")


class MacroSnapshot(Base):
    __tablename__ = "macro_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    macro_score = Column(Float, nullable=True)
    components = Column(JSON, nullable=True)
    indicators = Column(JSON, nullable=True)
    indicator_context = Column(JSON, nullable=True)
    indicator_meta = Column(JSON, nullable=True)
    analysis = Column(Text, nullable=True)
    data_source = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class TechnicalIndicator(Base):
    __tablename__ = "technical_indicators"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))

    sma_50 = Column(Float, nullable=True)
    sma_200 = Column(Float, nullable=True)
    ema_12 = Column(Float, nullable=True)
    ema_26 = Column(Float, nullable=True)
    bollinger_upper = Column(Float, nullable=True)
    bollinger_lower = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)
    macd = Column(Float, nullable=True)
    macd_signal = Column(Float, nullable=True)
    current_price = Column(Float, nullable=True)
    data_source = Column(String, default="finnhub")
    calculated_at = Column(DateTime, default=datetime.utcnow)


class FundamentalIndicator(Base):
    __tablename__ = "fundamental_indicators"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))

    pe_ratio = Column(Float, nullable=True)
    pb_ratio = Column(Float, nullable=True)
    debt_to_equity = Column(Float, nullable=True)
    profit_margin = Column(Float, nullable=True)
    dividend_yield = Column(Float, nullable=True)
    raw_metrics = Column(JSON, nullable=True)
    data_source = Column(String, default="finnhub")
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)


class AnalystRating(Base):
    __tablename__ = "analyst_ratings"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"))
    source = Column(String)
    rating = Column(String)
    target_price = Column(Float, nullable=True)
    analyst_name = Column(String, nullable=True)
    published_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class NewsArticle(Base):
    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), index=True)

    title = Column(String(500), nullable=False)
    summary = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    url = Column(String(1000), unique=True, index=True)

    source = Column(String(100), index=True)
    author = Column(String(200), nullable=True)

    published_at = Column(DateTime, index=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)

    sentiment_score = Column(Float, nullable=True)
    sentiment_label = Column(String(20), nullable=True)

    category = Column(String(50), nullable=True, index=True)

    search_vector = Column(TSVECTOR)

    stock = relationship("Stock", back_populates="news_articles")

    __table_args__ = (
        Index("idx_stock_date", "stock_id", "published_at"),
        Index("idx_source_date", "source", "published_at"),
        Index("idx_sentiment", "sentiment_label", "sentiment_score"),
        Index("idx_search", "search_vector", postgresql_using="gin"),
    )
