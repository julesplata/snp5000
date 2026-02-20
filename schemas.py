from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


# Sector Schemas
class SectorBase(BaseModel):
    name: str
    description: Optional[str] = None


class SectorCreate(SectorBase):
    pass


class Sector(SectorBase):
    id: int

    class Config:
        from_attributes = True


# Stock Schemas
class StockBase(BaseModel):
    symbol: str
    name: str
    sector_id: Optional[int] = None
    market_cap: Optional[float] = None
    current_price: Optional[float] = None


class StockCreate(StockBase):
    pass


class StockUpdate(BaseModel):
    name: Optional[str] = None
    sector_id: Optional[int] = None
    market_cap: Optional[float] = None
    current_price: Optional[float] = None


class Stock(StockBase):
    id: int
    created_at: datetime
    updated_at: datetime
    sector: Optional[Sector] = None

    class Config:
        from_attributes = True


# Rating Schemas
class RatingBase(BaseModel):
    overall_rating: float
    technical_score: Optional[float] = None
    analyst_score: Optional[float] = None
    fundamental_score: Optional[float] = None
    notes: Optional[str] = None


class RatingCreate(RatingBase):
    stock_id: int


class Rating(RatingBase):
    id: int
    stock_id: int
    rating_date: datetime
    data_sources: Optional[dict] = None

    class Config:
        from_attributes = True


class StockWithLatestRating(Stock):
    latest_rating: Optional[Rating] = None
    rating_trend: Optional[str] = None  # "up", "down", "stable"


# Technical Indicator Schemas
class TechnicalIndicatorBase(BaseModel):
    sma_50: Optional[float] = None
    sma_200: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    rsi: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    bollinger_upper: Optional[float] = None
    bollinger_lower: Optional[float] = None


class TechnicalIndicatorCreate(TechnicalIndicatorBase):
    stock_id: int


class TechnicalIndicator(TechnicalIndicatorBase):
    id: int
    stock_id: int
    calculated_at: datetime

    class Config:
        from_attributes = True


# Analyst Rating Schemas
class AnalystRatingBase(BaseModel):
    source: str
    rating: str
    target_price: Optional[float] = None
    analyst_name: Optional[str] = None
    published_at: datetime


class AnalystRatingCreate(AnalystRatingBase):
    stock_id: int


class AnalystRating(AnalystRatingBase):
    id: int
    stock_id: int
    created_at: datetime

    class Config:
        from_attributes = True


# Query Schemas
class StockFilter(BaseModel):
    sector_id: Optional[int] = None
    min_rating: Optional[float] = None
    max_rating: Optional[float] = None
    search: Optional[str] = None


class RatingHistoryResponse(BaseModel):
    stock: Stock
    ratings: List[Rating]
