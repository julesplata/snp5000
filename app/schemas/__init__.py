from datetime import datetime
from typing import Optional, List, Literal
from pydantic import BaseModel, Field


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
    macro_score: Optional[float] = None
    data_sources: Optional[dict] = None
    notes: Optional[str] = None


class RatingCreate(RatingBase):
    stock_id: int


class Rating(RatingBase):
    id: int
    stock_id: int
    rating_date: datetime

    class Config:
        from_attributes = True


class StockWithLatestRating(Stock):
    latest_rating: Optional[Rating] = None
    rating_trend: Optional[Literal["up", "down", "stable", "new"]] = None


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
    current_price: Optional[float] = None
    data_source: Optional[str] = None


class TechnicalIndicatorCreate(TechnicalIndicatorBase):
    stock_id: int


class TechnicalIndicator(TechnicalIndicatorBase):
    id: int
    stock_id: int
    calculated_at: datetime

    class Config:
        from_attributes = True


# Fundamental Indicator Schemas
class FundamentalIndicatorBase(BaseModel):
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    profit_margin: Optional[float] = None
    dividend_yield: Optional[float] = None
    raw_metrics: Optional[dict] = None
    data_source: Optional[str] = None


class FundamentalIndicatorCreate(FundamentalIndicatorBase):
    stock_id: int


class FundamentalIndicator(FundamentalIndicatorBase):
    id: int
    stock_id: int
    fetched_at: datetime

    class Config:
        from_attributes = True


# Fundamental Analysis Schemas
class FundamentalAnalysisCreate(BaseModel):
    stock_id: int
    fundamental_indicator_id: Optional[int] = None
    normalized_scores: Optional[dict] = None
    composite_scores: Optional[dict] = None
    anomalies: Optional[list] = None
    risk_rating: Optional[str] = None
    confidence: Optional[float] = None
    narrative: Optional[dict] = None
    analyzed_at: Optional[datetime] = None


class FundamentalAnalysisSlimResponse(BaseModel):
    stock_id: int
    investment_style: str
    analyzed_at: datetime
    normalized_scores: dict
    composite_score: Optional[float] = None
    narrative: dict


# News Schemas
class NewsArticleBase(BaseModel):
    title: str
    summary: Optional[str] = None
    content: Optional[str] = None
    url: str = Field(..., format="uri")
    source: Optional[str] = None
    author: Optional[str] = None
    published_at: datetime
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[Literal["positive", "negative", "neutral"]] = None
    category: Optional[
        Literal["earnings", "merger", "product", "guidance", "general", "company"]
    ] = None


class NewsArticleCreate(NewsArticleBase):
    stock_id: int


class NewsArticle(NewsArticleBase):
    id: int
    stock_id: int
    fetched_at: datetime

    class Config:
        from_attributes = True


class NewsSummary(BaseModel):
    count: int
    headlines: List[str]
    sources: dict
    latest_published_at: Optional[datetime]


class MacroHealth(BaseModel):
    status: Literal["healthy", "warning", "error", "degraded"]
    message: str
    recommendation: Optional[str] = None
    sample_data: Optional[str] = None


# Analyst Rating Schemas
class AnalystRatingBase(BaseModel):
    source: str
    rating: str
    target_price: Optional[float] = None
    published_at: datetime


class AnalystRatingCreate(AnalystRatingBase):
    stock_id: int


class AnalystRating(AnalystRatingBase):
    id: int
    stock_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class AnalystConsensusBase(BaseModel):
    strong_buy: Optional[int] = None
    buy: Optional[int] = None
    hold: Optional[int] = None
    sell: Optional[int] = None
    strong_sell: Optional[int] = None
    target_mean: Optional[float] = None
    last_updated: Optional[datetime] = None


class AnalystConsensusCreate(AnalystConsensusBase):
    stock_id: int


class AnalystConsensus(AnalystConsensusBase):
    id: int
    stock_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class AnalystRefreshResponse(BaseModel):
    rating: AnalystRating
    consensus: AnalystConsensus


# Query Schemas
class StockFilter(BaseModel):
    sector_id: Optional[int] = None
    min_rating: Optional[float] = None
    max_rating: Optional[float] = None
    search: Optional[str] = None


class RatingHistoryResponse(BaseModel):
    stock: Stock
    ratings: List[Rating]


# Macro Schemas
class MacroSnapshot(BaseModel):
    id: int
    macro_score: Optional[float] = None
    components: Optional[dict] = None
    indicators: Optional[dict] = None
    indicator_context: Optional[dict] = None
    indicator_meta: Optional[dict] = None
    analysis: Optional[str] = None
    data_source: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True
