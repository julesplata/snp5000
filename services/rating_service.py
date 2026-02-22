"""
Service for calculating stock ratings using Finnhub (free tier).
Stores technical and fundamental snapshots in the database to reduce API calls
and stay within the 60 req/min rate limit on the free plan.
"""

from collections import deque
from datetime import datetime, timedelta
import os
import time
from typing import Dict, Optional

import numpy as np
import pandas as pd
import requests
from sqlalchemy.orm import Session
from sqlalchemy import desc

from config import get_settings
from database import SessionLocal
import models
from services.macro_service import MacroeconomicService


class FinnhubClient:
    """Minimal Finnhub REST client with naive per-minute throttling."""

    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self, api_key: str, max_per_minute: int = 55):
        if not api_key:
            raise ValueError("FINNHUB_API_KEY is required")
        self.api_key = api_key
        self.max_per_minute = max_per_minute
        self._call_times = deque()

    def _throttle(self):
        now = time.time()
        # prune anything older than 60s
        while self._call_times and now - self._call_times[0] > 60:
            self._call_times.popleft()
        if len(self._call_times) >= self.max_per_minute:
            sleep_for = 60 - (now - self._call_times[0]) + 0.05
            time.sleep(max(sleep_for, 0))
        self._call_times.append(time.time())

    def get(self, path: str, params: Optional[dict] = None) -> Optional[dict]:
        params = params or {}
        params["token"] = self.api_key

        for attempt in range(3):
            self._throttle()
            resp = requests.get(f"{self.BASE_URL}{path}", params=params, timeout=10)
            if resp.status_code == 429:
                # back off gently
                time.sleep(1.5 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        return None


class RatingService:
    """Calculate stock ratings from Finnhub data and persist indicators."""

    def __init__(
        self,
        db_session: Optional[Session] = None,
        finnhub_api_key: Optional[str] = None,
    ):
        self.db = db_session
        settings = get_settings()
        self.finnhub = FinnhubClient(
            finnhub_api_key or os.getenv("FINNHUB_API_KEY") or settings.finnhub_api_key
        )
        self.macro_service = MacroeconomicService()

        # Cache macro data (refresh every hour)
        self._macro_cache = None
        self._macro_cache_time = None

        self.weights = {
            "technical": 0.25,
            "analyst": 0.25,
            "fundamental": 0.25,
            "macro": 0.25,
        }

        # TTLs to avoid hammering APIs
        self.technical_ttl_hours = 6  # intraday changes matter more
        self.fundamental_ttl_hours = 24  # fundamentals change slowly

    # ---------- Public API ----------
    def calculate_rating(self, symbol: str, db: Session = None) -> Optional[Dict]:
        """
        Calculate comprehensive rating for a stock.
        Saves/reads technical and fundamental snapshots from the DB.
        """
        db = db or self.db or SessionLocal()
        try:
            stock = self._get_or_create_stock(symbol, db)

            # Technical snapshot
            technical = self._get_or_fetch_technical(stock, db)
            technical_score = self._calculate_technical_score(technical)

            # Fundamental snapshot
            fundamental = self._get_or_fetch_fundamental(stock, db)
            fundamental_score = self._calculate_fundamental_score(fundamental)

            # Analyst score (no persistence yet, single lightweight call)
            analyst_score = self._get_analyst_score(symbol)

            # Macro score (cached in memory)
            macro_data = self._get_macro_score()
            macro_score = macro_data["macro_score"]

            overall_rating = (
                technical_score * self.weights["technical"]
                + analyst_score * self.weights["analyst"]
                + fundamental_score * self.weights["fundamental"]
                + macro_score * self.weights["macro"]
            )

            return {
                "overall_rating": round(overall_rating, 2),
                "technical_score": round(technical_score, 2),
                "analyst_score": round(analyst_score, 2),
                "fundamental_score": round(fundamental_score, 2),
                "macro_score": round(macro_score, 2),
                "macro_analysis": macro_data.get("analysis", ""),
                "macro_components": macro_data.get("components", {}),
                "data_sources": {
                    "technical": "finnhub",
                    "analyst": "finnhub",
                    "fundamental": "finnhub",
                    "macro": macro_data.get("data_source", "FRED"),
                    "cached": {
                        "technical": (
                            technical.calculated_at.isoformat() if technical else None
                        ),
                        "fundamental": (
                            fundamental.fetched_at.isoformat() if fundamental else None
                        ),
                    },
                },
            }
        except Exception as e:
            print(f"Error calculating rating for {symbol}: {e}")
            return None
        finally:
            if db is not self.db:
                db.close()

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """Get basic stock information from Finnhub profile endpoint."""
        try:
            data = self.finnhub.get("/stock/profile2", {"symbol": symbol})
            if not data:
                return None
            market_cap = data.get("marketCapitalization")
            shares = data.get("shareOutstanding")
            price = (market_cap / shares) if market_cap and shares else None
            return {
                "symbol": symbol,
                "name": data.get("name") or symbol,
                "sector": data.get("finnhubIndustry"),
                "market_cap": market_cap,
                "current_price": price,
            }
        except Exception as e:
            print(f"Error fetching stock profile: {e}")
            return None

    # ---------- Internal helpers ----------
    def _get_or_create_stock(self, symbol: str, db: Session) -> models.Stock:
        stock = db.query(models.Stock).filter(models.Stock.symbol == symbol).first()
        if stock:
            return stock

        info = self.get_stock_info(symbol) or {"symbol": symbol, "name": symbol}
        stock = models.Stock(
            symbol=symbol,
            name=info.get("name", symbol),
            market_cap=info.get("market_cap"),
            current_price=info.get("current_price"),
        )
        db.add(stock)
        db.commit()
        db.refresh(stock)
        return stock

    def _get_or_fetch_technical(
        self, stock: models.Stock, db: Session
    ) -> models.TechnicalIndicator:
        cutoff = datetime.utcnow() - timedelta(hours=self.technical_ttl_hours)
        cached = (
            db.query(models.TechnicalIndicator)
            .filter(
                models.TechnicalIndicator.stock_id == stock.id,
                models.TechnicalIndicator.calculated_at >= cutoff,
            )
            .order_by(desc(models.TechnicalIndicator.calculated_at))
            .first()
        )
        if cached:
            return cached

        hist = self._fetch_price_history(stock.symbol)
        if hist is None or hist.empty:
            raise ValueError(f"No historical data for {stock.symbol}")

        technical = self._compute_and_store_technical(stock, hist, db)
        return technical

    def _fetch_price_history(
        self, symbol: str, days: int = 365
    ) -> Optional[pd.DataFrame]:
        """Fetch daily candles from Finnhub and return DataFrame with Close prices."""
        end = int(time.time())
        start = end - days * 24 * 60 * 60
        data = self.finnhub.get(
            "/stock/candle",
            {"symbol": symbol, "resolution": "D", "from": start, "to": end},
        )
        if not data or data.get("s") != "ok":
            return None

        df = pd.DataFrame(
            {
                "Close": data["c"],
                "High": data["h"],
                "Low": data["l"],
                "Open": data["o"],
                "Volume": data["v"],
            },
            index=pd.to_datetime(data["t"], unit="s"),
        )
        return df

    def _compute_and_store_technical(
        self, stock: models.Stock, hist: pd.DataFrame, db: Session
    ) -> models.TechnicalIndicator:
        # Calculate moving averages
        sma_50 = hist["Close"].rolling(window=50).mean().iloc[-1]
        sma_200 = hist["Close"].rolling(window=200).mean().iloc[-1]

        # EMA for MACD
        ema_12 = hist["Close"].ewm(span=12).mean().iloc[-1]
        ema_26 = hist["Close"].ewm(span=26).mean().iloc[-1]

        rsi = self._calculate_rsi(hist["Close"])
        macd, macd_signal = self._calculate_macd(hist["Close"])

        # Bollinger Bands (20-day)
        rolling_mean = hist["Close"].rolling(window=20).mean().iloc[-1]
        rolling_std = hist["Close"].rolling(window=20).std().iloc[-1]
        bollinger_upper = rolling_mean + 2 * rolling_std
        bollinger_lower = rolling_mean - 2 * rolling_std

        technical = models.TechnicalIndicator(
            stock_id=stock.id,
            sma_50=float(sma_50),
            sma_200=float(sma_200),
            ema_12=float(ema_12),
            ema_26=float(ema_26),
            rsi=float(rsi),
            macd=float(macd),
            macd_signal=float(macd_signal),
            bollinger_upper=float(bollinger_upper),
            bollinger_lower=float(bollinger_lower),
            current_price=float(hist["Close"].iloc[-1]),
            data_source="finnhub",
        )
        db.add(technical)
        db.commit()
        db.refresh(technical)
        return technical

    def _get_or_fetch_fundamental(
        self, stock: models.Stock, db: Session
    ) -> models.FundamentalIndicator:
        cutoff = datetime.utcnow() - timedelta(hours=self.fundamental_ttl_hours)
        cached = (
            db.query(models.FundamentalIndicator)
            .filter(
                models.FundamentalIndicator.stock_id == stock.id,
                models.FundamentalIndicator.fetched_at >= cutoff,
            )
            .order_by(desc(models.FundamentalIndicator.fetched_at))
            .first()
        )
        if cached:
            return cached

        data = self.finnhub.get(
            "/stock/metric", {"symbol": stock.symbol, "metric": "all"}
        )
        if not data or not data.get("metric"):
            raise ValueError(f"No fundamental data for {stock.symbol}")
        metrics = data["metric"]

        fundamental = models.FundamentalIndicator(
            stock_id=stock.id,
            pe_ratio=self._first_metric(metrics, ["peBasicExclExtraTTM", "peTTM"]),
            pb_ratio=self._first_metric(metrics, ["pbAnnual", "pbQuarterly"]),
            debt_to_equity=self._first_metric(
                metrics,
                [
                    "totalDebt/totalEquityAnnual",
                    "totalDebt/totalEquityQuarterly",
                    "totalDebtToEquityAnnual",
                    "totalDebtToEquityQuarterly",
                ],
            ),
            profit_margin=self._first_metric(
                metrics, ["netProfitMarginTTM", "netProfitMarginAnnual"]
            ),
            dividend_yield=self._first_metric(
                metrics, ["dividendYieldIndicatedAnnual", "dividendYieldTTM"]
            ),
            raw_metrics=metrics,
            data_source="finnhub",
        )
        db.add(fundamental)
        db.commit()
        db.refresh(fundamental)
        return fundamental

    def _get_analyst_score(self, symbol: str) -> float:
        """
        Use Finnhub recommendation trends. Returns score 0-10.
        """
        try:
            recs = self.finnhub.get("/stock/recommendation", {"symbol": symbol}) or []
            if not recs:
                return 5.0
            latest = recs[0]
            # weight strong buys higher, strong sells lower
            total = (
                latest.get("strongBuy", 0) * 2
                + latest.get("buy", 0) * 1
                - latest.get("sell", 0) * 1
                - latest.get("strongSell", 0) * 2
            )
            count = sum(
                latest.get(k, 0)
                for k in ["strongBuy", "buy", "hold", "sell", "strongSell"]
            )
            if count == 0:
                return 5.0
            # Normalize to -2..2 then map to 0..10
            sentiment = total / max(count, 1)
            score = (sentiment + 2) * 2.5  # -2 =>0 , +2 =>10
            return min(max(score, 0), 10)
        except Exception as e:
            print(f"Error getting analyst score: {e}")
            return 5.0

    def _get_macro_score(self) -> Dict:
        """Get macro score with 1-hour caching."""
        if self._macro_cache and self._macro_cache_time:
            if datetime.now() - self._macro_cache_time < timedelta(hours=1):
                return self._macro_cache

        macro_data = self.macro_service.calculate_macro_score()
        self._macro_cache = macro_data
        self._macro_cache_time = datetime.now()
        return macro_data

    # ---------- Scoring helpers ----------
    def _calculate_technical_score(self, technical: models.TechnicalIndicator) -> float:
        try:
            score = 5.0
            price = technical.current_price

            # Price vs SMAs
            if price > technical.sma_50:
                score += 1.5
            if price > technical.sma_200:
                score += 1.5
            if technical.sma_50 > technical.sma_200:
                score += 1.0

            # RSI
            if 40 <= technical.rsi <= 60:
                score += 1.5
            elif 30 <= technical.rsi <= 70:
                score += 1.0
            elif technical.rsi < 30 or technical.rsi > 70:
                score += 0.5

            # MACD crossover
            if technical.macd > technical.macd_signal:
                score += 1.5

            return min(max(score, 0), 10)
        except Exception as e:
            print(f"Error calculating technical score: {e}")
            return 5.0

    def _calculate_fundamental_score(
        self, fundamental: models.FundamentalIndicator
    ) -> float:
        try:
            score = 5.0

            pe_ratio = fundamental.pe_ratio
            if pe_ratio:
                if 10 <= pe_ratio <= 20:
                    score += 1.5
                elif 5 <= pe_ratio <= 30:
                    score += 1.0
                elif pe_ratio < 5 or pe_ratio > 50:
                    score -= 0.5

            pb_ratio = fundamental.pb_ratio
            if pb_ratio:
                if 1 <= pb_ratio <= 3:
                    score += 1.0
                elif pb_ratio < 1:
                    score += 1.5

            debt_to_equity = fundamental.debt_to_equity
            if debt_to_equity:
                if debt_to_equity < 50:
                    score += 1.0
                elif debt_to_equity > 100:
                    score -= 0.5

            profit_margin = fundamental.profit_margin
            if profit_margin:
                if profit_margin > 0.15:
                    score += 1.5
                elif profit_margin > 0.05:
                    score += 1.0
                elif profit_margin < 0:
                    score -= 1.0

            return min(max(score, 0), 10)
        except Exception as e:
            print(f"Error calculating fundamental score: {e}")
            return 5.0

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1] if not np.isnan(rsi.iloc[-1]) else 50

    def _calculate_macd(
        self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
    ):
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal).mean()
        return macd.iloc[-1], signal_line.iloc[-1]

    @staticmethod
    def _first_metric(metrics: dict, keys) -> Optional[float]:
        for k in keys:
            if k in metrics and metrics[k] is not None:
                try:
                    return float(metrics[k])
                except Exception:
                    continue
        return None
