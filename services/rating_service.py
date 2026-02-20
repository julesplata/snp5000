"""
Service for calculating stock ratings from multiple data sources
Integrates with yfinance for stock data and technical indicators
Includes macroeconomic analysis from FRED
"""

import yfinance as yf
import pandas as pd
import numpy as np
from typing import Dict, Optional
from datetime import datetime, timedelta
from services.macro_service import MacroeconomicService
import os


class RatingService:
    """Calculate stock ratings from multiple data sources"""

    def __init__(self):
        # Initialize macroeconomic service
        self.macro_service = MacroeconomicService()

        # Cache macro data (refresh every hour)
        self._macro_cache = None
        self._macro_cache_time = None

        self.weights = {
            "technical": 0.25,
            "analyst": 0.25,
            "fundamental": 0.25,
            "macro": 0.25,  # New: Macroeconomic environment
        }

    def calculate_rating(self, symbol: str) -> Dict:
        """
        Calculate comprehensive rating for a stock
        Returns dict with overall rating and component scores
        """
        try:
            stock = yf.Ticker(symbol)

            # Get historical data
            hist = stock.history(period="1y")

            if hist.empty:
                raise ValueError(f"No data available for {symbol}")

            # Calculate component scores
            technical_score = self._calculate_technical_score(hist, stock)
            fundamental_score = self._calculate_fundamental_score(stock)
            analyst_score = self._get_analyst_score(stock)

            # Get macroeconomic score (cached for 1 hour)
            macro_data = self._get_macro_score()
            macro_score = macro_data["macro_score"]

            # Calculate weighted overall rating
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
                    "technical": "alpaca",
                    "analyst": "yfinance",
                    "fundamental": "alpaca",
                    "macro": macro_data.get("data_source", "FRED"),
                },
            }

        except Exception as e:
            print(f"Error calculating rating for {symbol}: {e}")
            return None

    def _get_macro_score(self) -> Dict:
        """
        Get macroeconomic score with 1-hour caching
        Reduces API calls to FRED
        """
        from datetime import datetime, timedelta

        # Check if cache is still valid (1 hour)
        if self._macro_cache and self._macro_cache_time:
            cache_age = datetime.now() - self._macro_cache_time
            if cache_age < timedelta(hours=1):
                return self._macro_cache

        # Fetch fresh macro data
        macro_data = self.macro_service.calculate_macro_score()

        # Update cache
        self._macro_cache = macro_data
        self._macro_cache_time = datetime.now()

        return macro_data

    def _calculate_technical_score(self, hist: pd.DataFrame, stock) -> float:
        """
        Calculate technical analysis score (0-10)
        Based on moving averages, RSI, MACD
        """
        try:
            current_price = hist["Close"].iloc[-1]

            # Calculate moving averages
            sma_50 = hist["Close"].rolling(window=50).mean().iloc[-1]
            sma_200 = hist["Close"].rolling(window=200).mean().iloc[-1]

            # Calculate RSI
            rsi = self._calculate_rsi(hist["Close"])

            # Calculate MACD
            macd, signal = self._calculate_macd(hist["Close"])

            score = 5.0  # Base score

            # Price vs SMAs (40% weight)
            if current_price > sma_50:
                score += 1.5
            if current_price > sma_200:
                score += 1.5
            if sma_50 > sma_200:  # Golden cross
                score += 1.0

            # RSI (30% weight)
            if 40 <= rsi <= 60:  # Neutral is good
                score += 1.5
            elif 30 <= rsi <= 70:
                score += 1.0
            elif rsi < 30:  # Oversold
                score += 0.5
            elif rsi > 70:  # Overbought
                score += 0.5

            # MACD (30% weight)
            if macd > signal:
                score += 1.5

            return min(max(score, 0), 10)  # Clamp between 0-10

        except Exception as e:
            print(f"Error calculating technical score: {e}")
            return 5.0  # Return neutral score on error

    def _calculate_fundamental_score(self, stock) -> float:
        """
        Calculate fundamental analysis score (0-10)
        Based on P/E ratio, P/B ratio, debt-to-equity, etc.
        """
        try:
            info = stock.info
            score = 5.0  # Base score

            # P/E Ratio (30% weight)
            pe_ratio = info.get("trailingPE", None)
            if pe_ratio:
                if 10 <= pe_ratio <= 20:  # Healthy range
                    score += 1.5
                elif 5 <= pe_ratio <= 30:
                    score += 1.0
                elif pe_ratio < 5 or pe_ratio > 50:
                    score -= 0.5

            # P/B Ratio (20% weight)
            pb_ratio = info.get("priceToBook", None)
            if pb_ratio:
                if 1 <= pb_ratio <= 3:
                    score += 1.0
                elif pb_ratio < 1:  # Undervalued
                    score += 1.5

            # Debt to Equity (20% weight)
            debt_to_equity = info.get("debtToEquity", None)
            if debt_to_equity:
                if debt_to_equity < 50:
                    score += 1.0
                elif debt_to_equity > 100:
                    score -= 0.5

            # Profit Margins (30% weight)
            profit_margin = info.get("profitMargins", None)
            if profit_margin:
                if profit_margin > 0.15:  # 15%+
                    score += 1.5
                elif profit_margin > 0.05:
                    score += 1.0
                elif profit_margin < 0:
                    score -= 1.0

            return min(max(score, 0), 10)

        except Exception as e:
            print(f"Error calculating fundamental score: {e}")
            return 5.0


    def _get_analyst_score(self, stock) -> float:
        """
        Get analyst recommendations score (0-10)
        Based on analyst ratings from yfinance
        """
        try:
            info = stock.info
            recommendation = info.get("recommendationKey", None)

            # Map recommendations to scores
            recommendation_scores = {
                "strong_buy": 10.0,
                "buy": 8.0,
                "hold": 5.0,
                "sell": 2.0,
                "strong_sell": 0.0,
            }

            if recommendation:
                return recommendation_scores.get(recommendation, 5.0)

            # Fallback to recommendation mean
            rec_mean = info.get("recommendationMean", None)
            if rec_mean:
                # Scale from 1-5 to 0-10 (1=strong buy, 5=strong sell)
                return max(0, 10 - (rec_mean - 1) * 2.5)

            return 5.0  # Neutral if no data

        except Exception as e:
            print(f"Error getting analyst score: {e}")
            return 5.0

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1] if not np.isnan(rsi.iloc[-1]) else 50

    def _calculate_macd(
        self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
    ):
        """Calculate MACD and signal line"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal).mean()
        return macd.iloc[-1], signal_line.iloc[-1]

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """Get basic stock information"""
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            return {
                "symbol": symbol,
                "name": info.get("longName", symbol),
                "sector": info.get("sector", "Unknown"),
                "market_cap": info.get("marketCap", None),
                "current_price": info.get("currentPrice", None),
            }
        except Exception as e:
            print(f"Error fetching stock info: {e}")
            return None
