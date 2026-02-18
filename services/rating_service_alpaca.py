import requests
import pandas as pd
import numpy as np
from typing import Dict, Optional
from datetime import datetime, timedelta
import time


class AlpacaRatingService:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://data.alpaca.markets/v2"
        self.headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}

        self.weights = {
            "technical": 0.30,
            "fundamental": 0.40,
            "momentum": 0.30,
        }

    def calculate_rating(self, symbol: str) -> Optional[Dict]:
        try:
            # Get historical data
            bars = self._get_historical_data(symbol, days=365)

            if bars is None or len(bars) == 0:
                print(f"No historical data available for {symbol}")
                return None

            # Get latest trade for current price
            latest_trade = self._get_latest_trade(symbol)

            # Calculate component scores
            technical_score = self._calculate_technical_score(bars)
            momentum_score = self._calculate_momentum_score(bars)
            fundamental_score = self._calculate_fundamental_score(symbol, bars)

            # Calculate weighted overall rating
            overall_rating = (
                technical_score * self.weights["technical"]
                + fundamental_score * self.weights["fundamental"]
                + momentum_score * self.weights["momentum"]
            )

            return {
                "overall_rating": round(overall_rating, 2),
                "technical_score": round(technical_score, 2),
                "fundamental_score": round(fundamental_score, 2),
                "momentum_score": round(momentum_score, 2),
                "analyst_score": None,  # Not available via Alpaca free tier
                "data_sources": {
                    "technical": "alpaca",
                    "fundamental": "alpaca",
                    "momentum": "alpaca",
                },
            }

        except Exception as e:
            print(f"Error calculating rating for {symbol}: {e}")
            return None

    def _get_historical_data(
        self, symbol: str, days: int = 365
    ) -> Optional[pd.DataFrame]:
        """Get historical price data from Alpaca"""
        try:
            end_date = datetime(2026,1,1)
            start_date = end_date - timedelta(days=days)

            url = f"{self.base_url}/stocks/{symbol}/bars"
            params = {
                "start": start_date.isoformat() + "Z",
                "end": end_date.isoformat() + "Z",
                "timeframe": "1Day",
                "limit": 10000,
            }

            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 429:
                print("Rate limited by Alpaca, waiting 60s...")
                time.sleep(60)
                response = requests.get(url, headers=self.headers, params=params)

            response.raise_for_status()
            data = response.json()

            if "bars" not in data or len(data["bars"]) == 0:
                return None

            df = pd.DataFrame(data["bars"])
            df["timestamp"] = pd.to_datetime(df["t"])
            df = df.rename(
                columns={
                    "o": "Open",
                    "h": "High",
                    "l": "Low",
                    "c": "Close",
                    "v": "Volume",
                }
            )
            df = df.set_index("timestamp")
            df = df.sort_index()

            return df

        except Exception as e:
            print(f"Error fetching historical data: {e}")
            return None

    def _get_latest_trade(self, symbol: str) -> Optional[Dict]:
        try:
            url = f"{self.base_url}/stocks/{symbol}/trades/latest"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching latest trade: {e}")
            return None

    def _calculate_technical_score(self, df: pd.DataFrame) -> float:
        try:
            current_price = df["Close"].iloc[-1]

            # Calculate moving averages
            sma_50 = df["Close"].rolling(window=50).mean().iloc[-1]
            sma_200 = df["Close"].rolling(window=200).mean().iloc[-1]

            # Calculate RSI
            rsi = self._calculate_rsi(df["Close"])

            # Calculate MACD
            macd, signal = self._calculate_macd(df["Close"])

            score = 5.0  # Base score

            # Price vs SMAs (40% weight)
            if not pd.isna(sma_50) and current_price > sma_50:
                score += 1.5
            if not pd.isna(sma_200) and current_price > sma_200:
                score += 1.5
            if not pd.isna(sma_50) and not pd.isna(sma_200) and sma_50 > sma_200:
                score += 1.0

            # RSI (30% weight)
            if not pd.isna(rsi):
                if 40 <= rsi <= 60:
                    score += 1.5
                elif 30 <= rsi <= 70:
                    score += 1.0
                elif rsi < 30:
                    score += 0.5
                elif rsi > 70:
                    score += 0.5

            if not pd.isna(macd) and not pd.isna(signal) and macd > signal:
                score += 1.5

            return min(max(score, 0), 10)

        except Exception as e:
            print(f"Error calculating technical score: {e}")
            return 5.0

    def _calculate_fundamental_score(self, symbol: str, df: pd.DataFrame) -> float:
        try:
            score = 5.0

            # Volume trend (20% weight)
            recent_volume = df["Volume"].tail(20).mean()
            older_volume = df["Volume"].tail(60).head(40).mean()
            if recent_volume > older_volume * 1.2:
                score += 1.0
            elif recent_volume > older_volume:
                score += 0.5

            # Volatility (30% weight)
            returns = df["Close"].pct_change()
            volatility = returns.std() * np.sqrt(252)  # Annualized
            if volatility < 0.20:  # Low volatility
                score += 1.5
            elif volatility < 0.35:  # Moderate
                score += 1.0
            elif volatility > 0.60:  # High volatility
                score -= 0.5

            # Price stability (25% weight)
            price_range = (df["High"].tail(20).max() - df["Low"].tail(20).min()) / df[
                "Close"
            ].iloc[-1]
            if price_range < 0.10:  # Stable
                score += 1.25
            elif price_range < 0.20:
                score += 0.75

            # Uptrend strength (25% weight)
            sma_20 = df["Close"].rolling(window=20).mean()
            sma_50 = df["Close"].rolling(window=50).mean()
            if not sma_20.empty and not sma_50.empty:
                if sma_20.iloc[-1] > sma_50.iloc[-1]:
                    score += 1.25

            return min(max(score, 0), 10)

        except Exception as e:
            print(f"Error calculating fundamental score: {e}")
            return 5.0

    def _calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """
        Calculate momentum score (0-10)
        Based on recent price performance
        """
        try:
            current_price = df["Close"].iloc[-1]

            # 1 week return
            week_ago_price = df["Close"].iloc[-5] if len(df) >= 5 else current_price
            week_return = (current_price - week_ago_price) / week_ago_price

            # 1 month return
            month_ago_price = df["Close"].iloc[-21] if len(df) >= 21 else current_price
            month_return = (current_price - month_ago_price) / month_ago_price

            # 3 month return
            three_month_ago_price = (
                df["Close"].iloc[-63] if len(df) >= 63 else current_price
            )
            three_month_return = (
                current_price - three_month_ago_price
            ) / three_month_ago_price

            score = 5.0

            # Weight recent performance more heavily
            if week_return > 0.02:
                score += 1.5
            elif week_return > 0:
                score += 0.5
            elif week_return < -0.02:
                score -= 1.5

            if month_return > 0.05:
                score += 1.5
            elif month_return > 0:
                score += 1.0
            elif month_return < -0.05:
                score -= 1.5

            if three_month_return > 0.10:
                score += 1.5
            elif three_month_return > 0:
                score += 1.0
            elif three_month_return < -0.10:
                score -= 1.5

            return min(max(score, 0), 10)

        except Exception as e:
            print(f"Error calculating momentum score: {e}")
            return 5.0

    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> float:
        """Calculate Relative Strength Index"""
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
        except:
            return 50

    def _calculate_macd(
        self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
    ):
        """Calculate MACD and signal line"""
        try:
            ema_fast = prices.ewm(span=fast).mean()
            ema_slow = prices.ewm(span=slow).mean()
            macd = ema_fast - ema_slow
            signal_line = macd.ewm(span=signal).mean()
            return macd.iloc[-1], signal_line.iloc[-1]
        except:
            return 0, 0

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """Get basic stock information"""
        try:
            # Get latest quote
            url = f"{self.base_url}/stocks/{symbol}/quotes/latest"
            response = requests.get(url, headers=self.headers)

            if response.status_code == 429:
                print("Rate limited, waiting 60s...")
                time.sleep(60)
                response = requests.get(url, headers=self.headers)

            response.raise_for_status()
            quote = response.json()

            # Get some historical data for additional info
            bars = self._get_historical_data(symbol, days=30)

            result = {
                "symbol": symbol,
                "name": symbol,  # Alpaca doesn't provide company names in free tier
                "sector": "Unknown",
                "current_price": quote.get("quote", {}).get("ap", None),  # Ask price
            }

            if bars is not None and len(bars) > 0:
                # Estimate market cap from volume and price (rough approximation)
                avg_volume = bars["Volume"].mean()
                current_price = bars["Close"].iloc[-1]
                result["market_cap"] = None  # Not available in free tier
                result["current_price"] = current_price

            return result

        except Exception as e:
            print(f"Error fetching stock info: {e}")
            return None
