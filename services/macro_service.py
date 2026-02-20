import requests
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import os


class MacroeconomicService:
    """
    Fetch and analyze macroeconomic indicators from FRED
    Calculates a macro score (0-10) based on current economic conditions
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"

        if not self.api_key:
            print(
                "WARNING: FRED_API_KEY not set. Macro score will default to neutral (5.0)"
            )

        # FRED series IDs for key indicators
        self.series_ids = {
            "fed_funds_rate": "DFF",  # Federal Funds Effective Rate
            "inflation_cpi": "CPIAUCSL",  # Consumer Price Index
            "gdp_growth": "A191RL1Q225SBEA",  # Real GDP % change (quarterly)
            "unemployment": "UNRATE",  # Unemployment Rate
            "treasury_10y": "DGS10",  # 10-Year Treasury Constant Maturity Rate
            "treasury_2y": "DGS2",  # 2-Year Treasury (for yield curve)
            "consumer_sentiment": "UMCSENT",  # University of Michigan Consumer Sentiment
        }

        # Optimal ranges for each indicator (ideal for stocks)
        self.optimal_ranges = {
            "fed_funds_rate": (2.0, 4.0),  # Moderate rates
            "inflation": (1.5, 3.0),  # Stable, low inflation
            "gdp_growth": (2.0, 4.0),  # Healthy growth
            "unemployment": (3.5, 5.0),  # Full employment
            "treasury_10y": (3.0, 5.0),  # Moderate rates
            "yield_curve": (0.5, 2.0),  # Positive, not too steep
            "consumer_sentiment": (75, 95),  # Confident consumers
        }

    def calculate_macro_score(self) -> Dict:
        """
        Calculate overall macroeconomic score (0-10)

        Returns dict with:
        - macro_score: Overall score
        - components: Individual indicator scores
        - indicators: Raw indicator values
        - analysis: Text explanation
        """
        try:
            if not self.api_key:
                return self._get_default_score()

            # Fetch all indicators
            indicators = self._fetch_all_indicators()

            if not indicators:
                return self._get_default_score()

            # Calculate component scores
            interest_rate_score = self._score_interest_rates(
                indicators.get("fed_funds_rate"), indicators.get("treasury_10y")
            )

            inflation_score = self._score_inflation(indicators.get("inflation_rate"))

            growth_score = self._score_gdp_growth(indicators.get("gdp_growth"))

            employment_score = self._score_unemployment(indicators.get("unemployment"))

            yield_curve_score = self._score_yield_curve(
                indicators.get("treasury_10y"), indicators.get("treasury_2y")
            )

            sentiment_score = self._score_consumer_sentiment(
                indicators.get("consumer_sentiment")
            )

            # Weighted average (interest rates and inflation matter most for valuations)
            weights = {
                "interest_rates": 0.25,
                "inflation": 0.25,
                "growth": 0.20,
                "employment": 0.10,
                "yield_curve": 0.10,
                "sentiment": 0.10,
            }

            macro_score = (
                interest_rate_score * weights["interest_rates"]
                + inflation_score * weights["inflation"]
                + growth_score * weights["growth"]
                + employment_score * weights["employment"]
                + yield_curve_score * weights["yield_curve"]
                + sentiment_score * weights["sentiment"]
            )

            # Generate analysis text
            analysis = self._generate_analysis(
                macro_score,
                indicators,
                {
                    "interest_rates": interest_rate_score,
                    "inflation": inflation_score,
                    "growth": growth_score,
                    "employment": employment_score,
                    "yield_curve": yield_curve_score,
                    "sentiment": sentiment_score,
                },
            )

            return {
                "macro_score": round(macro_score, 2),
                "components": {
                    "interest_rates": round(interest_rate_score, 2),
                    "inflation": round(inflation_score, 2),
                    "growth": round(growth_score, 2),
                    "employment": round(employment_score, 2),
                    "yield_curve": round(yield_curve_score, 2),
                    "sentiment": round(sentiment_score, 2),
                },
                "indicators": indicators,
                "analysis": analysis,
                "data_source": "FRED",
            }

        except Exception as e:
            print(f"Error calculating macro score: {e}")
            return self._get_default_score()

    def _fetch_all_indicators(self) -> Dict:
        """Fetch all economic indicators from FRED"""
        indicators = {}

        try:
            # Federal Funds Rate
            fed_funds = self._fetch_latest_value("fed_funds_rate")
            if fed_funds:
                indicators["fed_funds_rate"] = fed_funds

            # Inflation (YoY change in CPI)
            inflation_rate = self._calculate_inflation_rate()
            if inflation_rate:
                indicators["inflation_rate"] = inflation_rate

            # GDP Growth (latest quarterly rate)
            gdp_growth = self._fetch_latest_value("gdp_growth")
            if gdp_growth:
                indicators["gdp_growth"] = gdp_growth

            # Unemployment Rate
            unemployment = self._fetch_latest_value("unemployment")
            if unemployment:
                indicators["unemployment"] = unemployment

            # Treasury Yields
            treasury_10y = self._fetch_latest_value("treasury_10y")
            if treasury_10y:
                indicators["treasury_10y"] = treasury_10y

            treasury_2y = self._fetch_latest_value("treasury_2y")
            if treasury_2y:
                indicators["treasury_2y"] = treasury_2y

            # Consumer Sentiment
            sentiment = self._fetch_latest_value("consumer_sentiment")
            if sentiment:
                indicators["consumer_sentiment"] = sentiment

            return indicators

        except Exception as e:
            print(f"Error fetching indicators: {e}")
            return {}

    def _fetch_latest_value(
        self, indicator_key: str, days_back: int = 90
    ) -> Optional[float]:
        """Fetch the most recent value for an indicator"""
        try:
            series_id = self.series_ids.get(indicator_key)
            if not series_id:
                return None

            # Get data from last 90 days to ensure we have recent value
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days_back)).strftime(
                "%Y-%m-%d"
            )

            params = {
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": start_date,
                "observation_end": end_date,
                "sort_order": "desc",
                "limit": 1,
            }

            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "observations" in data and len(data["observations"]) > 0:
                value = data["observations"][0]["value"]
                if value != ".":  # FRED uses '.' for missing data
                    return float(value)

            return None

        except Exception as e:
            print(f"Error fetching {indicator_key}: {e}")
            return None

    def _calculate_inflation_rate(self) -> Optional[float]:
        """Calculate year-over-year inflation rate from CPI data"""
        try:
            series_id = self.series_ids["inflation_cpi"]

            # Get CPI from last 13 months
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

            params = {
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": start_date,
                "observation_end": end_date,
                "sort_order": "desc",
                "limit": 13,
            }

            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "observations" in data and len(data["observations"]) >= 2:
                # Most recent CPI
                current_cpi = float(data["observations"][0]["value"])
                # CPI from ~12 months ago
                year_ago_cpi = float(data["observations"][-1]["value"])

                # Calculate YoY % change
                inflation_rate = ((current_cpi - year_ago_cpi) / year_ago_cpi) * 100
                return inflation_rate

            return None

        except Exception as e:
            print(f"Error calculating inflation: {e}")
            return None

    def _score_interest_rates(
        self, fed_funds: Optional[float], treasury_10y: Optional[float]
    ) -> float:
        """
        Score interest rate environment (0-10)
        Low-moderate rates are best for stocks (easier borrowing, higher valuations)
        Very high rates pressure valuations through higher discount rates
        """
        if not fed_funds and not treasury_10y:
            return 5.0

        rate = fed_funds if fed_funds else treasury_10y
        optimal_low, optimal_high = self.optimal_ranges["fed_funds_rate"]

        if rate < 1.0:
            # Too low - potential bubble concerns
            return 7.0
        elif optimal_low <= rate <= optimal_high:
            # Goldilocks - ideal for stocks
            return 9.0
        elif rate <= 6.0:
            # Moderate-high rates - some pressure
            penalty = (rate - optimal_high) / 2.0
            return max(4.0, 9.0 - penalty)
        else:
            # Very high rates - significant valuation pressure
            return 3.0

    def _score_inflation(self, inflation_rate: Optional[float]) -> float:
        """
        Score inflation environment (0-10)
        Moderate inflation is healthy. High inflation erodes purchasing power
        and forces Fed to raise rates (bad for stocks)
        """
        if not inflation_rate:
            return 5.0

        optimal_low, optimal_high = self.optimal_ranges["inflation"]

        if inflation_rate < 0:
            # Deflation - demand concerns
            return 4.0
        elif optimal_low <= inflation_rate <= optimal_high:
            # Healthy inflation
            return 9.0
        elif inflation_rate <= 5.0:
            # Moderate inflation - some concerns
            penalty = (inflation_rate - optimal_high) * 1.5
            return max(5.0, 9.0 - penalty)
        else:
            # High inflation - major concern for stocks
            return max(2.0, 10.0 - inflation_rate)

    def _score_gdp_growth(self, gdp_growth: Optional[float]) -> float:
        """
        Score GDP growth (0-10)
        Healthy growth supports corporate earnings
        """
        if not gdp_growth:
            return 5.0

        optimal_low, optimal_high = self.optimal_ranges["gdp_growth"]

        if gdp_growth < -2.0:
            # Recession
            return 2.0
        elif gdp_growth < 0:
            # Negative growth
            return 4.0
        elif optimal_low <= gdp_growth <= optimal_high:
            # Goldilocks growth
            return 9.0
        elif gdp_growth <= 6.0:
            # Strong growth (may bring inflation concerns)
            return 7.0
        else:
            # Overheating economy
            return 6.0

    def _score_unemployment(self, unemployment: Optional[float]) -> float:
        """
        Score unemployment (0-10)
        Low unemployment = healthy economy & consumer spending
        """
        if not unemployment:
            return 5.0

        optimal_low, optimal_high = self.optimal_ranges["unemployment"]

        if unemployment < 3.0:
            # Too low - wage inflation concerns
            return 7.0
        elif optimal_low <= unemployment <= optimal_high:
            # Full employment
            return 9.0
        elif unemployment <= 7.0:
            # Elevated unemployment
            penalty = (unemployment - optimal_high) * 1.0
            return max(4.0, 9.0 - penalty)
        else:
            # High unemployment - recession indicator
            return max(2.0, 12.0 - unemployment)

    def _score_yield_curve(
        self, treasury_10y: Optional[float], treasury_2y: Optional[float]
    ) -> float:
        """
        Score yield curve (0-10)
        Inverted curve (2Y > 10Y) predicts recession
        Normal upward slope is healthy
        """
        if not treasury_10y or not treasury_2y:
            return 5.0

        spread = treasury_10y - treasury_2y

        if spread < -0.5:
            # Deeply inverted - strong recession signal
            return 2.0
        elif spread < 0:
            # Inverted curve - recession warning
            return 4.0
        elif 0.5 <= spread <= 2.0:
            # Normal, healthy curve
            return 9.0
        elif spread <= 3.0:
            # Steep curve - recovery or inflation expectations
            return 7.0
        else:
            # Very steep - unusual conditions
            return 6.0

    def _score_consumer_sentiment(self, sentiment: Optional[float]) -> float:
        """
        Score consumer sentiment (0-10)
        Higher sentiment supports consumer spending (70% of GDP)
        """
        if not sentiment:
            return 5.0

        optimal_low, optimal_high = self.optimal_ranges["consumer_sentiment"]

        if sentiment < 60:
            # Very pessimistic
            return 3.0
        elif sentiment < 70:
            # Pessimistic
            return 5.0
        elif optimal_low <= sentiment <= optimal_high:
            # Healthy confidence
            return 9.0
        else:
            # Very optimistic (may signal peak)
            return 8.0

    def _generate_analysis(
        self, macro_score: float, indicators: Dict, components: Dict
    ) -> str:
        """Generate human-readable analysis of macro conditions"""

        # Overall assessment
        if macro_score >= 8.0:
            overall = "Favorable macroeconomic environment for stocks"
        elif macro_score >= 6.0:
            overall = "Moderately supportive macro conditions"
        elif macro_score >= 4.0:
            overall = "Mixed macroeconomic signals"
        else:
            overall = "Challenging macro environment for equities"

        # Key concerns/positives
        concerns = []
        positives = []

        # Interest rates
        if components.get("interest_rates", 5) >= 7:
            positives.append("interest rates in favorable range")
        elif components.get("interest_rates", 5) <= 4:
            concerns.append("elevated interest rates pressuring valuations")

        # Inflation
        inflation_rate = indicators.get("inflation_rate", 0)
        if inflation_rate and inflation_rate > 4.0:
            concerns.append(f"inflation elevated at {inflation_rate:.1f}%")
        elif inflation_rate and 1.5 <= inflation_rate <= 3.0:
            positives.append("inflation well-controlled")

        # GDP growth
        gdp = indicators.get("gdp_growth")
        if gdp and gdp < 0:
            concerns.append("negative GDP growth")
        elif gdp and gdp > 3.0:
            positives.append("strong economic growth")

        # Yield curve
        t10y = indicators.get("treasury_10y", 0)
        t2y = indicators.get("treasury_2y", 0)
        if t10y and t2y and (t10y - t2y) < 0:
            concerns.append("inverted yield curve (recession signal)")

        analysis = overall + ". "
        if positives:
            analysis += "Positives: " + ", ".join(positives) + ". "
        if concerns:
            analysis += "Concerns: " + ", ".join(concerns) + "."

        return analysis.strip()

    def _get_default_score(self) -> Dict:
        """Return neutral score when API key is not available"""
        return {
            "macro_score": 5.0,
            "components": {
                "interest_rates": 5.0,
                "inflation": 5.0,
                "growth": 5.0,
                "employment": 5.0,
                "yield_curve": 5.0,
                "sentiment": 5.0,
            },
            "indicators": {},
            "analysis": "Macro score unavailable (FRED API key not configured)",
            "data_source": "default",
        }
