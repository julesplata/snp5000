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
        self.series_release_url = "https://api.stlouisfed.org/fred/series/release"
        self.release_dates_url = "https://api.stlouisfed.org/fred/release/dates"

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

        # Cache release lookups to avoid extra API calls
        self._release_cache: Dict[str, Dict] = {}

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
            indicators, indicator_meta = self._fetch_all_indicators()

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

            indicator_context = self._generate_indicator_context(
                indicators,
                {
                    "interest_rates": interest_rate_score,
                    "inflation": inflation_score,
                    "growth": growth_score,
                    "employment": employment_score,
                    "yield_curve": yield_curve_score,
                    "sentiment": sentiment_score,
                },
                indicator_meta,
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
                "indicator_context": indicator_context,
                "indicator_meta": indicator_meta,
                "analysis": analysis,
                "data_source": "FRED",
            }

        except Exception as e:
            print(f"Error calculating macro score: {e}")
            return self._get_default_score()

    def _fetch_all_indicators(self) -> Tuple[Dict, Dict]:
        """Fetch all economic indicators from FRED with publication metadata"""
        indicators: Dict[str, float] = {}
        meta: Dict[str, Dict] = {}

        try:
            # Federal Funds Rate
            fed_funds = self._fetch_latest_observation("fed_funds_rate")
            if fed_funds and fed_funds.get("value") is not None:
                indicators["fed_funds_rate"] = fed_funds["value"]
                meta["fed_funds_rate"] = self._build_meta("fed_funds_rate", fed_funds)

            # Inflation (YoY change in CPI)
            inflation_rate = self._calculate_inflation_rate()
            if inflation_rate and inflation_rate.get("value") is not None:
                indicators["inflation_rate"] = inflation_rate["value"]
                meta["inflation_rate"] = self._build_meta(
                    "inflation_cpi", inflation_rate
                )

            # GDP Growth (latest quarterly rate)
            gdp_growth = self._fetch_latest_observation("gdp_growth")
            if gdp_growth and gdp_growth.get("value") is not None:
                indicators["gdp_growth"] = gdp_growth["value"]
                meta["gdp_growth"] = self._build_meta("gdp_growth", gdp_growth)

            # Unemployment Rate
            unemployment = self._fetch_latest_observation("unemployment")
            if unemployment and unemployment.get("value") is not None:
                indicators["unemployment"] = unemployment["value"]
                meta["unemployment"] = self._build_meta("unemployment", unemployment)

            # Treasury Yields
            treasury_10y = self._fetch_latest_observation("treasury_10y")
            if treasury_10y and treasury_10y.get("value") is not None:
                indicators["treasury_10y"] = treasury_10y["value"]
                meta["treasury_10y"] = self._build_meta("treasury_10y", treasury_10y)

            treasury_2y = self._fetch_latest_observation("treasury_2y")
            if treasury_2y and treasury_2y.get("value") is not None:
                indicators["treasury_2y"] = treasury_2y["value"]
                meta["treasury_2y"] = self._build_meta("treasury_2y", treasury_2y)

            # Consumer Sentiment
            sentiment = self._fetch_latest_observation("consumer_sentiment")
            if sentiment and sentiment.get("value") is not None:
                indicators["consumer_sentiment"] = sentiment["value"]
                meta["consumer_sentiment"] = self._build_meta(
                    "consumer_sentiment", sentiment
                )

            return indicators, meta

        except Exception as e:
            print(f"Error fetching indicators: {e}")
            return {}, {}

    def _fetch_latest_observation(
        self, indicator_key: str, days_back: int = 400, observations: int = 2
    ) -> Optional[Dict]:
        """Fetch the most recent observation (and optional previous) for an indicator"""

        try:
            series_id = self.series_ids.get(indicator_key)
            if not series_id:
                return None

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
                "limit": observations,
            }

            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            observations_list = data.get("observations", [])
            if not observations_list:
                return None

            latest = observations_list[0]
            value = latest.get("value")
            if value == ".":
                return None

            previous = observations_list[1] if len(observations_list) > 1 else None

            result = {
                "value": float(value),
                "date": latest.get("date"),
            }

            if previous and previous.get("value") and previous.get("value") != ".":
                result["previous_value"] = float(previous.get("value"))
                result["previous_date"] = previous.get("date")

            return result

        except Exception as e:
            print(f"Error fetching {indicator_key}: {e}")
            return None

    def _calculate_inflation_rate(self) -> Optional[Dict]:
        """Calculate year-over-year inflation rate from CPI data with metadata"""
        try:
            series_id = self.series_ids["inflation_cpi"]

            # Get CPI from last 13 months (enough for YoY)
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

            params = {
                "series_id": series_id,
                "api_key": self.api_key,
                "file_type": "json",
                "observation_start": start_date,
                "observation_end": end_date,
                "sort_order": "desc",
                "limit": 14,
            }

            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            observations = data.get("observations", [])
            if len(observations) < 2:
                return None

            current = observations[0]
            year_ago = observations[-1]

            current_cpi = float(current.get("value"))
            year_ago_cpi = float(year_ago.get("value"))

            inflation_rate = ((current_cpi - year_ago_cpi) / year_ago_cpi) * 100

            result = {
                "value": inflation_rate,
                "date": current.get("date"),
            }

            # Best-effort previous YoY figure for change context
            if len(observations) >= 14:
                prev_current = observations[1]
                prev_year_ago = observations[-2]
                try:
                    prev_inflation = (
                        (
                            float(prev_current.get("value"))
                            - float(prev_year_ago.get("value"))
                        )
                        / float(prev_year_ago.get("value"))
                    ) * 100
                    result["previous_value"] = prev_inflation
                    result["previous_date"] = prev_current.get("date")
                except Exception:
                    pass

            return result

        except Exception as e:
            print(f"Error calculating inflation: {e}")
            return None

    def _build_meta(self, indicator_key: str, observation: Dict) -> Dict:
        """Add publication and next-release metadata for an indicator"""

        meta: Dict[str, Optional[str]] = {
            "published_at": observation.get("date"),
        }

        if observation.get("previous_value") is not None:
            meta["previous"] = {
                "value": observation.get("previous_value"),
                "date": observation.get("previous_date"),
            }

        next_release = self._get_next_release_date(indicator_key)
        if next_release:
            meta["next_release"] = next_release

        return meta

    def _get_next_release_date(self, indicator_key: str) -> Optional[str]:
        """Look up the next scheduled release date for a series (best effort)"""

        try:
            # Cache hit
            cached = self._release_cache.get(indicator_key)
            if cached and cached.get("next_release"):
                return cached.get("next_release")

            series_id = self.series_ids.get(indicator_key)
            if not series_id:
                return None

            # First find the release_id associated with the series
            release_id = self._release_cache.get(indicator_key, {}).get("release_id")

            if not release_id:
                params = {
                    "series_id": series_id,
                    "api_key": self.api_key,
                    "file_type": "json",
                }

                resp = requests.get(self.series_release_url, params=params, timeout=10)
                resp.raise_for_status()
                release_payload = resp.json()
                releases = release_payload.get("releases") or release_payload.get(
                    "release"
                )
                if releases and isinstance(releases, list):
                    release_id = releases[0].get("id")

            if not release_id:
                return None

            # Query the release calendar
            params = {
                "release_id": release_id,
                "api_key": self.api_key,
                "file_type": "json",
                "include_release_dates_with_no_data": True,
                "order_by": "release_date",
                "sort_order": "asc",
                "limit": 25,
            }

            resp = requests.get(self.release_dates_url, params=params, timeout=10)
            resp.raise_for_status()
            dates_payload = resp.json()
            release_dates = dates_payload.get("release_dates", [])

            today = datetime.now().date()
            future_dates = []
            for item in release_dates:
                date_str = item.get("date")
                try:
                    parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if parsed >= today:
                        future_dates.append(parsed)
                except Exception:
                    continue

            next_release = (
                min(future_dates).strftime("%Y-%m-%d") if future_dates else None
            )

            # Cache the result
            self._release_cache[indicator_key] = {
                "release_id": release_id,
                "next_release": next_release,
            }

            return next_release

        except Exception as e:
            print(f"Error fetching next release for {indicator_key}: {e}")
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

    def _generate_indicator_context(
        self, indicators: Dict, components: Dict, indicator_meta: Dict
    ) -> Dict:
        """Attach reusable, category-style context plus timing metadata for each indicator"""

        meta = {
            "inflation_rate": self.optimal_ranges.get("inflation"),
            "unemployment": self.optimal_ranges.get("unemployment"),
            "gdp_growth": self.optimal_ranges.get("gdp_growth"),
            "fed_funds_rate": self.optimal_ranges.get("fed_funds_rate"),
            "treasury_10y": self.optimal_ranges.get("treasury_10y"),
            "consumer_sentiment": self.optimal_ranges.get("consumer_sentiment"),
        }

        context = {}

        # Yield curve spread is derived, not fetched directly
        t10y = indicators.get("treasury_10y")
        t2y = indicators.get("treasury_2y")
        if t10y is not None and t2y is not None:
            indicators["yield_curve_spread"] = round(t10y - t2y, 2)
            meta["yield_curve_spread"] = self.optimal_ranges.get("yield_curve")

        for key, value in indicators.items():
            if value is None:
                continue

            optimal = meta.get(key)
            category = description = None
            change = change_pct = trend = None

            if optimal:
                category = self._categorize_against_optimal(value, optimal)
                description = self._generic_description(key, category, optimal)
            else:
                # Fallback: map component score if available
                component_key = self._map_indicator_to_component(key)
                score = components.get(component_key) if component_key else None
                category = (
                    self._score_to_category(score) if score is not None else "n/a"
                )
                description = f"{component_key or key} sits in {category} territory."

            prev_meta = (
                indicator_meta.get(key, {}).get("previous") if indicator_meta else None
            )
            if prev_meta and prev_meta.get("value") is not None:
                prev_val = prev_meta["value"]
                change = round(value - prev_val, 4)
                if prev_val != 0:
                    change_pct = round((change / prev_val) * 100, 3)
                if change > 0:
                    trend = "up"
                elif change < 0:
                    trend = "down"
                else:
                    trend = "flat"

            context[key] = {
                "category": category,
                "description": description,
                "value": round(value, 2) if isinstance(value, (int, float)) else value,
                "published_at": indicator_meta.get(key, {}).get("published_at"),
                "next_release": indicator_meta.get(key, {}).get("next_release"),
                "previous": indicator_meta.get(key, {}).get("previous"),
                "change": change,
                "change_pct": change_pct,
                "trend": trend,
                "expectation_supported": False,  # FRED does not include forecasts/consensus
                "surprise": None,
            }

        return context

    def _categorize_against_optimal(
        self, value: float, optimal_range: Tuple[float, float]
    ) -> str:
        """Generic bucketing versus optimal range"""

        low, high = optimal_range
        width = max(high - low, 0.01)

        if low <= value <= high:
            return "ideal"
        if low - width * 0.5 <= value <= high + width * 0.5:
            return "acceptable"
        if value < low:
            return "below-range"
        return "above-range"

    def _generic_description(
        self, indicator_key: str, category: str, optimal_range: Tuple[float, float]
    ) -> str:
        """Plain template description usable across indicators"""

        low, high = optimal_range
        return f"{indicator_key} is {category} relative to target band {low:.1f}-{high:.1f}."

    def _map_indicator_to_component(self, indicator_key: str) -> Optional[str]:
        mapping = {
            "fed_funds_rate": "interest_rates",
            "treasury_10y": "interest_rates",
            "treasury_2y": "interest_rates",
            "yield_curve_spread": "yield_curve",
            "inflation_rate": "inflation",
            "gdp_growth": "growth",
            "unemployment": "employment",
            "consumer_sentiment": "sentiment",
        }
        return mapping.get(indicator_key)

    def _score_to_category(self, score: Optional[float]) -> str:
        if score is None:
            return "unknown"
        if score >= 8:
            return "very strong"
        if score >= 6:
            return "favorable"
        if score >= 4:
            return "neutral"
        if score >= 2:
            return "caution"
        return "weak"

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
            "indicator_context": {},
            "indicator_meta": {},
            "analysis": "Macro score unavailable (FRED API key not configured)",
            "data_source": "default",
        }
