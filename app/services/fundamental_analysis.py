from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional, List, Tuple
import statistics


class PeerAnalyzer:
    """Compute peer statistics and handle peer-relative analysis."""

    def __init__(self, peer_data: Dict) -> None:
        """
        Args:
            peer_data: {
                "metadata": {...},
                "peers": [{peer_id, name, ticker, metrics: {...}}, ...],
                "your_stock": {stock_id, name, ...}
            }
        """
        self.metadata = peer_data.get("metadata", {})
        self.peers = peer_data.get("peers", [])
        self.your_stock = peer_data.get("your_stock", {})

    def compute_peer_stats(self, metric_name: str) -> Dict:
        """
        Compute mean, median, std, min, max, percentile, z-score for a metric.
        Returns dict with all stats and your stock's relative position.
        """
        peer_values = [
            p["metrics"].get(metric_name)
            for p in self.peers
            if p["metrics"].get(metric_name) is not None
        ]

        if not peer_values:
            return {
                "peer_mean": None,
                "peer_median": None,
                "peer_std": None,
                "peer_min": None,
                "peer_max": None,
                "your_value": None,
                "your_percentile": None,
                "your_z_score": None,
                "your_rank": "N/A",
                "note": "Insufficient peer data",
            }

        peer_mean = statistics.mean(peer_values)
        peer_median = statistics.median(peer_values)
        peer_std = statistics.stdev(peer_values) if len(peer_values) > 1 else 0
        peer_min = min(peer_values)
        peer_max = max(peer_values)

        # Compute percentile: rank position / total peers * 100
        sorted_peers = sorted(peer_values)
        rank = sum(1 for v in sorted_peers if v <= peer_mean)  # Simplified; can refine
        percentile = (rank / len(peer_values)) * 100 if peer_values else None

        # Compute z-score
        your_value = self.your_stock.get("metrics", {}).get(metric_name)
        z_score = None
        rank_str = "N/A"

        if your_value is not None:
            if peer_std > 0:
                z_score = (your_value - peer_mean) / peer_std

            # Determine rank
            ranked_peers = sorted(enumerate(peer_values), key=lambda x: x[1])
            for idx, (peer_idx, peer_val) in enumerate(ranked_peers):
                if abs(peer_val - your_value) < 1e-6:  # Approximate match
                    rank_str = f"{idx + 1} of {len(peer_values)}"
                    break

            # Flag outliers
            if z_score is not None:
                if z_score > 2 or z_score < -2:
                    rank_str = f"outlier ({rank_str})"

        return {
            "peer_mean": round(peer_mean, 4) if peer_mean else None,
            "peer_median": round(peer_median, 4) if peer_median else None,
            "peer_std": round(peer_std, 4) if peer_std else None,
            "peer_min": round(peer_min, 4) if peer_min else None,
            "peer_max": round(peer_max, 4) if peer_max else None,
            "your_value": round(your_value, 4) if your_value is not None else None,
            "your_percentile": round(percentile, 1) if percentile else None,
            "your_z_score": round(z_score, 2) if z_score is not None else None,
            "your_rank": rank_str,
        }

    def compute_all_peer_stats(self, metrics_to_analyze: List[str]) -> Dict[str, dict]:
        """Compute peer stats for multiple metrics."""
        stats = {}
        for metric in metrics_to_analyze:
            stats[metric] = self.compute_peer_stats(metric)
        return stats

    def recalibrate_benchmarks(
        self, metrics_to_analyze: List[str], sigma_range: float = 1.0
    ) -> Dict[str, Tuple[float, float]]:
        """
        Recalibrate benchmarks based on peer universe.
        New range: [peer_mean - sigma_range*std, peer_mean + sigma_range*std]
        """
        benchmarks = {}
        for metric in metrics_to_analyze:
            stats = self.compute_peer_stats(metric)
            if stats["peer_mean"] is None:
                continue

            mean = stats["peer_mean"]
            std = stats["peer_std"] or 0

            lower = max(0, mean - (sigma_range * std))
            upper = mean + (sigma_range * std)

            benchmarks[metric] = (lower, upper)

        return benchmarks


class ComparableAnalysis:
    """
    Comparable Company Analysis (CCA).

    Compares a stock's key ratios against sector peers using percentile and
    Z-score ranking, then produces a valuation verdict by crossing the P/E
    premium/discount against the stock's ROE standing within the sector.

    Verdict labels:
        justified_premium  — high P/E, high ROE  → Neutral/Buy
        overvalued         — high P/E, low ROE   → Sell
        undervalued_gem    — low P/E,  high ROE  → Strong Buy
        discount           — low P/E,  low ROE   → Review
        fair_value         — within ±15% of sector P/E median
        insufficient_data  — P/E unavailable
    """

    PEER_METRICS = ["pe_ratio", "pb_ratio", "debt_to_equity", "net_margin", "roe"]

    # P/E vs. sector median thresholds (%)
    PREMIUM_THRESHOLD = 15.0
    DISCOUNT_THRESHOLD = -15.0
    # ROE percentile required to be considered "high ROE"
    HIGH_ROE_PERCENTILE = 60.0

    def __init__(
        self,
        stock_metrics: Dict[str, Optional[float]],
        peer_metrics_list: List[Dict[str, Optional[float]]],
        sector_name: Optional[str] = None,
    ) -> None:
        self.stock = stock_metrics
        self.peers = peer_metrics_list
        self.sector_name = sector_name

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(self) -> Dict:
        if not self.peers:
            return {"available": False, "reason": "no_sector_peers"}

        metrics_result: Dict[str, dict] = {}
        for metric in self.PEER_METRICS:
            peer_vals = [p[metric] for p in self.peers if p.get(metric) is not None]
            your_val = self.stock.get(metric)

            if not peer_vals or your_val is None:
                metrics_result[metric] = {"available": False}
                continue

            median = statistics.median(peer_vals)
            mean = statistics.mean(peer_vals)
            premium_pct = (
                ((your_val - median) / abs(median)) * 100 if median != 0 else None
            )

            metrics_result[metric] = {
                "your_value": round(your_val, 4),
                "sector_median": round(median, 4),
                "sector_mean": round(mean, 4),
                "premium_pct": (
                    round(premium_pct, 1) if premium_pct is not None else None
                ),
                "z_score": self._z_score(your_val, peer_vals),
                "percentile": self._percentile_rank(your_val, peer_vals),
                "peer_count": len(peer_vals),
            }

        return {
            "available": True,
            "sector": self.sector_name,
            "peer_count": len(self.peers),
            "metrics": metrics_result,
            "valuation_verdict": self._valuation_verdict(metrics_result),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _percentile_rank(value: float, all_values: List[float]) -> float:
        below = sum(1 for v in all_values if v < value)
        return round(below / len(all_values) * 100, 1)

    @staticmethod
    def _z_score(value: float, all_values: List[float]) -> Optional[float]:
        if len(all_values) < 2:
            return None
        mean = statistics.mean(all_values)
        std = statistics.stdev(all_values)
        if std == 0:
            return 0.0
        return round((value - mean) / std, 2)

    def _valuation_verdict(self, metrics_result: Dict) -> Dict:
        pe_data = metrics_result.get("pe_ratio", {})
        roe_data = metrics_result.get("roe", {})

        pe_premium = (
            pe_data.get("premium_pct") if pe_data.get("available", True) else None
        )
        roe_percentile = (
            roe_data.get("percentile") if roe_data.get("available", True) else None
        )

        if pe_premium is None:
            return {
                "label": "insufficient_data",
                "explanation": "P/E data unavailable for CCA verdict.",
                "pe_premium_pct": None,
                "roe_percentile": roe_percentile,
            }

        roe_str = f"{roe_percentile:.0f}th" if roe_percentile is not None else "unknown"

        if pe_premium > self.PREMIUM_THRESHOLD:
            if (
                roe_percentile is not None
                and roe_percentile >= self.HIGH_ROE_PERCENTILE
            ):
                label = "justified_premium"
                explanation = (
                    f"Trading at {pe_premium:+.1f}% P/E premium to sector median, "
                    f"but ROE ranks in {roe_str} percentile of peers — "
                    "premium appears earned. Neutral/Buy."
                )
            else:
                label = "overvalued"
                explanation = (
                    f"Trading at {pe_premium:+.1f}% P/E premium to sector median "
                    f"with ROE in {roe_str} percentile — "
                    "premium not supported by returns. Sell signal."
                )
        elif pe_premium < self.DISCOUNT_THRESHOLD:
            if (
                roe_percentile is not None
                and roe_percentile >= self.HIGH_ROE_PERCENTILE
            ):
                label = "undervalued_gem"
                explanation = (
                    f"Trading at {pe_premium:+.1f}% P/E discount to sector median "
                    f"with ROE in {roe_str} percentile — "
                    "strong returns at a discount. Strong Buy."
                )
            else:
                label = "discount"
                explanation = (
                    f"Trading at {pe_premium:+.1f}% P/E discount to sector median "
                    f"with ROE in {roe_str} percentile — "
                    "discount may reflect weaker returns. Review before buying."
                )
        else:
            label = "fair_value"
            explanation = (
                f"Trading within ±{self.PREMIUM_THRESHOLD:.0f}% of sector P/E median "
                f"({pe_premium:+.1f}%) — broadly at fair value."
            )

        return {
            "label": label,
            "explanation": explanation,
            "pe_premium_pct": pe_premium,
            "roe_percentile": roe_percentile,
        }


class FundamentalAnalysisEngine:
    """Normalize raw fundamental metrics and produce structured analysis."""

    BENCHMARKS: Dict[str, dict] = {
        "pe_ratio": {"range": (10, 50), "lower_better": True},
        "pb_ratio": {"range": (1, 60), "lower_better": True},
        "debt_to_equity": {"range": (0, 2), "lower_better": True},
        "net_margin": {"range": (5, 40), "lower_better": False},
        "roe": {"range": (10, 200), "lower_better": False},
        "current_ratio": {"range": (0.5, 3), "lower_better": False},
        "eps_growth_yoy": {"range": (0, 50), "lower_better": False},
        "revenue_growth_yoy": {"range": (0, 40), "lower_better": False},
        "dividend_yield": {"range": (0, 8), "lower_better": False},
        "payout_ratio": {"range": (0, 100), "lower_better": False},
    }

    STYLE_WEIGHTS: Dict[str, Dict[str, float]] = {
        "growth": {
            "eps_growth_yoy": 0.25,
            "revenue_growth_yoy": 0.20,
            "roe": 0.20,
            "net_margin": 0.15,
            "pe_ratio": 0.10,
            "debt_to_equity": 0.10,
        },
        "value": {
            "pe_ratio": 0.25,
            "pb_ratio": 0.20,
            "dividend_yield": 0.20,
            "debt_to_equity": 0.15,
            "current_ratio": 0.10,
            "roe": 0.10,
        },
        "income": {
            "dividend_yield": 0.30,
            "payout_ratio": 0.20,
            "current_ratio": 0.20,
            "debt_to_equity": 0.15,
            "net_margin": 0.15,
        },
        "quality": {
            "roe": 0.25,
            "net_margin": 0.20,
            "current_ratio": 0.15,
            "debt_to_equity": 0.15,
            "eps_growth_yoy": 0.10,
        },
    }

    def analyze(
        self,
        record,
        investment_style: str = "value",
        peer_data: Optional[dict] = None,
        use_peer_benchmarks: bool = True,
    ) -> dict:
        """
        Main analysis pipeline.

        Args:
            record: Financial data record with raw_metrics
            investment_style: "growth", "value", "income", or "quality"
            peer_data: Optional peer universe data
            use_peer_benchmarks: If True and peer_data provided, recalibrate benchmarks

        Returns:
            Complete analysis dict with normalized scores, anomalies, composites, narrative
        """
        metrics = self._extract_metrics(record)

        # Step 1: Determine which benchmarks to use
        benchmarks = dict(self.BENCHMARKS)  # Default
        peer_stats = None
        peer_analyzer = None

        if peer_data and use_peer_benchmarks:
            peer_analyzer = PeerAnalyzer(peer_data)
            metric_names = list(metrics.keys())
            dynamic_benches = peer_analyzer.recalibrate_benchmarks(metric_names)
            # Merge: use peer benchmarks where available, fallback to defaults
            benchmarks.update(
                {
                    m: {
                        "range": dynamic_benches[m],
                        "lower_better": self.BENCHMARKS[m]["lower_better"],
                    }
                    for m in metric_names
                    if m in dynamic_benches
                }
            )
            peer_stats = peer_analyzer.compute_all_peer_stats(metric_names)

        # Step 2: Normalize metrics using (possibly recalibrated) benchmarks
        normalized = self._normalize_metrics(metrics, benchmarks)

        # Step 3: Detect anomalies
        anomalies = self._detect_anomalies(metrics)

        # Step 4: Compute composite scores
        composite_scores = self._composite_scores(normalized)

        # Step 5: Detect style mismatch
        style_mismatch = self._detect_style_mismatch(composite_scores, investment_style)

        # Step 6: Compute confidence
        confidence = self._confidence_score(
            metrics, anomalies, investment_style, composite_scores
        )

        # Step 7: Risk rating
        risk_rating = self._risk_rating(anomalies, confidence)

        # Step 8: Build narrative
        narrative = self._build_narrative(
            metrics=metrics,
            normalized=normalized,
            composite=composite_scores,
            investment_style=investment_style,
            risk_rating=risk_rating,
            confidence=confidence,
            peer_stats=peer_stats,
            style_mismatch=style_mismatch,
        )

        return {
            "metadata": {
                "stock_id": record.stock_id,
                "analysis_timestamp": datetime.utcnow().isoformat(),
                "investment_style": investment_style,
                "benchmarks_used": "peer_adjusted_v1" if peer_data else "default_v1",
                "data_source": record.data_source,
                "peer_universe": peer_analyzer.metadata if peer_analyzer else None,
            },
            "normalized_scores": normalized,
            "anomalies": anomalies,
            "composite_scores": composite_scores,
            "style_mismatch": style_mismatch,
            "narrative": narrative,
            "peer_comparison": peer_stats,
            "raw_snapshot": {
                "pe_ratio": metrics.get("pe_ratio"),
                "pb_ratio": metrics.get("pb_ratio"),
                "debt_to_equity": metrics.get("debt_to_equity"),
                "net_margin": metrics.get("net_margin"),
                "roe": metrics.get("roe"),
                "current_ratio": metrics.get("current_ratio"),
                "eps_growth_yoy": metrics.get("eps_growth_yoy"),
                "revenue_growth_yoy": metrics.get("revenue_growth_yoy"),
                "dividend_yield": metrics.get("dividend_yield"),
                "payout_ratio": metrics.get("payout_ratio"),
            },
        }

    def _extract_metrics(self, record) -> Dict[str, Optional[float]]:
        """Extract and prioritize metrics from raw data."""
        raw = record.raw_metrics or {}
        return {
            "pe_ratio": self._first_non_null(
                [record.pe_ratio, raw.get("peTTM"), raw.get("peBasicExclExtraTTM")]
            ),
            "pb_ratio": self._first_non_null(
                [record.pb_ratio, raw.get("pbAnnual"), raw.get("pb")]
            ),
            "debt_to_equity": self._first_non_null(
                [
                    record.debt_to_equity,
                    raw.get("totalDebt/totalEquityAnnual"),
                    raw.get("totalDebtToEquityAnnual"),
                ]
            ),
            "net_margin": self._first_non_null(
                [
                    record.profit_margin,
                    raw.get("netProfitMarginTTM"),
                    raw.get("netMargin"),
                ]
            ),
            "roe": self._first_non_null(
                [raw.get("roeTTM"), raw.get("roeRfy"), raw.get("roe5Y")]
            ),
            "current_ratio": self._first_non_null(
                [
                    raw.get("currentRatioQuarterly"),
                    raw.get("currentRatioAnnual"),
                ]
            ),
            "eps_growth_yoy": self._first_non_null(
                [
                    raw.get("epsGrowthTTMYoy"),
                    raw.get("epsGrowthQuarterlyYoy"),
                ]
            ),
            "revenue_growth_yoy": self._first_non_null(
                [
                    raw.get("revenueGrowthTTMYoy"),
                    raw.get("revenueGrowthQuarterlyYoy"),
                ]
            ),
            "dividend_yield": self._first_non_null(
                [
                    record.dividend_yield,
                    raw.get("dividendYieldIndicatedAnnual"),
                    raw.get("currentDividendYieldTTM"),
                ]
            ),
            "payout_ratio": self._first_non_null(
                [
                    raw.get("payoutRatioTTM"),
                    raw.get("payoutRatioAnnual"),
                ]
            ),
        }

    def _normalize_metrics(
        self, metrics: Dict[str, Optional[float]], benchmarks: Dict[str, dict]
    ) -> Dict[str, dict]:
        """Normalize metrics using provided benchmarks (default or peer-adjusted)."""
        normalized = {}
        for name, value in metrics.items():
            bench = benchmarks.get(name)
            if not bench:
                continue
            if value is None:
                normalized[name] = {
                    "raw_value": None,
                    "normalized_score": None,
                    "direction": (
                        "lower_better" if bench["lower_better"] else "higher_better"
                    ),
                    "benchmark_range": list(bench["range"]),
                    "percentile_vs_peer": None,
                    "status": "DATA_GAP",
                }
                continue
            min_b, max_b = bench["range"]
            if bench["lower_better"]:
                score = ((max_b - value) / (max_b - min_b)) * 100
            else:
                score = ((value - min_b) / (max_b - min_b)) * 100
            score = max(0, min(100, score))
            normalized[name] = {
                "raw_value": float(value),
                "normalized_score": float(score),
                "direction": (
                    "lower_better" if bench["lower_better"] else "higher_better"
                ),
                "benchmark_range": list(bench["range"]),
                "percentile_vs_peer": None,
                "status": "ok",
            }
        return normalized

    def _detect_anomalies(self, metrics: Dict[str, Optional[float]]) -> List[Dict]:
        """Detect anomalies and edge cases."""
        anomalies = []

        def add(metric, severity, message):
            anomalies.append(
                {
                    "metric": metric,
                    "severity": severity,
                    "raw_value": metrics.get(metric),
                    "message": message,
                    "action": "review",
                }
            )

        if (metrics.get("pb_ratio") or 0) > 40:
            add("pb_ratio", "CRITICAL", "Stock trading at extreme premium (P/B > 40)")
        if (
            metrics.get("current_ratio") is not None
            and metrics.get("current_ratio") < 1.0
        ):
            add(
                "current_ratio",
                "MEDIUM",
                "Liquidity squeeze risk (current ratio < 1.0)",
            )
        if (metrics.get("debt_to_equity") or 0) > 1.5:
            add("debt_to_equity", "MEDIUM", "High leverage; monitor earnings stability")
        if (metrics.get("roe") or 0) > 150 and (
            metrics.get("debt_to_equity") or 0
        ) > 1.2:
            add("roe", "LOW", "High ROE driven by leverage; assess sustainability")
        if (metrics.get("payout_ratio") or 0) > 100:
            add(
                "payout_ratio",
                "CRITICAL",
                "Dividend likely unsustainable (payout > 100%)",
            )
        return anomalies

    def _composite_scores(self, normalized: Dict[str, dict]) -> Dict[str, dict]:
        """Compute weighted composite scores per investment style."""
        scores = {}
        for style, weights in self.STYLE_WEIGHTS.items():
            total_weight = 0.0
            weighted_sum = 0.0
            breakdown = {}
            for metric, weight in weights.items():
                score = normalized.get(metric, {}).get("normalized_score")
                if score is None:
                    continue
                contribution = score * weight
                weighted_sum += contribution
                total_weight += weight
                breakdown[metric] = {
                    "score": score,
                    "weight": weight,
                    "contribution": contribution,
                }
            overall = weighted_sum / total_weight if total_weight else None
            scores[style] = {
                "overall_score": round(overall, 2) if overall is not None else None,
                "weighted_breakdown": breakdown,
            }
        return scores

    def _detect_style_mismatch(
        self, composite_scores: Dict[str, dict], selected_style: str
    ) -> Optional[Dict]:
        """
        Detect if stock fundamentals better match a different investment style.
        Returns dict with mismatch info, or None if selected style is appropriate.
        """
        if not composite_scores:
            return None

        scores_sorted = sorted(
            [
                (style, composite_scores[style]["overall_score"])
                for style in composite_scores
                if composite_scores[style]["overall_score"] is not None
            ],
            key=lambda x: x[1],
            reverse=True,
        )

        if not scores_sorted:
            return None

        best_style, best_score = scores_sorted[0]
        selected_score = composite_scores.get(selected_style, {}).get("overall_score")

        # Flag mismatch if selected style is significantly worse than best
        if selected_score is not None and (best_score - selected_score) > 15:
            return {
                "severity": "CRITICAL",
                "selected_style": selected_style,
                "selected_score": selected_score,
                "recommended_style": best_style,
                "recommended_score": best_score,
                "delta": round(best_score - selected_score, 2),
                "message": (
                    f"Stock is better suited for {best_style.upper()} investing "
                    f"(score {best_score:.1f}/100) vs. selected {selected_style.upper()} "
                    f"(score {selected_score:.1f}/100). Consider reassessing lens."
                ),
                "action": "Consider primary lens switch",
            }

        return None

    def _confidence_score(
        self,
        metrics: Dict[str, Optional[float]],
        anomalies: List[Dict],
        investment_style: str,
        composite_scores: Dict[str, dict],
    ) -> float:
        """
        Compute confidence score with enhanced calibration.
        Base 1.0, reduced by data gaps, anomalies, and risk factors.
        """
        confidence = 1.0

        # Penalty for missing data
        missing = sum(1 for v in metrics.values() if v is None)
        confidence -= 0.1 * missing

        # Penalty for anomalies
        critical_count = sum(1 for a in anomalies if a["severity"] == "CRITICAL")
        medium_count = sum(1 for a in anomalies if a["severity"] == "MEDIUM")

        confidence -= critical_count * 0.15
        confidence -= medium_count * 0.08

        # Penalty if negative margin (data quality issue)
        if (metrics.get("net_margin") or 0) < 0:
            confidence -= 0.15

        # Cap confidence based on risk rating
        # (Computed separately, so we use logic from anomalies)
        risk_rating = self._risk_rating(anomalies, confidence)
        if risk_rating == "HIGH":
            confidence = min(confidence, 0.70)
        elif risk_rating == "MEDIUM":
            confidence = min(confidence, 0.80)

        # Ensure floor
        confidence = max(0.3, min(1.0, confidence))
        return round(confidence, 2)

    def _risk_rating(self, anomalies: List[Dict], confidence: float = None) -> str:
        """Determine risk rating from anomalies."""
        severities = {a["severity"] for a in anomalies}
        if "CRITICAL" in severities:
            return "HIGH"
        if "MEDIUM" in severities:
            return "MEDIUM"
        return "LOW"

    def _build_narrative(
        self,
        metrics: Dict[str, Optional[float]],
        normalized: Dict[str, dict],
        composite: Dict[str, dict],
        investment_style: str,
        risk_rating: str,
        confidence: float,
        peer_stats: Optional[Dict[str, dict]] = None,
        style_mismatch: Optional[Dict] = None,
    ) -> Dict:
        """
        Build a signal-driven narrative with four opinionated sections:
          signal           — archetype label + vibe (e.g. "The Expensive Winner")
          core_strength    — single best reason to own it (the Moat)
          critical_warning — single biggest risk to the trade (the Red Flag)
          actionable_context — how to approach the current price
        """

        def _score(metric: str) -> float:
            """Return normalized_score for a metric, defaulting to 0."""
            return normalized.get(metric, {}).get("normalized_score") or 0.0

        roe_s    = _score("roe")
        pe_s     = _score("pe_ratio")          # high = cheap (lower_better)
        debt_s   = _score("debt_to_equity")    # high = low debt (lower_better)
        margin_s = _score("net_margin")
        cr_s     = _score("current_ratio")
        yield_s  = _score("dividend_yield")
        growth_s = max(_score("eps_growth_yoy"), _score("revenue_growth_yoy"))

        # ----------------------------------------------------------------
        # 1. SIGNAL — classify into an archetype
        # ----------------------------------------------------------------
        # Priority order matters: more specific / negative signals before broader positives.
        ARCHETYPES = [
            # (condition_fn,                                            label,                    vibe,                       description)
            (lambda: roe_s >= 70 and pe_s < 50,                       "The Expensive Winner",    "Quality at a Premium",     "A high-quality business trading at a premium — the market is paying up for proven returns."),
            (lambda: debt_s < 40 and yield_s >= 70,                   "The Dividend Trap?",      "Income with Hidden Risk",  "High yield is enticing, but leverage could pressure the payout in a downturn."),
            (lambda: growth_s >= 70 and margin_s < 40,                "The Land Grabber",        "Growth at All Costs",      "Burning margin to capture market share — bet on scale before profitability."),
            (lambda: roe_s >= 70 and debt_s >= 70 and margin_s >= 70, "The Quality Compounder",  "Elite Business Model",     "Rare combination of high returns, wide margins, and a clean balance sheet."),
            (lambda: pe_s >= 70 and margin_s < 40 and debt_s < 40,   "The Value Trap",          "Cheap for a Reason",       "Low valuation reflects real fundamental weakness — cheap does not mean safe."),
            (lambda: growth_s >= 70 and roe_s >= 70,                  "The Momentum Machine",    "Growth + Quality",         "Accelerating growth backed by strong returns — momentum and fundamentals are aligned."),
            (lambda: pe_s >= 70 and cr_s >= 70,                       "The Safe Bargain",        "Value + Stability",        "Attractively priced with a solid balance sheet — downside is limited, upside is patient."),
            (lambda: yield_s >= 70 and debt_s >= 70,                  "The Steady Income Play",  "Safe Dividend",            "Reliable yield supported by a conservative balance sheet — income with low drama."),
        ]

        signal_label = "The Mixed Bag"
        signal_vibe  = "Balanced Profile"
        signal_desc  = "No dominant pattern — the stock shows a mix of strengths and weaknesses across metrics."

        for condition, label, vibe, desc in ARCHETYPES:
            try:
                if condition():
                    signal_label, signal_vibe, signal_desc = label, vibe, desc
                    break
            except Exception:
                continue

        signal = {"label": signal_label, "vibe": signal_vibe, "description": signal_desc}

        # ----------------------------------------------------------------
        # 2. CORE STRENGTH — highest normalized score → Moat phrase
        # ----------------------------------------------------------------
        MOAT_MAP = {
            "roe":               "Exceptional return on equity signals a durable competitive advantage",
            "net_margin":        "Industry-leading margins indicate strong pricing power",
            "eps_growth_yoy":    "Accelerating earnings growth positions this as a market-share winner",
            "revenue_growth_yoy":"Accelerating revenue growth positions this as a market-share winner",
            "debt_to_equity":    "A clean balance sheet provides resilience and strategic optionality",
            "dividend_yield":    "Generous dividend income backed by solid payout coverage",
            "current_ratio":     "Robust liquidity buffer shields against near-term headwinds",
            "pb_ratio":          "Attractive book value suggests the market is underpricing tangible assets",
            "pe_ratio":          "Compelling valuation offers a margin of safety for new investors",
            "payout_ratio":      "Conservative payout ratio leaves room for dividend growth",
        }
        scored_metrics = [
            (m, _score(m))
            for m in MOAT_MAP
            if normalized.get(m, {}).get("status") != "DATA_GAP"
        ]
        top_metric = max(scored_metrics, key=lambda x: x[1], default=("roe", 0))[0]
        core_strength = MOAT_MAP.get(top_metric, "Strong fundamental profile across key metrics")

        if peer_stats and top_metric in peer_stats:
            pct = peer_stats[top_metric].get("your_percentile")
            if pct is not None and pct >= 70:
                core_strength += f" — top {100 - int(pct)}% of sector peers"

        # ----------------------------------------------------------------
        # 3. CRITICAL WARNING — worst metric → Red Flag phrase
        # ----------------------------------------------------------------
        WARNING_MAP = {
            "debt_to_equity":    "Elevated leverage amplifies downside in any rate or revenue shock",
            "current_ratio":     "Tight working capital increases execution risk — watch cash conversion",
            "net_margin":        "Thin or negative margins leave no buffer for cost shocks",
            "pe_ratio":          "Premium valuation leaves little margin of safety if growth disappoints",
            "roe":               "Weak returns on capital suggest the business lacks a durable competitive moat",
            "eps_growth_yoy":    "Decelerating earnings growth may disappoint momentum investors",
            "revenue_growth_yoy":"Slowing revenue growth signals the growth story may be maturing",
            "pb_ratio":          "Elevated price-to-book reduces the margin of safety on tangible assets",
            "dividend_yield":    "Low or no dividend limits income appeal for yield-seeking investors",
            "payout_ratio":      "Stretched payout ratio leaves little room for dividend growth or reinvestment",
        }
        # Hard-coded CRITICAL overrides first
        pb_raw  = metrics.get("pb_ratio")
        payout  = metrics.get("payout_ratio")
        if pb_raw is not None and pb_raw > 40:
            critical_warning = "Extreme valuation multiple (P/B > 40) — any growth miss will be severely punished"
        elif payout is not None and payout > 100:
            critical_warning = "Dividend is being funded from reserves, not earnings — a dividend cut is a real risk"
        else:
            worst_metric = min(scored_metrics, key=lambda x: x[1], default=("pe_ratio", 0))[0]
            critical_warning = WARNING_MAP.get(worst_metric, "Monitor all metrics closely; no single dominant risk identified")
            # Tag peer outlier if applicable
            if peer_stats and worst_metric in peer_stats:
                pct = peer_stats[worst_metric].get("your_percentile")
                if pct is not None and pct < 20:
                    critical_warning = f"Peer outlier — {critical_warning}"

        # ----------------------------------------------------------------
        # 4. ACTIONABLE CONTEXT — composite score + valuation
        # ----------------------------------------------------------------
        style_score = composite.get(investment_style, {}).get("overall_score") or 0.0

        if style_score >= 70 and pe_s >= 60:
            actionable_context = "Fundamentals and valuation align — current price offers a compelling entry point"
        elif style_score >= 70:
            actionable_context = "Strong business but premium valuation — wait for a pullback or size the position gradually"
        elif style_score >= 50:
            actionable_context = "Quality is emerging but unproven — watch one more earnings cycle before committing capital"
        else:
            actionable_context = "Risk/reward is unfavorable at current levels — avoid or reduce exposure"

        if style_mismatch:
            rec = style_mismatch.get("recommended_style", "").upper()
            actionable_context += f" Note: this stock scores better as a {rec} play."

        # ----------------------------------------------------------------
        # 5. SUMMARY — combined one-paragraph text for simple display
        # ----------------------------------------------------------------
        summary = (
            f"{signal_label}: {signal_desc} "
            f"{core_strength}. "
            f"Key risk: {critical_warning}. "
            f"{actionable_context}. "
            f"[Risk: {risk_rating} | Confidence: {confidence:.0%}]"
        )

        return {
            "signal":             signal,
            "core_strength":      core_strength,
            "critical_warning":   critical_warning,
            "actionable_context": actionable_context,
            "risk_rating":        risk_rating,
            "confidence":         confidence,
            "summary":            summary,
        }

    @staticmethod
    def _count_peer_outliers(peer_stats: Dict[str, dict]) -> int:
        """Count how many metrics are flagged as peer outliers."""
        count = 0
        for metric, stats in peer_stats.items():
            rank = stats.get("your_rank", "")
            if "outlier" in str(rank).lower():
                count += 1
        return count

    @staticmethod
    def _first_non_null(values):
        """Extract first non-null value from list, coerce to float."""
        for v in values:
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    continue
        return None
