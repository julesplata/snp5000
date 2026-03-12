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
        peer_values = [p["metrics"].get(metric_name) for p in self.peers if p["metrics"].get(metric_name) is not None]
        
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
                "note": "Insufficient peer data"
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

    def recalibrate_benchmarks(self, metrics_to_analyze: List[str], sigma_range: float = 1.0) -> Dict[str, Tuple[float, float]]:
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
            benchmarks.update({
                m: {"range": dynamic_benches[m], "lower_better": self.BENCHMARKS[m]["lower_better"]}
                for m in metric_names if m in dynamic_benches
            })
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
        confidence = self._confidence_score(metrics, anomalies, investment_style, composite_scores)
        
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
                [record.profit_margin, raw.get("netProfitMarginTTM"), raw.get("netMargin")] 
            ),
            "roe": self._first_non_null([raw.get("roeTTM"), raw.get("roeRfy"), raw.get("roe5Y")]),
            "current_ratio": self._first_non_null([
                raw.get("currentRatioQuarterly"),
                raw.get("currentRatioAnnual"),
            ]),
            "eps_growth_yoy": self._first_non_null([
                raw.get("epsGrowthTTMYoy"),
                raw.get("epsGrowthQuarterlyYoy"),
            ]),
            "revenue_growth_yoy": self._first_non_null([
                raw.get("revenueGrowthTTMYoy"),
                raw.get("revenueGrowthQuarterlyYoy"),
            ]),
            "dividend_yield": self._first_non_null([
                record.dividend_yield,
                raw.get("dividendYieldIndicatedAnnual"),
                raw.get("currentDividendYieldTTM"),
            ]),
            "payout_ratio": self._first_non_null([
                raw.get("payoutRatioTTM"),
                raw.get("payoutRatioAnnual"),
            ]),
        }

    def _normalize_metrics(
        self,
        metrics: Dict[str, Optional[float]],
        benchmarks: Dict[str, dict]
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
                    "direction": "lower_better" if bench["lower_better"] else "higher_better",
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
                "direction": "lower_better" if bench["lower_better"] else "higher_better",
                "benchmark_range": list(bench["range"]),
                "percentile_vs_peer": None,
                "status": "ok",
            }
        return normalized

    def _detect_anomalies(self, metrics: Dict[str, Optional[float]]) -> List[Dict]:
        """Detect anomalies and edge cases."""
        anomalies = []
        def add(metric, severity, message):
            anomalies.append({
                "metric": metric,
                "severity": severity,
                "raw_value": metrics.get(metric),
                "message": message,
                "action": "review",
            })

        if (metrics.get("pb_ratio") or 0) > 40:
            add("pb_ratio", "CRITICAL", "Stock trading at extreme premium (P/B > 40)")
        if metrics.get("current_ratio") is not None and metrics.get("current_ratio") < 1.0:
            add("current_ratio", "MEDIUM", "Liquidity squeeze risk (current ratio < 1.0)")
        if (metrics.get("debt_to_equity") or 0) > 1.5:
            add("debt_to_equity", "MEDIUM", "High leverage; monitor earnings stability")
        if (metrics.get("roe") or 0) > 150 and (metrics.get("debt_to_equity") or 0) > 1.2:
            add("roe", "LOW", "High ROE driven by leverage; assess sustainability")
        if (metrics.get("payout_ratio") or 0) > 100:
            add("payout_ratio", "CRITICAL", "Dividend likely unsustainable (payout > 100%)")
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
                breakdown[metric] = {"score": score, "weight": weight, "contribution": contribution}
            overall = weighted_sum / total_weight if total_weight else None
            scores[style] = {
                "overall_score": round(overall, 2) if overall is not None else None,
                "weighted_breakdown": breakdown,
            }
        return scores

    def _detect_style_mismatch(self, composite_scores: Dict[str, dict], selected_style: str) -> Optional[Dict]:
        """
        Detect if stock fundamentals better match a different investment style.
        Returns dict with mismatch info, or None if selected style is appropriate.
        """
        if not composite_scores:
            return None
        
        scores_sorted = sorted(
            [(style, composite_scores[style]["overall_score"]) 
             for style in composite_scores if composite_scores[style]["overall_score"] is not None],
            key=lambda x: x[1],
            reverse=True
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
        Build rich narrative with peer context and style insights.
        """
        # Strengths and weaknesses
        strengths = [
            f"Strong {m.replace('_',' ')} ({normalized[m]['raw_value']:.2f}) scoring {normalized[m]['normalized_score']:.0f}/100"
            for m in normalized
            if normalized[m].get("normalized_score") is not None and normalized[m]["normalized_score"] >= 70
        ]
        weaknesses = [
            f"Weak {m.replace('_',' ')} ({normalized[m]['raw_value']:.2f}) scoring {normalized[m]['normalized_score']:.0f}/100"
            for m in normalized
            if normalized[m].get("normalized_score") is not None and normalized[m]["normalized_score"] <= 40
        ]
        balance = "Strengths currently outweigh weaknesses" if len(strengths) >= len(weaknesses) else "Weaknesses dominate; monitor closely"

        style_score = composite.get(investment_style, {}).get("overall_score")
        
        # Base verdict
        verdict = (
            f"For {investment_style.upper()} investors, composite score is {style_score or 'N/A'}/100. "
        )
        
        if style_mismatch:
            verdict += (
                f"⚠️  STYLE MISMATCH: This stock is better suited for {style_mismatch['recommended_style'].upper()} "
                f"investing (score {style_mismatch['recommended_score']:.1f}/100). Consider reassessing investment lens."
            )
        else:
            verdict += "Proceed with awareness of flagged risks and valuation context."

        # Build summary with peer context
        summary_chunks = []
        
        if strengths:
            summary_chunks.append("Strengths: " + "; ".join(strengths[:3]) + ".")
        if weaknesses:
            summary_chunks.append("Weaknesses: " + "; ".join(weaknesses[:3]) + ".")
        
        # Leverage narrative with peer context
        if metrics.get("debt_to_equity") is not None:
            de_msg = f"Leverage at D/E {metrics['debt_to_equity']:.2f}"
            if peer_stats and "debt_to_equity" in peer_stats:
                peer_mean = peer_stats["debt_to_equity"].get("peer_mean")
                if peer_mean:
                    delta = ((metrics['debt_to_equity'] - peer_mean) / peer_mean) * 100
                    your_rank = peer_stats["debt_to_equity"].get("your_rank")
                    de_msg += f" ({delta:+.0f}% vs. peer median {peer_mean:.2f}); ranked {your_rank}"
                else:
                    de_msg += "; within comfort for most investors"
            else:
                de_msg += "; " + ("within comfort for most investors" if metrics["debt_to_equity"] <= 1.5 else "above preferred range—monitor coverage ratios.")
            summary_chunks.append(de_msg + ".")
        
        # Liquidity narrative with peer context
        if metrics.get("current_ratio") is not None:
            cr_msg = f"Liquidity at current ratio {metrics['current_ratio']:.2f}"
            if peer_stats and "current_ratio" in peer_stats:
                your_rank = peer_stats["current_ratio"].get("your_rank")
                cr_msg += f" (ranked {your_rank})"
            cr_msg += " suggests " + ("a modest cushion" if metrics["current_ratio"] >= 1.2 else "tight working capital—watch cash conversion.")
            summary_chunks.append(cr_msg + ".")
        
        # Profitability with peer context
        if metrics.get("net_margin") is not None:
            nm_msg = f"Net margin {metrics['net_margin']:.2f}%"
            if peer_stats and "net_margin" in peer_stats:
                your_rank = peer_stats["net_margin"].get("your_rank")
                nm_msg += f" (ranked {your_rank})"
            nm_msg += " indicates strong profitability"
            summary_chunks.append(nm_msg + ".")
        
        # Growth context
        if metrics.get("eps_growth_yoy") is not None:
            eps_msg = f"EPS growth {metrics['eps_growth_yoy']:.2f}% YoY"
            if metrics.get("revenue_growth_yoy") is not None:
                eps_msg += f" vs. revenue growth {metrics['revenue_growth_yoy']:.2f}% YoY"
                if metrics['eps_growth_yoy'] > metrics['revenue_growth_yoy'] * 1.5:
                    eps_msg += " (margin expansion story)"
                elif metrics['eps_growth_yoy'] < metrics['revenue_growth_yoy']:
                    eps_msg += " (margin pressure)"
            summary_chunks.append(eps_msg + ".")
        
        # Analysis caveats
        if peer_stats:
            summary_chunks.append(f"Peer-adjusted analysis; {self._count_peer_outliers(peer_stats)} metric(s) flagged as peer outlier(s).")
        else:
            summary_chunks.append("Standalone analysis using fixed benchmarks; peer data not available.")
        
        summary_chunks.append(
            f"Risk: {risk_rating}; Confidence: {confidence:.2f}."
        )
        
        summary = " ".join(summary_chunks)

        return {
            "strengths": strengths[:3],
            "weaknesses": weaknesses[:3],
            "balance": balance,
            "verdict": verdict,
            "risk_rating": risk_rating,
            "confidence": confidence,
            "summary": summary,
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
