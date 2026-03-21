import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Decile threshold constants
# ---------------------------------------------------------------------------

# Lower-is-better: score 10 if value <= cutoffs[0], down to 1 if > cutoffs[-1]
PE_CUTOFFS = [15, 20, 25, 30, 35, 40, 45, 50, 60]
PB_CUTOFFS = [1, 2, 3, 5, 10, 20, 30, 50]
PEG_CUTOFFS = [0.5, 1.0, 1.5, 2.0, 3.0]
DE_CUTOFFS = [0.1, 0.5, 1.0, 2.0, 3.0]
EVFCF_CUTOFFS = [10, 20, 30, 40, 60]

# Higher-is-better: score 1 if value < cutoffs[0], up to 10 if >= cutoffs[-1]
MARGIN_CUTOFFS = [10, 15, 20, 25, 30, 40]
ROE_CUTOFFS = [5, 10, 15, 20, 25, 30, 40, 50]
GROWTH_CUTOFFS = [0, 5, 10, 15, 20, 30]
CR_CUTOFFS = [0.8, 1.0, 1.2, 1.5, 2.0]
FCF_MARGIN_CUTOFFS = [5, 10, 15, 20, 25, 30]
ASSET_TURN_CUTOFFS = [0.5, 1.0, 1.5, 2.0]
INV_TURN_CUTOFFS = [2, 4, 6, 8, 12, 20]


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------


@dataclass
class StockContext:
    market_cap: Optional[float]
    current_price: Optional[float]


@dataclass
class PillarResult:
    valuation_score: Optional[float]
    profitability_score: Optional[float]
    growth_score: Optional[float]
    health_score: Optional[float]
    cashflow_score: Optional[float]
    efficiency_score: Optional[float]
    overall_fundamental_rating: Optional[float]


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no class state needed)
# ---------------------------------------------------------------------------


def _decile_score(value: float, cutoffs: list, lower_better: bool) -> float:
    """Return a 1–10 score for *value* relative to *cutoffs*.

    lower_better=True  → score 10 when value <= cutoffs[0], 1 when > cutoffs[-1]
    lower_better=False → score 1 when value < cutoffs[0],  10 when >= cutoffs[-1]
    """
    n = len(cutoffs)
    if lower_better:
        for i, threshold in enumerate(cutoffs):
            if value <= threshold:
                return float(10 - i)
        return 1.0
    else:
        for i, threshold in enumerate(reversed(cutoffs)):
            if value >= threshold:
                return float(10 - i)
        return 1.0


def _weighted_avg(components: dict) -> Optional[float]:
    """Weighted average over available (non-None) components.

    components: {name: (score_or_None, weight)}
    Weights are redistributed proportionally among available components.
    Returns None if all scores are None.
    """
    total_w = sum(w for s, w in components.values() if s is not None)
    if total_w == 0:
        return None
    return sum(s * w for s, w in components.values() if s is not None) / total_w


def _first(raw: dict, keys: list) -> Optional[float]:
    """Return the first non-None float value found in *raw* for any of *keys*."""
    for key in keys:
        if key in raw and raw[key] is not None:
            try:
                return float(raw[key])
            except (TypeError, ValueError):
                continue
    return None


# ---------------------------------------------------------------------------
# Main calculator
# ---------------------------------------------------------------------------


class PillarRatingCalculator:
    """Compute 1–10 pillar scores and an overall rating from raw Finnhub metrics.

    Scores are computed independently of the existing FundamentalAnalysisEngine.
    Any pillar whose metrics are entirely unavailable returns None for that pillar.
    The overall rating is the arithmetic mean of all non-None pillar scores.
    """

    def compute(self, raw_metrics: dict, stock_ctx: StockContext) -> PillarResult:
        v = self._valuation(raw_metrics)
        p = self._profitability(raw_metrics)
        g = self._growth(raw_metrics)
        h = self._health(raw_metrics)
        c = self._cashflow(raw_metrics)
        e = self._efficiency(raw_metrics)
        overall = self._overall(v, p, g, h, c, e)
        return PillarResult(
            valuation_score=v,
            profitability_score=p,
            growth_score=g,
            health_score=h,
            cashflow_score=c,
            efficiency_score=e,
            overall_fundamental_rating=overall,
        )

    # ------------------------------------------------------------------
    # Pillar methods
    # ------------------------------------------------------------------

    def _valuation(self, raw: dict) -> Optional[float]:
        pe = _first(raw, ["peBasicExclExtraTTM", "peTTM"])
        pb = _first(raw, ["pbAnnual", "pbQuarterly"])
        eps_growth = _first(raw, ["epsGrowthTTMYoy", "epsGrowthQuarterlyYoy"])

        score_pe = (
            _decile_score(pe, PE_CUTOFFS, lower_better=True)
            if pe is not None and pe > 0
            else None
        )
        score_pb = (
            _decile_score(pb, PB_CUTOFFS, lower_better=True)
            if pb is not None and pb > 0
            else None
        )

        # PEG = PE / (EPS growth rate expressed as %). Finnhub delivers growth
        # as a decimal (0.15 == 15%), so multiply by 100 before dividing.
        peg = None
        if pe is not None and pe > 0 and eps_growth is not None and eps_growth > 0:
            peg = pe / (eps_growth * 100)
        score_peg = (
            _decile_score(peg, PEG_CUTOFFS, lower_better=True)
            if peg is not None
            else None
        )

        return _weighted_avg(
            {"pe": (score_pe, 0.4), "pb": (score_pb, 0.3), "peg": (score_peg, 0.3)}
        )

    def _profitability(self, raw: dict) -> Optional[float]:
        # Finnhub returns margins as decimals; convert to % for cutoff comparison
        margin = _first(raw, ["netProfitMarginTTM", "netProfitMarginAnnual"])
        roe = _first(raw, ["roeTTM", "roeRfy"])

        margin_pct = margin * 100 if margin is not None else None
        roe_pct = roe * 100 if roe is not None else None

        score_m = (
            _decile_score(margin_pct, MARGIN_CUTOFFS, lower_better=False)
            if margin_pct is not None
            else None
        )
        score_r = (
            _decile_score(roe_pct, ROE_CUTOFFS, lower_better=False)
            if roe_pct is not None
            else None
        )

        return _weighted_avg({"margin": (score_m, 0.6), "roe": (score_r, 0.4)})

    def _growth(self, raw: dict) -> Optional[float]:
        rev_growth = _first(raw, ["revenueGrowthTTMYoy"])
        eps_growth = _first(raw, ["epsGrowthTTMYoy", "epsGrowthQuarterlyYoy"])

        rev_pct = rev_growth * 100 if rev_growth is not None else None
        eps_pct = eps_growth * 100 if eps_growth is not None else None

        score_r = (
            _decile_score(rev_pct, GROWTH_CUTOFFS, lower_better=False)
            if rev_pct is not None
            else None
        )
        score_e = (
            _decile_score(eps_pct, GROWTH_CUTOFFS, lower_better=False)
            if eps_pct is not None
            else None
        )

        return _weighted_avg({"rev": (score_r, 0.5), "eps": (score_e, 0.5)})

    def _health(self, raw: dict) -> Optional[float]:
        de = _first(
            raw,
            [
                "totalDebt/totalEquityAnnual",
                "totalDebt/totalEquityQuarterly",
                "totalDebtToEquityAnnual",
                "totalDebtToEquityQuarterly",
            ],
        )
        cr = _first(raw, ["currentRatioAnnual", "currentRatioQuarterly"])

        score_de = (
            _decile_score(de, DE_CUTOFFS, lower_better=True)
            if de is not None and de >= 0
            else None
        )
        score_cr = (
            _decile_score(cr, CR_CUTOFFS, lower_better=False)
            if cr is not None
            else None
        )

        return _weighted_avg({"de": (score_de, 0.5), "cr": (score_cr, 0.5)})

    def _cashflow(self, raw: dict) -> Optional[float]:
        ev_fcf = _first(raw, ["evToFreeCashFlowTTM", "priceToFreeCashFlowTTM"])
        fcf_margin = _first(raw, ["freeCashFlowMarginTTM"])

        # Require at least EV/FCF to score this pillar
        if ev_fcf is None:
            return None

        score_ef = (
            _decile_score(ev_fcf, EVFCF_CUTOFFS, lower_better=True)
            if ev_fcf > 0
            else None
        )
        fcf_margin_pct = fcf_margin * 100 if fcf_margin is not None else None
        score_fm = (
            _decile_score(fcf_margin_pct, FCF_MARGIN_CUTOFFS, lower_better=False)
            if fcf_margin_pct is not None
            else None
        )

        return _weighted_avg({"evfcf": (score_ef, 0.7), "fcfm": (score_fm, 0.3)})

    def _efficiency(self, raw: dict) -> Optional[float]:
        asset_turn = _first(raw, ["assetTurnoverAnnual", "assetTurnoverTTM"])
        inv_turn = _first(raw, ["inventoryTurnoverAnnual", "inventoryTurnoverTTM"])

        if asset_turn is None and inv_turn is None:
            return None

        score_at = (
            _decile_score(asset_turn, ASSET_TURN_CUTOFFS, lower_better=False)
            if asset_turn is not None
            else None
        )
        score_it = (
            _decile_score(inv_turn, INV_TURN_CUTOFFS, lower_better=False)
            if inv_turn is not None
            else None
        )

        return _weighted_avg({"at": (score_at, 0.5), "it": (score_it, 0.5)})

    # ------------------------------------------------------------------
    # Overall aggregation
    # ------------------------------------------------------------------

    @staticmethod
    def _overall(*pillar_scores) -> Optional[float]:
        available = [s for s in pillar_scores if s is not None]
        if not available:
            return None
        return round(sum(available) / len(available), 4)


# ---------------------------------------------------------------------------
# Validation: sanity checks + sensitivity analysis
# ---------------------------------------------------------------------------

_PILLAR_NAMES = [
    "valuation",
    "profitability",
    "growth",
    "health",
    "cashflow",
    "efficiency",
]


def _result_to_dict(result: PillarResult) -> dict:
    return {
        "valuation": result.valuation_score,
        "profitability": result.profitability_score,
        "growth": result.growth_score,
        "health": result.health_score,
        "cashflow": result.cashflow_score,
        "efficiency": result.efficiency_score,
    }


class PillarValidator:
    """Server-side validation: sanity checks, sensitivity analysis.

    Peer benchmarking is a placeholder pending sector peer data availability.
    Call run_all() after PillarRatingCalculator.compute() — results are emitted
    as structured log lines only (no API impact).
    """

    EXTREME_HIGH = 9.5
    EXTREME_LOW = 1.5
    MIN_PILLARS_FOR_OVERALL = 4
    SENSITIVITY_SHIFT = 0.20  # ±20 %
    SENSITIVITY_LOG_THRESHOLD = 0.05  # only log deltas > 0.05

    def run_all(
        self,
        result: PillarResult,
        raw_metrics: dict,
        stock_id: int,
        sector_pillar_scores: "list[dict] | None" = None,
    ) -> None:
        self._sanity_check(result, raw_metrics, stock_id)
        self._sensitivity_analysis(result, stock_id)
        if sector_pillar_scores:
            self._peer_benchmark(result, stock_id, sector_pillar_scores)

    # ------------------------------------------------------------------
    # Sanity checks
    # ------------------------------------------------------------------

    def _sanity_check(
        self, result: PillarResult, raw_metrics: dict, stock_id: int
    ) -> None:
        scores = _result_to_dict(result)
        available = {k: v for k, v in scores.items() if v is not None}
        n = len(available)

        # 1. Too few pillars contributing to overall
        if n < self.MIN_PILLARS_FOR_OVERALL:
            logger.warning(
                "pillar_sanity stock_id=%d: overall built from only %d/%d pillars "
                "(cashflow/efficiency metrics likely absent from Finnhub response)",
                stock_id,
                n,
                len(_PILLAR_NAMES),
            )

        # 2. Extreme scores
        for name, score in available.items():
            if score >= self.EXTREME_HIGH:
                logger.warning(
                    "pillar_sanity stock_id=%d: %s=%.2f is near maximum (>=%.1f) — verify source data",
                    stock_id,
                    name,
                    score,
                    self.EXTREME_HIGH,
                )
            elif score <= self.EXTREME_LOW:
                logger.warning(
                    "pillar_sanity stock_id=%d: %s=%.2f is near minimum (<=%.1f) — "
                    "stock may be distressed or data anomaly",
                    stock_id,
                    name,
                    score,
                    self.EXTREME_LOW,
                )

        # 3. Single-metric dominance per pillar
        self._check_valuation_dominance(result, raw_metrics, stock_id)
        self._check_profitability_dominance(result, raw_metrics, stock_id)
        self._check_growth_dominance(result, raw_metrics, stock_id)
        self._check_health_dominance(result, raw_metrics, stock_id)

    def _check_valuation_dominance(
        self, result: PillarResult, raw: dict, stock_id: int
    ) -> None:
        if result.valuation_score is None:
            return
        pe_val = _first(raw, ["peBasicExclExtraTTM", "peTTM"])
        eps_g = _first(raw, ["epsGrowthTTMYoy", "epsGrowthQuarterlyYoy"])
        pe_ok = pe_val is not None and pe_val > 0
        pb_ok = _first(raw, ["pbAnnual", "pbQuarterly"]) is not None
        peg_ok = pe_ok and eps_g is not None and eps_g > 0
        n = sum([pe_ok, pb_ok, peg_ok])
        if n == 1:
            sole = "PE" if pe_ok else ("PB" if pb_ok else "PEG")
            logger.warning(
                "pillar_sanity stock_id=%d: valuation_score driven solely by %s "
                "(other valuation metrics unavailable)",
                stock_id,
                sole,
            )

    def _check_profitability_dominance(
        self, result: PillarResult, raw: dict, stock_id: int
    ) -> None:
        if result.profitability_score is None:
            return
        margin_ok = (
            _first(raw, ["netProfitMarginTTM", "netProfitMarginAnnual"]) is not None
        )
        roe_ok = _first(raw, ["roeTTM", "roeRfy"]) is not None
        if not margin_ok:
            logger.warning(
                "pillar_sanity stock_id=%d: profitability_score driven solely by ROE (margin unavailable)",
                stock_id,
            )
        elif not roe_ok:
            logger.warning(
                "pillar_sanity stock_id=%d: profitability_score driven solely by margin (ROE unavailable)",
                stock_id,
            )

    def _check_growth_dominance(
        self, result: PillarResult, raw: dict, stock_id: int
    ) -> None:
        if result.growth_score is None:
            return
        rev_ok = _first(raw, ["revenueGrowthTTMYoy"]) is not None
        eps_ok = _first(raw, ["epsGrowthTTMYoy", "epsGrowthQuarterlyYoy"]) is not None
        if not rev_ok:
            logger.warning(
                "pillar_sanity stock_id=%d: growth_score driven solely by EPS growth "
                "(revenue growth unavailable)",
                stock_id,
            )
        elif not eps_ok:
            logger.warning(
                "pillar_sanity stock_id=%d: growth_score driven solely by revenue growth "
                "(EPS growth unavailable)",
                stock_id,
            )

    def _check_health_dominance(
        self, result: PillarResult, raw: dict, stock_id: int
    ) -> None:
        if result.health_score is None:
            return
        de = _first(
            raw,
            [
                "totalDebt/totalEquityAnnual",
                "totalDebt/totalEquityQuarterly",
                "totalDebtToEquityAnnual",
                "totalDebtToEquityQuarterly",
            ],
        )
        cr = _first(raw, ["currentRatioAnnual", "currentRatioQuarterly"])
        de_ok = de is not None and de >= 0
        cr_ok = cr is not None
        if not de_ok:
            logger.warning(
                "pillar_sanity stock_id=%d: health_score driven solely by current ratio "
                "(D/E unavailable or negative)",
                stock_id,
            )
        elif not cr_ok:
            logger.warning(
                "pillar_sanity stock_id=%d: health_score driven solely by D/E ratio "
                "(current ratio unavailable)",
                stock_id,
            )

    # ------------------------------------------------------------------
    # Sensitivity analysis — ±20 % pillar weight shifts
    # ------------------------------------------------------------------

    def _sensitivity_analysis(self, result: PillarResult, stock_id: int) -> None:
        """For each available pillar, shift its weight ±20 % in the overall score
        and log the resulting delta so anomalously sensitive pillars are visible."""
        available = {k: v for k, v in _result_to_dict(result).items() if v is not None}
        n = len(available)
        if n < 2 or result.overall_fundamental_rating is None:
            return

        base = result.overall_fundamental_rating
        base_w = 1.0 / n

        for pillar_name in available:
            for direction, factor in [
                ("+20%", 1.0 + self.SENSITIVITY_SHIFT),
                ("-20%", 1.0 - self.SENSITIVITY_SHIFT),
            ]:
                shifted_w = base_w * factor
                # Remaining weight spread equally across the other n-1 pillars
                other_w = (1.0 - shifted_w) / (n - 1) if n > 1 else 0.0

                alt = sum(
                    score * (shifted_w if name == pillar_name else other_w)
                    for name, score in available.items()
                )
                delta = alt - base
                if abs(delta) > self.SENSITIVITY_LOG_THRESHOLD:
                    logger.info(
                        "pillar_sensitivity stock_id=%d: %s weight %s → overall Δ=%.3f "
                        "(%.4f → %.4f)",
                        stock_id,
                        pillar_name,
                        direction,
                        delta,
                        base,
                        alt,
                    )

    # ------------------------------------------------------------------
    # Peer benchmarking — sector percentile ranking
    # ------------------------------------------------------------------

    OUTLIER_HIGH_PCT = 95.0
    OUTLIER_LOW_PCT = 5.0

    def _peer_benchmark(
        self,
        result: PillarResult,
        stock_id: int,
        sector_pillar_scores: "list[dict]",
    ) -> None:
        """Compare each pillar against sector peers and log outlier warnings.

        sector_pillar_scores: list of dicts, each with keys matching _PILLAR_NAMES,
        one entry per peer stock (None values tolerated).
        """
        my_scores = _result_to_dict(result)

        for pillar in _PILLAR_NAMES:
            my_val = my_scores.get(pillar)
            if my_val is None:
                continue

            peer_vals = [
                p[pillar] for p in sector_pillar_scores if p.get(pillar) is not None
            ]
            if len(peer_vals) < 2:
                continue

            below = sum(1 for v in peer_vals if v < my_val)
            percentile = round(below / len(peer_vals) * 100, 1)

            if percentile >= self.OUTLIER_HIGH_PCT:
                logger.warning(
                    "pillar_peer_benchmark stock_id=%d: %s=%.2f is in %.0fth percentile "
                    "of %d sector peers (top outlier) — verify data quality",
                    stock_id,
                    pillar,
                    my_val,
                    percentile,
                    len(peer_vals),
                )
            elif percentile <= self.OUTLIER_LOW_PCT:
                logger.warning(
                    "pillar_peer_benchmark stock_id=%d: %s=%.2f is in %.0fth percentile "
                    "of %d sector peers (bottom outlier) — stock may be distressed",
                    stock_id,
                    pillar,
                    my_val,
                    percentile,
                    len(peer_vals),
                )
            else:
                logger.info(
                    "pillar_peer_benchmark stock_id=%d: %s=%.2f → %.0fth percentile "
                    "(n=%d sector peers)",
                    stock_id,
                    pillar,
                    my_val,
                    percentile,
                    len(peer_vals),
                )
