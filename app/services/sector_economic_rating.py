"""
Sector Economic Rating Service
==============================

Derives a sector-specific economic score from a macro EconomicSnapshot by
re-weighting the macro component scores according to each sector's known
sensitivity profile.

Formula
-------

    sector_economic_score(S) =  Σ  weight(S, c) × component_score(c)
                               c∈C

where:
    C                    = {interest_rates, inflation, growth,
                             employment, yield_curve, sentiment}
    component_score(c)   ∈ [0, 10]   from EconomicSnapshot.components
    weight(S, c)         ∈ [0, 1]    from SECTOR_WEIGHTS table below
    Σ weight(S, c)       = 1.0       (normalised per sector)
      c

If a component is missing from the snapshot the weight is redistributed
proportionally across the remaining available components (same logic used
throughout the rest of the app via _weighted_avg).

Sub-scores stored on the SectorEconomicRating row
--------------------------------------------------
These are the RAW (un-weighted) component scores, stored for transparency
and frontend display:

    gdp_sensitivity_score        = component_score("growth")
    rate_sensitivity_score       = component_score("interest_rates")
    inflation_sensitivity_score  = component_score("inflation")
    employment_sensitivity_score = component_score("employment")

The full weight table + per-component weighted contributions are persisted
in the JSON `components` column so the calculation is fully auditable.

Worked example (Technology sector, typical macro)
-------------------------------------------------
    interest_rates  score=7.0,  weight=0.20  → contribution=1.40
    inflation       score=6.0,  weight=0.15  → contribution=0.90
    growth          score=8.0,  weight=0.35  → contribution=2.80
    employment      score=9.0,  weight=0.10  → contribution=0.90
    yield_curve     score=5.0,  weight=0.10  → contribution=0.50
    sentiment       score=7.0,  weight=0.10  → contribution=0.70
                                             ─────────────────────
    sector_economic_score                  = 7.20 / 10
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

import app.models as models

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sector sensitivity weight tables
# ---------------------------------------------------------------------------
# Component keys match EconomicSnapshot.components:
#   interest_rates, inflation, growth, employment, yield_curve, sentiment
#
# Design rationale per sector:
#   Technology          — growth-driven; rate-sensitive via DCF discounting
#   Financials          — profits from rate spreads; yield-curve is a direct lever
#   Utilities           — bond proxies; most rate-sensitive sector
#   Consumer Discret.   — spending-driven; employment + sentiment dominate
#   Consumer Staples    — inflation pass-through risk; defensive vs. growth
#   Energy              — commodity cycle; inflation + growth as demand proxy
#   Health Care         — defensive; employment (insured coverage) + inflation
#   Industrials         — capex-linked; growth + employment
#   Materials           — commodity inputs; inflation + global growth
#   Real Estate         — rate + inflation (cap-rate compression); bond proxy
#   Comm. Services      — growth + sentiment (ad spend); moderate rate exposure
# ---------------------------------------------------------------------------

SECTOR_WEIGHTS: Dict[str, Dict[str, float]] = {
    "technology": {
        "interest_rates": 0.20,
        "inflation": 0.15,
        "growth": 0.35,
        "employment": 0.10,
        "yield_curve": 0.10,
        "sentiment": 0.10,
    },
    "financials": {
        "interest_rates": 0.30,
        "inflation": 0.10,
        "growth": 0.20,
        "employment": 0.15,
        "yield_curve": 0.20,
        "sentiment": 0.05,
    },
    "utilities": {
        "interest_rates": 0.35,
        "inflation": 0.20,
        "growth": 0.10,
        "employment": 0.05,
        "yield_curve": 0.20,
        "sentiment": 0.10,
    },
    "consumer discretionary": {
        "interest_rates": 0.15,
        "inflation": 0.20,
        "growth": 0.20,
        "employment": 0.20,
        "yield_curve": 0.05,
        "sentiment": 0.20,
    },
    "consumer staples": {
        "interest_rates": 0.20,
        "inflation": 0.30,
        "growth": 0.10,
        "employment": 0.15,
        "yield_curve": 0.10,
        "sentiment": 0.15,
    },
    "energy": {
        "interest_rates": 0.10,
        "inflation": 0.25,
        "growth": 0.25,
        "employment": 0.15,
        "yield_curve": 0.10,
        "sentiment": 0.15,
    },
    "health care": {
        "interest_rates": 0.15,
        "inflation": 0.20,
        "growth": 0.20,
        "employment": 0.20,
        "yield_curve": 0.10,
        "sentiment": 0.15,
    },
    "industrials": {
        "interest_rates": 0.15,
        "inflation": 0.20,
        "growth": 0.30,
        "employment": 0.15,
        "yield_curve": 0.10,
        "sentiment": 0.10,
    },
    "materials": {
        "interest_rates": 0.10,
        "inflation": 0.30,
        "growth": 0.25,
        "employment": 0.10,
        "yield_curve": 0.10,
        "sentiment": 0.15,
    },
    "real estate": {
        "interest_rates": 0.35,
        "inflation": 0.20,
        "growth": 0.15,
        "employment": 0.10,
        "yield_curve": 0.15,
        "sentiment": 0.05,
    },
    "communication services": {
        "interest_rates": 0.20,
        "inflation": 0.15,
        "growth": 0.30,
        "employment": 0.10,
        "yield_curve": 0.10,
        "sentiment": 0.15,
    },
}

# Fallback when the sector name doesn't match anything above
DEFAULT_WEIGHTS: Dict[str, float] = {
    "interest_rates": 0.25,
    "inflation": 0.25,
    "growth": 0.20,
    "employment": 0.10,
    "yield_curve": 0.10,
    "sentiment": 0.10,
}


def _resolve_weights(sector_name: str) -> Dict[str, float]:
    """Match sector name to weight table via case-insensitive substring search."""
    name_lower = sector_name.lower()
    for key, weights in SECTOR_WEIGHTS.items():
        if key in name_lower or name_lower in key:
            return weights
    logger.debug(
        "sector_economic_rating: no weight profile for '%s', using default", sector_name
    )
    return DEFAULT_WEIGHTS


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class SectorEconomicRatingService:
    """
    Derive and persist sector economic ratings from an EconomicSnapshot.

    Call rate_all_sectors() immediately after saving a new EconomicSnapshot.
    Each call inserts fresh rows (append-only history — same pattern as the
    rest of the app).
    """

    def rate_all_sectors(
        self, db: Session, snapshot: models.EconomicSnapshot
    ) -> List[models.SectorEconomicRating]:
        """Insert one SectorEconomicRating per sector, linked to *snapshot*."""
        macro = snapshot.components or {}
        if not macro:
            logger.warning(
                "sector_economic_rating: snapshot_id=%d has no components, skipping",
                snapshot.id,
            )
            return []

        sectors = db.query(models.Sector).all()
        rows = []
        for sector in sectors:
            try:
                row = self._rate_sector(db, sector, macro, snapshot.id)
                rows.append(row)
            except Exception:
                logger.exception(
                    "sector_economic_rating: failed for sector_id=%d (non-fatal)",
                    sector.id,
                )
                db.rollback()

        logger.info(
            "sector_economic_rating: rated %d/%d sectors from snapshot_id=%d",
            len(rows),
            len(sectors),
            snapshot.id,
        )
        return rows

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _rate_sector(
        self,
        db: Session,
        sector: models.Sector,
        macro_components: Dict[str, float],
        snapshot_id: int,
    ) -> models.SectorEconomicRating:
        weights = _resolve_weights(sector.name)

        # ----------------------------------------------------------------
        # Core formula:
        #   score = Σ weight(c) × component_score(c)  /  Σ weight(c)
        #                                               (available only)
        # ----------------------------------------------------------------
        contributions: Dict[str, dict] = {}
        total_weight = 0.0
        weighted_sum = 0.0

        for component, weight in weights.items():
            score = macro_components.get(component)
            if score is None:
                continue
            contribution = score * weight
            contributions[component] = {
                "score": round(float(score), 4),
                "weight": weight,
                "contribution": round(contribution, 4),
            }
            weighted_sum += contribution
            total_weight += weight

        economic_score = (
            round(weighted_sum / total_weight, 4) if total_weight > 0 else None
        )

        row = models.SectorEconomicRating(
            sector_id=sector.id,
            economic_score=economic_score,
            # Sub-scores: raw (un-weighted) component values for display
            gdp_sensitivity_score=macro_components.get("growth"),
            rate_sensitivity_score=macro_components.get("interest_rates"),
            inflation_sensitivity_score=macro_components.get("inflation"),
            employment_sensitivity_score=macro_components.get("employment"),
            components={
                "weights": weights,
                "contributions": contributions,
                "macro_used": {
                    k: round(float(v), 4)
                    for k, v in macro_components.items()
                    if v is not None
                },
            },
            economic_snapshot_id=snapshot_id,
            analysis=self._build_analysis(
                sector.name, economic_score, weights, macro_components
            ),
            data_source="fred_derived",
            rated_at=datetime.utcnow(),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    @staticmethod
    def _build_analysis(
        sector_name: str,
        score: Optional[float],
        weights: Dict[str, float],
        macro: Dict[str, float],
    ) -> str:
        if score is None:
            return f"{sector_name}: insufficient macro data to produce a rating."

        # Top driver = highest weighted contribution
        drivers = sorted(
            [
                (c, weights.get(c, 0) * macro.get(c, 0))
                for c in weights
                if macro.get(c) is not None
            ],
            key=lambda x: x[1],
            reverse=True,
        )
        top_driver, top_contrib = drivers[0] if drivers else ("unknown", 0.0)

        if score >= 7.5:
            label = "Favorable"
        elif score >= 5.0:
            label = "Neutral"
        else:
            label = "Headwinds"

        return (
            f"{label} economic environment for {sector_name} "
            f"(score {score:.1f}/10). "
            f"Primary driver: {top_driver.replace('_', ' ')} "
            f"(weighted contribution {top_contrib:.2f})."
        )
