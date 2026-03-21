import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException
from sqlalchemy.orm import Session

import app.crud.fundamental as fundamental_crud
import app.crud.fundamental_analysis as fundamental_analysis_crud
import app.models as models
import app.schemas as schemas
from app.services.fundamental_analysis import (
    ComparableAnalysis,
    FundamentalAnalysisEngine,
)
from app.services.pillar_rating import (
    PillarRatingCalculator,
    PillarResult,
    PillarValidator,
    StockContext,
)
from app.utils.rating_utils import FinnhubClient
from config import get_settings

logger = logging.getLogger(__name__)


class FundamentalService:
    def __init__(self, finnhub_api_key: Optional[str] = None, ttl_hours: int = 24):
        settings = get_settings()
        self.client = FinnhubClient(
            finnhub_api_key or settings.finnhub_api_key, max_per_minute=55
        )
        self.ttl_hours = ttl_hours

    def refresh_for_stock(
        self, db: Session, stock_id: int, force_refresh: bool = False
    ) -> models.FundamentalIndicator:
        stock = db.query(models.Stock).filter(models.Stock.id == stock_id).first()
        if not stock:
            raise HTTPException(status_code=404, detail="Stock not found")

        cutoff = datetime.utcnow() - timedelta(hours=self.ttl_hours)
        if not force_refresh:
            cached = fundamental_crud.latest(db, stock.id, since=cutoff)
            if cached:
                return cached

        payload = self._fetch_metrics_payload(stock.symbol, stock.id)
        new_record = fundamental_crud.create(db, payload)
        self._store_analysis(db, new_record)
        return new_record

    def refresh_all(self, db: Session, force_refresh: bool = False) -> dict:
        updated = 0
        stocks = db.query(models.Stock).all()
        for stock in stocks:
            try:
                self.refresh_for_stock(db, stock.id, force_refresh=force_refresh)
                updated += 1
            except Exception:
                db.rollback()
                continue
        return {"updated": updated, "timestamp": datetime.utcnow()}

    def _fetch_metrics_payload(self, symbol: str, stock_id: int) -> dict:
        data = self.client.get("/stock/metric", {"symbol": symbol, "metric": "all"})
        if not data or not data.get("metric"):
            raise HTTPException(
                status_code=503, detail="Fundamental metrics unavailable right now"
            )
        metrics = data["metric"] or {}
        return {
            "stock_id": stock_id,
            "pe_ratio": self._first_metric(metrics, ["peBasicExclExtraTTM", "peTTM"]),
            "pb_ratio": self._first_metric(metrics, ["pbAnnual", "pbQuarterly"]),
            "debt_to_equity": self._first_metric(
                metrics,
                [
                    "totalDebt/totalEquityAnnual",
                    "totalDebt/totalEquityQuarterly",
                    "totalDebtToEquityAnnual",
                    "totalDebtToEquityQuarterly",
                ],
            ),
            "profit_margin": self._first_metric(
                metrics, ["netProfitMarginTTM", "netProfitMarginAnnual"]
            ),
            "dividend_yield": self._first_metric(
                metrics, ["dividendYieldIndicatedAnnual", "dividendYieldTTM"]
            ),
            "raw_metrics": metrics,
            "data_source": "finnhub",
            "fetched_at": datetime.utcnow(),
        }

    def _analyze_and_store(
        self, db: Session, raw_record: models.FundamentalIndicator
    ) -> models.FundamentalAnalysis:
        result = FundamentalAnalysisEngine().analyze(raw_record)

        stock = db.query(models.Stock).filter_by(id=raw_record.stock_id).first()
        stock_ctx = StockContext(
            market_cap=stock.market_cap if stock else None,
            current_price=stock.current_price if stock else None,
        )
        pillar = PillarRatingCalculator().compute(
            raw_record.raw_metrics or {}, stock_ctx
        )

        # CCA: sector peer comparison
        peer_metrics_list, sector_name = self._build_peer_metrics(
            db, raw_record.stock_id
        )
        raw = raw_record.raw_metrics or {}
        stock_metrics: Dict[str, Optional[float]] = {
            "pe_ratio": self._first_metric(raw, ["peBasicExclExtraTTM", "peTTM"]),
            "pb_ratio": self._first_metric(raw, ["pbAnnual", "pbQuarterly"]),
            "debt_to_equity": self._first_metric(
                raw,
                [
                    "totalDebt/totalEquityAnnual",
                    "totalDebt/totalEquityQuarterly",
                    "totalDebtToEquityAnnual",
                ],
            ),
            "net_margin": self._first_metric(
                raw, ["netProfitMarginTTM", "netProfitMarginAnnual"]
            ),
            "roe": self._first_metric(raw, ["roeTTM", "roeRfy"]),
        }
        cca_result = ComparableAnalysis(
            stock_metrics, peer_metrics_list, sector_name
        ).analyze()

        narrative = result["narrative"]
        narrative["peer_cca"] = cca_result

        payload = schemas.FundamentalAnalysisCreate(
            stock_id=raw_record.stock_id,
            fundamental_indicator_id=raw_record.id,
            normalized_scores=result["normalized_scores"],
            composite_scores=result["composite_scores"],
            anomalies=result["anomalies"],
            risk_rating=narrative["risk_rating"],
            confidence=narrative["confidence"],
            narrative=narrative,
            analyzed_at=datetime.utcnow(),
            valuation_score=pillar.valuation_score,
            profitability_score=pillar.profitability_score,
            growth_score=pillar.growth_score,
            health_score=pillar.health_score,
            cashflow_score=pillar.cashflow_score,
            efficiency_score=pillar.efficiency_score,
            overall_fundamental_rating=pillar.overall_fundamental_rating,
        )
        row = fundamental_analysis_crud.upsert(db, payload)

        # Server-side validation: sanity checks, sensitivity analysis, distribution
        try:
            sector_pillar_scores = self._build_sector_pillar_scores(
                db, raw_record.stock_id
            )
            PillarValidator().run_all(
                pillar,
                raw_record.raw_metrics or {},
                raw_record.stock_id,
                sector_pillar_scores=sector_pillar_scores,
            )
            self._log_distribution_check(db, raw_record.stock_id, pillar)
            self._log_cca_result(raw_record.stock_id, cca_result)
        except Exception:
            logger.exception(
                "pillar_validation failed for stock_id=%d (non-fatal)",
                raw_record.stock_id,
            )

        return row

    def _build_peer_metrics(
        self, db: Session, stock_id: int
    ) -> Tuple[List[Dict[str, Optional[float]]], Optional[str]]:
        """Return (peer_metrics_list, sector_name) for all sector peers with data."""
        stock = db.query(models.Stock).filter_by(id=stock_id).first()
        if not stock or stock.sector_id is None:
            return [], None

        sector = db.query(models.Sector).filter_by(id=stock.sector_id).first()
        sector_name = sector.name if sector else None

        peer_stocks = (
            db.query(models.Stock)
            .filter(
                models.Stock.sector_id == stock.sector_id,
                models.Stock.id != stock_id,
            )
            .all()
        )

        peer_metrics: List[Dict[str, Optional[float]]] = []
        for peer in peer_stocks:
            fi = (
                db.query(models.FundamentalIndicator)
                .filter_by(stock_id=peer.id)
                .order_by(models.FundamentalIndicator.fetched_at.desc())
                .first()
            )
            if fi is None:
                continue
            raw = fi.raw_metrics or {}
            peer_metrics.append(
                {
                    "pe_ratio": self._first_metric(
                        raw, ["peBasicExclExtraTTM", "peTTM"]
                    ),
                    "pb_ratio": self._first_metric(raw, ["pbAnnual", "pbQuarterly"]),
                    "debt_to_equity": self._first_metric(
                        raw,
                        [
                            "totalDebt/totalEquityAnnual",
                            "totalDebt/totalEquityQuarterly",
                            "totalDebtToEquityAnnual",
                        ],
                    ),
                    "net_margin": self._first_metric(
                        raw, ["netProfitMarginTTM", "netProfitMarginAnnual"]
                    ),
                    "roe": self._first_metric(raw, ["roeTTM", "roeRfy"]),
                }
            )

        return peer_metrics, sector_name

    def _build_sector_pillar_scores(
        self, db: Session, stock_id: int
    ) -> List[Dict[str, Optional[float]]]:
        """Return pillar score dicts for all sector peers with a FundamentalAnalysis row."""
        stock = db.query(models.Stock).filter_by(id=stock_id).first()
        if not stock or stock.sector_id is None:
            return []

        peer_ids = [
            row.id
            for row in db.query(models.Stock.id)
            .filter(
                models.Stock.sector_id == stock.sector_id,
                models.Stock.id != stock_id,
            )
            .all()
        ]
        if not peer_ids:
            return []

        analyses = (
            db.query(models.FundamentalAnalysis)
            .filter(models.FundamentalAnalysis.stock_id.in_(peer_ids))
            .distinct(models.FundamentalAnalysis.stock_id)
            .order_by(
                models.FundamentalAnalysis.stock_id,
                models.FundamentalAnalysis.analyzed_at.desc(),
            )
            .all()
        )
        return [
            {
                "valuation": a.valuation_score,
                "profitability": a.profitability_score,
                "growth": a.growth_score,
                "health": a.health_score,
                "cashflow": a.cashflow_score,
                "efficiency": a.efficiency_score,
            }
            for a in analyses
        ]

    def _log_cca_result(self, stock_id: int, cca: dict) -> None:
        if not cca.get("available"):
            logger.info("cca stock_id=%d: no sector peers available", stock_id)
            return
        verdict = cca.get("valuation_verdict", {})
        logger.info(
            "cca stock_id=%d: sector=%s peers=%d verdict=%s pe_premium=%s roe_pct=%s",
            stock_id,
            cca.get("sector"),
            cca.get("peer_count", 0),
            verdict.get("label"),
            verdict.get("pe_premium_pct"),
            verdict.get("roe_percentile"),
        )

    def _log_distribution_check(
        self, db: Session, stock_id: int, pillar: PillarResult
    ) -> None:
        """Log each pillar score's percentile rank across all stocks in the DB."""
        fields = [
            ("valuation_score", pillar.valuation_score),
            ("profitability_score", pillar.profitability_score),
            ("growth_score", pillar.growth_score),
            ("health_score", pillar.health_score),
            ("cashflow_score", pillar.cashflow_score),
            ("efficiency_score", pillar.efficiency_score),
            ("overall_fundamental_rating", pillar.overall_fundamental_rating),
        ]
        for field_name, score in fields:
            if score is None:
                continue
            col = getattr(models.FundamentalAnalysis, field_name)
            total = db.query(models.FundamentalAnalysis).filter(col.isnot(None)).count()
            if total == 0:
                continue
            below = db.query(models.FundamentalAnalysis).filter(col < score).count()
            pct = round(below / total * 100, 1)
            logger.info(
                "pillar_distribution stock_id=%d: %s=%.4f is %.1f percentile (n=%d stocks)",
                stock_id,
                field_name,
                score,
                pct,
                total,
            )

    def _store_analysis(
        self, db: Session, raw_record: models.FundamentalIndicator
    ) -> None:
        self._analyze_and_store(db, raw_record)

    @staticmethod
    def _first_metric(metrics: dict, keys) -> Optional[float]:
        for key in keys:
            if key in metrics and metrics[key] is not None:
                try:
                    return float(metrics[key])
                except Exception:
                    continue
        return None
