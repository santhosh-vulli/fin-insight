# fpa/intelligence_engine.py

from typing import Dict, Any, List
from decimal import Decimal
from core.db import execute


class IntelligenceEngine:

    # ─────────────────────────────────────────────
    # MASTER INSIGHT ENTRY
    # ─────────────────────────────────────────────

    def generate_insights(
        self,
        scenario_id: str,
        start_period: str,
        end_period: str,
    ) -> Dict[str, Any]:

        insights = {
            "variance_alerts": self._variance_analysis(scenario_id),
            "liquidity_risk": self._liquidity_analysis(scenario_id),
            "driver_volatility": self._driver_volatility(scenario_id),
            "approval_bottlenecks": self._workflow_bottlenecks(),
            "sla_patterns": self._sla_analysis(),
        }

        insights["risk_score"] = self._calculate_risk_score(insights)

        return insights

    # ─────────────────────────────────────────────
    # VARIANCE ANALYSIS
    # ─────────────────────────────────────────────

    def _variance_analysis(self, scenario_id):

        rows = execute(
            """
            SELECT f.account_id,
                   f.projected_amount,
                   a.amount
            FROM fpa_forecasts f
            JOIN fact_financials a
              ON f.account_id = a.account_id
             AND f.period = a.period
            WHERE f.scenario_id = %s
            """,
            (scenario_id,),
            fetch=True,
        )

        alerts = []

        for row in rows:
            forecast = Decimal(row["projected_amount"])
            actual = Decimal(row["amount"])

            if actual == 0:
                continue

            variance_pct = abs((forecast - actual) / actual * 100)

            if variance_pct >= 15:
                alerts.append({
                    "account_id": row["account_id"],
                    "variance_pct": float(variance_pct),
                })

        return alerts

    # ─────────────────────────────────────────────
    # LIQUIDITY ANALYSIS
    # ─────────────────────────────────────────────

    def _liquidity_analysis(self, scenario_id):

        rows = execute(
            """
            SELECT period, SUM(projected_amount) as total
            FROM fpa_forecasts
            WHERE scenario_id = %s
            GROUP BY period
            ORDER BY period
            """,
            (scenario_id,),
            fetch=True,
        )

        cumulative = Decimal("0")
        risk_periods = []

        for row in rows:
            cumulative += Decimal(row["total"])

            if cumulative < 0:
                risk_periods.append(row["period"])

        return {
            "negative_balance_periods": risk_periods,
            "risk": len(risk_periods) > 0,
        }

    # ─────────────────────────────────────────────
    # DRIVER VOLATILITY
    # ─────────────────────────────────────────────

    def _driver_volatility(self, scenario_id):

        rows = execute(
            """
            SELECT driver_name,
                   MAX(value) - MIN(value) as range
            FROM fpa_drivers
            WHERE scenario_id = %s
            GROUP BY driver_name
            """,
            (scenario_id,),
            fetch=True,
        )

        volatile = []

        for row in rows:
            if Decimal(row["range"]) > 20:  # configurable later
                volatile.append(row["driver_name"])

        return volatile

    # ─────────────────────────────────────────────
    # WORKFLOW BOTTLENECKS
    # ─────────────────────────────────────────────

    def _workflow_bottlenecks(self):

        rows = execute(
            """
            SELECT state,
                   AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_seconds
            FROM workflow_instances
            GROUP BY state
            """,
            fetch=True,
        )

        bottlenecks = []

        for row in rows:
            if row["avg_seconds"] and row["avg_seconds"] > 86400:
                bottlenecks.append(row["state"])

        return bottlenecks

    # ─────────────────────────────────────────────
    # SLA ANALYSIS
    # ─────────────────────────────────────────────

    def _sla_analysis(self):

        rows = execute(
            """
            SELECT entity_type, COUNT(*) as breaches
            FROM sla_instances
            WHERE breached = TRUE
            GROUP BY entity_type
            """,
            fetch=True,
        )

        return rows

    # ─────────────────────────────────────────────
    # RISK SCORING
    # ─────────────────────────────────────────────

    def _calculate_risk_score(self, insights):

        score = 0

        score += len(insights["variance_alerts"]) * 5

        if insights["liquidity_risk"]["risk"]:
            score += 20

        score += len(insights["driver_volatility"]) * 3

        score += len(insights["approval_bottlenecks"]) * 2

        return score