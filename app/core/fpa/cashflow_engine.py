# fpa/cashflow_engine.py

from typing import Dict, Any, List
from decimal import Decimal
from core.db import execute
from core.governance import GovernanceOrchestrator


class CashflowEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # PUBLIC ENTRY – GENERATE CASHFLOW
    # ─────────────────────────────────────────────

    def generate_projection(
        self,
        scenario_id: str,
        start_period: str,
        end_period: str,
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        """
        Generate governed cashflow projection.
        """

        base_data = self._fetch_financial_data(
            scenario_id,
            start_period,
            end_period,
        )

        projection = self._calculate_cashflow(base_data)

        liquidity_risk = self._detect_liquidity_risk(projection)

        governance_payload = {
            "entity_type": "cashflow_projection",
            "entity_id": f"{scenario_id}:{start_period}:{end_period}",
            "liquidity_risk": liquidity_risk,
            "metadata": projection,
        }

        if liquidity_risk:
            self.governance.execute_financial_action(
                entity_id=governance_payload["entity_id"],
                entity_type="cashflow_projection",
                payload=governance_payload,
                user_context=user_context,
            )

        return {
            "projection": projection,
            "liquidity_risk": liquidity_risk,
        }

    # ─────────────────────────────────────────────
    # DATA FETCH
    # ─────────────────────────────────────────────

    def _fetch_financial_data(
        self,
        scenario_id: str,
        start_period: str,
        end_period: str,
    ) -> List[Dict[str, Any]]:

        return execute(
            """
            SELECT account_id, amount, period
            FROM fact_financials
            WHERE scenario_id = %s
            AND period BETWEEN %s AND %s
            """,
            (scenario_id, start_period, end_period),
            fetch=True,
        )

    # ─────────────────────────────────────────────
    # CASHFLOW CALCULATION
    # ─────────────────────────────────────────────

    def _calculate_cashflow(self, records):

        projection = {}
        running_balance = Decimal("0")

        for row in records:

            period = row["period"]
            amount = Decimal(row["amount"])

            # TODO: replace with DB-driven account classification
            cash_impact = self._classify_cash_impact(row["account_id"], amount)

            running_balance += cash_impact

            projection.setdefault(period, {})
            projection[period]["net_cash"] = float(cash_impact)
            projection[period]["cumulative_balance"] = float(running_balance)

        return projection

    # ─────────────────────────────────────────────
    # ACCOUNT CLASSIFICATION
    # ─────────────────────────────────────────────

    def _classify_cash_impact(self, account_id, amount):

        classification = execute(
            """
            SELECT cashflow_type
            FROM dim_account
            WHERE id = %s
            """,
            (account_id,),
            fetchone=True,
        )

        if not classification:
            return Decimal("0")

        if classification["cashflow_type"] == "non_cash":
            return Decimal("0")

        return Decimal(amount)

    # ─────────────────────────────────────────────
    # LIQUIDITY RISK DETECTION
    # ─────────────────────────────────────────────

    def _detect_liquidity_risk(self, projection):

        for period, data in projection.items():
            if data["cumulative_balance"] < 0:
                return True

        return False