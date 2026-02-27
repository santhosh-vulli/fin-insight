# fpa/reconciliation_engine.py

from typing import Dict, Any, List
from decimal import Decimal
from app.database.db import execute
from app.core.governance import GovernanceOrchestrator


class ReconciliationEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # PUBLIC ENTRY – RUN RECONCILIATION
    # ─────────────────────────────────────────────

    def reconcile(
        self,
        scenario_id: str,
        period: str,
        reference_type: str,  # "plan" or "forecast"
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        records = self._fetch_data(scenario_id, period, reference_type)

        mismatches = []

        for row in records:

            actual = Decimal(row["actual_amount"] or 0)
            reference = Decimal(row["reference_amount"] or 0)

            variance = actual - reference
            variance_pct = (
                abs((variance / reference) * 100)
                if reference != 0 else 100
            )

            status = "balanced"

            if variance_pct >= 10:  # later DB configurable
                status = "mismatch"

                entity_id = (
                    f"{scenario_id}:{row['account_id']}:"
                    f"{row['cost_center_id']}:{period}"
                )

                self.governance.execute_financial_action(
                    entity_id=entity_id,
                    entity_type="reconciliation",
                    payload={
                        "variance_pct": float(variance_pct),
                        "actual": float(actual),
                        "reference": float(reference),
                    },
                    user_context=user_context,
                )

                mismatches.append(entity_id)

            self._persist_result(
                scenario_id,
                row,
                actual,
                reference,
                variance,
                variance_pct,
                status,
            )

        return {
            "reconciliation_completed": True,
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
        }

    # ─────────────────────────────────────────────
    # DATA FETCH
    # ─────────────────────────────────────────────

    def _fetch_data(self, scenario_id, period, reference_type):

        if reference_type == "plan":
            reference_table = "fpa_plans"
            reference_field = "planned_amount"
        else:
            reference_table = "fpa_forecasts"
            reference_field = "projected_amount"

        query = f"""
            SELECT a.account_id,
                   a.cost_center_id,
                   a.amount AS actual_amount,
                   r.{reference_field} AS reference_amount
            FROM fact_financials a
            LEFT JOIN {reference_table} r
              ON a.account_id = r.account_id
             AND a.cost_center_id = r.cost_center_id
             AND a.period = r.period
             AND r.scenario_id = %s
            WHERE a.scenario_id = %s
            AND a.period = %s
        """

        return execute(
            query,
            (scenario_id, scenario_id, period),
            fetch=True,
        )

    # ─────────────────────────────────────────────
    # PERSIST RECONCILIATION RESULT
    # ─────────────────────────────────────────────

    def _persist_result(
        self,
        scenario_id,
        row,
        actual,
        reference,
        variance,
        variance_pct,
        status,
    ):

        execute(
            """
            INSERT INTO fpa_reconciliation (
                tenant_id,
                scenario_id,
                account_id,
                cost_center_id,
                period,
                actual_amount,
                reference_amount,
                variance,
                variance_pct,
                status,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                "default",
                scenario_id,
                row["account_id"],
                row["cost_center_id"],
                row.get("period"),
                actual,
                reference,
                variance,
                variance_pct,
                status,
            ),
        )