# fpa/forecast_engine.py

from typing import Dict, Any, List
from decimal import Decimal
from app.database.db import execute
from app.core.governance import GovernanceOrchestrator


class ForecastEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # PUBLIC ENTRY – GENERATE FORECAST
    # ─────────────────────────────────────────────

    def generate_forecast(
        self,
        scenario_id: str,
        start_period: str,
        end_period: str,
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        baseline = self._get_last_actuals(scenario_id, start_period)

        drivers = self._get_drivers(scenario_id, start_period, end_period)

        projections = self._apply_driver_model(baseline, drivers)

        material_shift = self._detect_material_shift(
            scenario_id,
            projections,
        )

        entity_id = f"{scenario_id}:{start_period}:{end_period}"

        if material_shift:
            self.governance.execute_financial_action(
                entity_id=entity_id,
                entity_type="forecast",
                payload={
                    "scenario_id": scenario_id,
                    "material_shift": True,
                    "metadata": projections,
                },
                user_context=user_context,
            )

        self._persist_forecast(scenario_id, projections)

        return {
            "forecast_generated": True,
            "material_shift": material_shift,
        }

    # ─────────────────────────────────────────────
    # BASELINE
    # ─────────────────────────────────────────────

    def _get_last_actuals(self, scenario_id, start_period):

        return execute(
            """
            SELECT account_id, cost_center_id, amount
            FROM fact_financials
            WHERE scenario_id = %s
            AND period = (
                SELECT MAX(period)
                FROM fact_financials
                WHERE scenario_id = %s
                AND period < %s
            )
            """,
            (scenario_id, scenario_id, start_period),
            fetch=True,
        )

    # ─────────────────────────────────────────────
    # DRIVERS
    # ─────────────────────────────────────────────

    def _get_drivers(self, scenario_id, start_period, end_period):

        return execute(
            """
            SELECT driver_name, period, value
            FROM fpa_drivers
            WHERE scenario_id = %s
            AND period BETWEEN %s AND %s
            """,
            (scenario_id, start_period, end_period),
            fetch=True,
        )

    # ─────────────────────────────────────────────
    # APPLY DRIVER MODEL
    # ─────────────────────────────────────────────

    def _apply_driver_model(self, baseline, drivers):

        projections = []

        driver_map = {
            (d["driver_name"], d["period"]): Decimal(d["value"])
            for d in drivers
        }

        for row in baseline:

            account_id = row["account_id"]
            cost_center_id = row["cost_center_id"]
            base_amount = Decimal(row["amount"])

            for (driver_name, period), driver_value in driver_map.items():

                if driver_name == "growth_rate":
                    projected = base_amount * (1 + driver_value / 100)
                elif driver_name == "inflation_rate":
                    projected = base_amount * (1 + driver_value / 100)
                else:
                    projected = base_amount

                projections.append({
                    "account_id": account_id,
                    "cost_center_id": cost_center_id,
                    "period": period,
                    "projected_amount": float(projected),
                })

        return projections

    # ─────────────────────────────────────────────
    # MATERIAL SHIFT DETECTION
    # ─────────────────────────────────────────────

    def _detect_material_shift(self, scenario_id, projections):

        for p in projections:

            existing = execute(
                """
                SELECT projected_amount
                FROM fpa_forecasts
                WHERE scenario_id = %s
                AND account_id = %s
                AND cost_center_id = %s
                AND period = %s
                """,
                (
                    scenario_id,
                    p["account_id"],
                    p["cost_center_id"],
                    p["period"],
                ),
                fetchone=True,
            )

            if not existing:
                continue

            old = Decimal(existing["projected_amount"])
            new = Decimal(p["projected_amount"])

            if old == 0:
                return True

            variance_pct = abs((new - old) / old * 100)

            if variance_pct >= 10:
                return True

        return False

    # ─────────────────────────────────────────────
    # PERSIST FORECAST
    # ─────────────────────────────────────────────

    def _persist_forecast(self, scenario_id, projections):

        for p in projections:

            execute(
                """
                INSERT INTO fpa_forecasts (
                    tenant_id,
                    scenario_id,
                    account_id,
                    cost_center_id,
                    period,
                    projected_amount,
                    version,
                    created_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (scenario_id, account_id, cost_center_id, period)
                DO UPDATE SET
                    projected_amount = EXCLUDED.projected_amount,
                    version = fpa_forecasts.version + 1,
                    updated_at = NOW()
                """,
                (
                    "default",
                    scenario_id,
                    p["account_id"],
                    p["cost_center_id"],
                    p["period"],
                    p["projected_amount"],
                    1,
                ),
            )