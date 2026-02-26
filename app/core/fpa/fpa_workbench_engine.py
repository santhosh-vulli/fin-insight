# app/core/fpa_workbench_engine.py

from app.fpa.advanced_fpa_engine import AdvancedFPAEngine
from app.fpa.intelligence_engine import IntelligenceEngine
from app.fpa.planning_engine import PlanningEngine
from app.fpa.forecast_engine import ForecastEngine
from app.fpa.driver_engine import DriverEngine
from app.fpa.reconciliation_engine import ReconciliationEngine
from app.database.db import execute


class FPAWorkbenchEngine:

    def __init__(self):
        self.analytics = AdvancedFPAEngine()
        self.intelligence = IntelligenceEngine()
        self.planning = PlanningEngine()
        self.forecast = ForecastEngine()
        self.driver = DriverEngine()
        self.reconciliation = ReconciliationEngine()

    # ─────────────────────────────────────────────
    # LOAD GRID DATA
    # ─────────────────────────────────────────────

    def load_workbench(self, scenario_id: str, period: str):

        rows = execute("""
            SELECT account_id,
                   cost_center_id,
                   amount
            FROM fact_financials
            WHERE scenario_id = %s
            AND period = %s
        """, (scenario_id, period), fetch=True)

        # Convert to grid format
        grid_data = [
            {
                "account_id": r["account_id"],
                "cost_center_id": r["cost_center_id"],
                "actual": float(r["amount"]),
            }
            for r in rows
        ]

        # Analytics summary
        amounts = [r["amount"] for r in rows]
        anomaly = self.analytics.z_score_anomaly(amounts)

        return {
            "grid": grid_data,
            "analytics": anomaly
        }

    # ─────────────────────────────────────────────
    # UPDATE CELL
    # ─────────────────────────────────────────────

    def update_cell(self, payload, user_context):

        sheet = payload.get("sheet")

        if sheet == "plan":
            result = self.planning.submit_plan(payload, user_context)

        elif sheet == "forecast":
            result = self.forecast.generate_forecast(
                payload["scenario_id"],
                payload["start_period"],
                payload["end_period"],
                user_context
            )

        elif sheet == "driver":
            result = self.driver.set_driver(payload, user_context)

        else:
            return {"error": "Invalid sheet type"}

        return result

    # ─────────────────────────────────────────────
    # LOAD ANALYTICS PANEL
    # ─────────────────────────────────────────────

    def load_analytics(self, scenario_id: str):

        intelligence = self.intelligence.generate_insights(
            scenario_id,
            start_period=None,
            end_period=None
        )

        return intelligence