# app/core/fpa_workbench_engine.py

from .advanced_fpa_engine import AdvancedFPAEngine
from .intelligence_engine import IntelligenceEngine
from .planning_engine import PlanningEngine
from .forecast_engine import ForecastEngine
from .driver_engine import DriverEngine
from .reconciliation_engine import ReconciliationEngine

from app.database.db import execute


class FPAWorkbenchEngine:

    def __init__(self, governance=None):
        self.analytics = AdvancedFPAEngine()
        self.intelligence = IntelligenceEngine()
        self.planning = PlanningEngine(governance)
        self.forecast = ForecastEngine(governance)
        self.driver = DriverEngine(governance)
        self.reconciliation = ReconciliationEngine(governance)

    # ─────────────────────────────────────────────
    # INTERNAL HELPERS
    # ─────────────────────────────────────────────

    def _resolve_scenario(self, scenario_code: str):
        row = execute("""
            SELECT id
            FROM dim_scenario
            WHERE code = %s
        """, (scenario_code,), fetch=True)

        if not row:
            return None

        return row[0]["id"]

    def _resolve_version(self, scenario_id, version_number: int):
        row = execute("""
            SELECT id, status
            FROM dim_version
            WHERE scenario_id = %s
              AND version_number = %s
        """, (scenario_id, version_number), fetch=True)

        if not row:
            return None

        return row[0]["id"], row[0]["status"]

    def _resolve_period(self, period_code: str):
        row = execute("""
            SELECT id
            FROM dim_period
            WHERE code = %s
        """, (period_code,), fetch=True)

        if not row:
            return None

        return row[0]["id"]

    # ─────────────────────────────────────────────
    # LOAD GRID DATA (VERSION AWARE)
    # ─────────────────────────────────────────────

    def load_workbench(self, scenario_code: str, version_number: int, period_code: str):

        scenario_id = self._resolve_scenario(scenario_code)
        if not scenario_id:
            return {"error": f"Invalid scenario_code: {scenario_code}"}

        version_data = self._resolve_version(scenario_id, version_number)
        if not version_data:
            return {"error": f"Invalid version_number: {version_number}"}

        version_id, version_status = version_data

        period_id = self._resolve_period(period_code)
        if not period_id:
            return {"error": f"Invalid period_code: {period_code}"}

        # Version-aware fact query
        rows = execute("""
            SELECT account_id,
                   cost_center_id,
                   amount
            FROM fact_financials
            WHERE scenario_id = %s
              AND version_id  = %s
              AND period_id   = %s
        """, (scenario_id, version_id, period_id), fetch=True)

        grid_data = [
            {
                "account_id": r["account_id"],
                "cost_center_id": r["cost_center_id"],
                "value": float(r["amount"]),
            }
            for r in rows
        ]

        amounts = [r["amount"] for r in rows]
        anomaly = self.analytics.z_score_anomaly(amounts) if amounts else {}

        return {
            "scenario_id": str(scenario_id),
            "version_number": version_number,
            "version_status": version_status,
            "period_id": str(period_id),
            "grid": grid_data,
            "analytics": anomaly
        }

    # ─────────────────────────────────────────────
    # UPDATE CELL (LIFECYCLE ENFORCED)
    # ─────────────────────────────────────────────

    def update_cell(self, payload, user_context):

        scenario_code = payload.get("scenario_code")
        version_number = payload.get("version_number")

        if not scenario_code or version_number is None:
            return {"error": "scenario_code and version_number required"}

        scenario_id = self._resolve_scenario(scenario_code)
        if not scenario_id:
            return {"error": "Invalid scenario"}

        version_data = self._resolve_version(scenario_id, version_number)
        if not version_data:
            return {"error": "Invalid version"}

        version_id, version_status = version_data

        # 🔐 Lifecycle enforcement
        if version_status != "draft":
            return {"error": f"Version is '{version_status}' and cannot be modified"}

        # Inject resolved identifiers
        payload["scenario_id"] = scenario_id
        payload["version_id"] = version_id

        sheet = payload.get("sheet")

        if sheet == "plan":
            result = self.planning.submit_plan(payload, user_context)

        elif sheet == "forecast":
            result = self.forecast.generate_forecast(
                payload["scenario_code"],
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
    # LOAD ANALYTICS PANEL (VERSION AWARE)
    # ─────────────────────────────────────────────

    def load_analytics(self, scenario_code: str, version_number: int):

        scenario_id = self._resolve_scenario(scenario_code)
        if not scenario_id:
            return {"error": f"Invalid scenario_code: {scenario_code}"}

        version_data = self._resolve_version(scenario_id, version_number)
        if not version_data:
            return {"error": f"Invalid version_number: {version_number}"}

        version_id, version_status = version_data

        intelligence = self.intelligence.generate_insights(
            scenario_id=scenario_id,
            version_id=version_id,
            start_period=None,
            end_period=None
        )

        return {
            "scenario_id": str(scenario_id),
            "version_number": version_number,
            "version_status": version_status,
            "insights": intelligence
        }