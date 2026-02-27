# fpa/scenario_engine.py

from typing import Dict, Any, List
from app.database.db import execute
from core.governance import GovernanceOrchestrator


class ScenarioEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # CREATE SCENARIO
    # ─────────────────────────────────────────────

    def create_scenario(
        self,
        scenario_name: str,
        base_scenario_id: str,
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        scenario = execute(
            """
            INSERT INTO fpa_scenarios (
                tenant_id,
                scenario_name,
                base_scenario_id,
                status,
                version,
                created_at
            )
            VALUES (%s,%s,%s,'draft',1,NOW())
            RETURNING id
            """,
            (
                user_context.get("tenant_id", "default"),
                scenario_name,
                base_scenario_id,
            ),
            fetchone=True,
        )

        new_scenario_id = scenario["id"]

        # Clone drivers from base
        self._clone_drivers(base_scenario_id, new_scenario_id)

        return {
            "scenario_created": True,
            "scenario_id": new_scenario_id,
        }

    # ─────────────────────────────────────────────
    # CLONE DRIVERS
    # ─────────────────────────────────────────────

    def _clone_drivers(self, base_scenario_id, new_scenario_id):

        execute(
            """
            INSERT INTO fpa_drivers (
                tenant_id,
                scenario_id,
                driver_name,
                driver_type,
                period,
                value,
                version,
                created_at
            )
            SELECT tenant_id,
                   %s,
                   driver_name,
                   driver_type,
                   period,
                   value,
                   1,
                   NOW()
            FROM fpa_drivers
            WHERE scenario_id = %s
            """,
            (new_scenario_id, base_scenario_id),
        )

    # ─────────────────────────────────────────────
    # COMPARE SCENARIOS
    # ─────────────────────────────────────────────

    def compare_scenarios(
        self,
        scenario_a: str,
        scenario_b: str,
    ) -> List[Dict[str, Any]]:

        rows = execute(
            """
            SELECT a.account_id,
                   a.period,
                   a.projected_amount AS value_a,
                   b.projected_amount AS value_b
            FROM fpa_forecasts a
            JOIN fpa_forecasts b
              ON a.account_id = b.account_id
             AND a.period = b.period
            WHERE a.scenario_id = %s
              AND b.scenario_id = %s
            """,
            (scenario_a, scenario_b),
            fetch=True,
        )

        comparison = []

        for row in rows:
            delta = row["value_b"] - row["value_a"]

            comparison.append({
                "account_id": row["account_id"],
                "period": row["period"],
                "delta": float(delta),
            })

        return comparison

    # ─────────────────────────────────────────────
    # APPROVE SCENARIO
    # ─────────────────────────────────────────────

    def approve_scenario(
        self,
        scenario_id: str,
        user_context: Dict[str, Any],
    ):

        result = self.governance.execute_financial_action(
            entity_id=scenario_id,
            entity_type="scenario",
            payload={"scenario_id": scenario_id},
            user_context=user_context,
        )

        if result.get("status") != "success":
            return result

        execute(
            """
            UPDATE fpa_scenarios
            SET status = 'approved',
                updated_at = NOW()
            WHERE id = %s
            """,
            (scenario_id,),
        )

        return {"scenario_approved": True}