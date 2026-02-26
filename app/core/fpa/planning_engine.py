# fpa/planning_engine.py

from typing import Dict, Any
from decimal import Decimal
from core.db import execute
from core.governance import GovernanceOrchestrator


class PlanningEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # CREATE / UPDATE PLAN
    # ─────────────────────────────────────────────

    def submit_plan(
        self,
        payload: Dict[str, Any],
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        self._validate_payload(payload)

        existing = self._get_existing_plan(payload)

        if existing and existing["locked"]:
            raise Exception("Plan is locked and cannot be modified.")

        variance_meta = self._detect_material_change(existing, payload)

        governance_payload = {
            "entity_type": "plan",
            "entity_id": f"{payload['scenario_id']}:{payload['account_id']}:{payload['cost_center_id']}:{payload['period']}",
            "old_value": existing["planned_amount"] if existing else None,
            "new_value": payload["planned_amount"],
            "variance_percentage": variance_meta.get("variance_pct"),
            "material_change": variance_meta.get("material"),
        }

        result = self.governance.execute_financial_action(
            entity_id=governance_payload["entity_id"],
            entity_type="plan",
            payload=governance_payload,
            user_context=user_context,
        )

        if result.get("status") != "success":
            return result

        self._persist_plan(payload)

        return {
            "status": "plan_submitted",
            "state": result.get("state"),
        }

    # ─────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────

    def _validate_payload(self, payload):

        required = [
            "scenario_id",
            "account_id",
            "cost_center_id",
            "period",
            "planned_amount",
        ]

        for key in required:
            if key not in payload:
                raise ValueError(f"Missing field: {key}")

    # ─────────────────────────────────────────────
    # FETCH EXISTING
    # ─────────────────────────────────────────────

    def _get_existing_plan(self, payload):

        return execute(
            """
            SELECT *
            FROM fpa_plans
            WHERE scenario_id = %s
            AND account_id = %s
            AND cost_center_id = %s
            AND period = %s
            """,
            (
                payload["scenario_id"],
                payload["account_id"],
                payload["cost_center_id"],
                payload["period"],
            ),
            fetchone=True,
        )

    # ─────────────────────────────────────────────
    # MATERIAL CHANGE DETECTION
    # ─────────────────────────────────────────────

    def _detect_material_change(self, existing, payload):

        if not existing:
            return {"material": True, "variance_pct": 100}

        old = Decimal(existing["planned_amount"])
        new = Decimal(payload["planned_amount"])

        if old == 0:
            return {"material": True, "variance_pct": 100}

        variance_pct = abs((new - old) / old * 100)

        material = variance_pct >= 10  # configurable later

        return {
            "material": material,
            "variance_pct": float(variance_pct),
        }

    # ─────────────────────────────────────────────
    # PERSIST PLAN
    # ─────────────────────────────────────────────

    def _persist_plan(self, payload):

        execute(
            """
            INSERT INTO fpa_plans (
                tenant_id,
                scenario_id,
                account_id,
                cost_center_id,
                period,
                planned_amount,
                version,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (scenario_id, account_id, cost_center_id, period)
            DO UPDATE SET
                planned_amount = EXCLUDED.planned_amount,
                version = fpa_plans.version + 1,
                updated_at = NOW()
            """,
            (
                payload.get("tenant_id", "default"),
                payload["scenario_id"],
                payload["account_id"],
                payload["cost_center_id"],
                payload["period"],
                payload["planned_amount"],
                payload.get("version", 1),
            ),
        )