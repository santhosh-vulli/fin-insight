# fpa/actuals_engine.py

from typing import Dict, Any, Optional
from decimal import Decimal
from app.database.db import execute
from core.governance import GovernanceOrchestrator


class ActualsEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # PUBLIC ENTRY – GOVERNED SUBMISSION
    # ─────────────────────────────────────────────

    def submit_actual(
        self,
        payload: Dict[str, Any],
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        """
        Advanced controlled actual submission.

        Flow:
        1. Referential validation
        2. Lock validation
        3. Detect variance/material change
        4. Route via governance
        5. Apply update after approval
        """

        self._validate_referential_integrity(payload)
        self._check_period_lock(payload)

        existing = self._get_existing_record(payload)

        variance_meta = self._detect_material_change(existing, payload)

        governance_payload = {
            "entity_type": "actual",
            "entity_id": payload.get("id"),
            "amount": payload["amount"],
            "variance_percentage": variance_meta.get("variance_pct"),
            "material_change": variance_meta.get("material"),
            "metadata": payload,
        }

        result = self.governance.execute_financial_action(
            entity_id=str(payload.get("id")),
            entity_type="actual",
            payload=governance_payload,
            user_context=user_context,
        )

        if result.get("status") != "success":
            return result

        # Apply update only if governance allows
        self._apply_update(payload)

        return {
            "status": "posted",
            "state": result.get("state"),
        }

    # ─────────────────────────────────────────────
    # DOMAIN VALIDATIONS
    # ─────────────────────────────────────────────

    def _validate_referential_integrity(self, payload):

        required_keys = [
            "account_id",
            "cost_center_id",
            "scenario_id",
            "period",
            "amount",
        ]

        for key in required_keys:
            if key not in payload:
                raise ValueError(f"Missing required field: {key}")

    def _check_period_lock(self, payload):

        locked = execute(
            """
            SELECT 1 FROM period_locks
            WHERE period = %s
            AND locked = TRUE
            """,
            (payload["period"],),
            fetchone=True,
        )

        if locked:
            raise Exception("Financial period is locked.")

    # ─────────────────────────────────────────────
    # VARIANCE + MATERIALITY
    # ─────────────────────────────────────────────

    def _get_existing_record(self, payload):

        return execute(
            """
            SELECT *
            FROM fact_financials
            WHERE account_id = %s
            AND cost_center_id = %s
            AND scenario_id = %s
            AND period = %s
            """,
            (
                payload["account_id"],
                payload["cost_center_id"],
                payload["scenario_id"],
                payload["period"],
            ),
            fetchone=True,
        )

    def _detect_material_change(self, existing, payload):

        if not existing:
            return {"material": True, "variance_pct": 100}

        old_amount = Decimal(existing["amount"])
        new_amount = Decimal(payload["amount"])

        if old_amount == 0:
            return {"material": True, "variance_pct": 100}

        delta = new_amount - old_amount
        variance_pct = abs((delta / old_amount) * 100)

        material = variance_pct >= 10  # configurable later via DB

        return {
            "material": material,
            "variance_pct": float(variance_pct),
        }

    # ─────────────────────────────────────────────
    # APPLY UPDATE (CONTROLLED WRITE)
    # ─────────────────────────────────────────────

    def _apply_update(self, payload):

        execute(
            """
            INSERT INTO fact_financials (
                account_id,
                cost_center_id,
                scenario_id,
                period,
                amount,
                version,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (account_id, cost_center_id, scenario_id, period)
            DO UPDATE SET
                amount = EXCLUDED.amount,
                version = fact_financials.version + 1,
                updated_at = NOW()
            """,
            (
                payload["account_id"],
                payload["cost_center_id"],
                payload["scenario_id"],
                payload["period"],
                payload["amount"],
                payload.get("version", 1),
            ),
        )