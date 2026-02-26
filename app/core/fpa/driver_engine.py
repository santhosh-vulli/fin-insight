# fpa/driver_engine.py

from typing import Dict, Any
from decimal import Decimal
from core.db import execute
from core.governance import GovernanceOrchestrator


class DriverEngine:

    def __init__(self, governance: GovernanceOrchestrator):
        self.governance = governance

    # ─────────────────────────────────────────────
    # PUBLIC ENTRY – SET DRIVER VALUE
    # ─────────────────────────────────────────────

    def set_driver(
        self,
        payload: Dict[str, Any],
        user_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        """
        Controlled driver update.
        Triggers governance if material change.
        """

        self._validate_payload(payload)

        existing = self._get_existing_driver(payload)

        variance_meta = self._detect_material_change(existing, payload)

        governance_payload = {
            "entity_type": "driver",
            "entity_id": f"{payload['scenario_id']}:{payload['driver_name']}:{payload['period']}",
            "old_value": existing["value"] if existing else None,
            "new_value": payload["value"],
            "variance_percentage": variance_meta.get("variance_pct"),
            "material_change": variance_meta.get("material"),
        }

        result = self.governance.execute_financial_action(
            entity_id=governance_payload["entity_id"],
            entity_type="driver",
            payload=governance_payload,
            user_context=user_context,
        )

        if result.get("status") != "success":
            return result

        self._apply_driver_update(payload)

        return {
            "status": "driver_updated",
            "state": result.get("state"),
        }

    # ─────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────

    def _validate_payload(self, payload):

        required = [
            "scenario_id",
            "driver_name",
            "driver_type",
            "period",
            "value",
        ]

        for key in required:
            if key not in payload:
                raise ValueError(f"Missing required field: {key}")

    # ─────────────────────────────────────────────
    # FETCH EXISTING
    # ─────────────────────────────────────────────

    def _get_existing_driver(self, payload):

        return execute(
            """
            SELECT *
            FROM public.fpa_drivers
            WHERE scenario_id = %s
            AND driver_name = %s
            AND period = %s
            """,
            (
                payload["scenario_id"],
                payload["driver_name"],
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

        old = Decimal(existing["value"])
        new = Decimal(payload["value"])

        if old == 0:
            return {"material": True, "variance_pct": 100}

        delta = new - old
        variance_pct = abs((delta / old) * 100)

        material = variance_pct >= 15  # configurable later via DB

        return {
            "material": material,
            "variance_pct": float(variance_pct),
        }

    # ─────────────────────────────────────────────
    # APPLY UPDATE
    # ─────────────────────────────────────────────

    def _apply_driver_update(self, payload):

        execute(
            """
            INSERT INTO public.fpa_drivers (
                tenant_id,
                scenario_id,
                driver_name,
                driver_type,
                period,
                value,
                version,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (scenario_id, driver_name, period)
            DO UPDATE SET
                value = EXCLUDED.value,
                version = fpa_drivers.version + 1,
                updated_at = NOW()
            """,
            (
                payload.get("tenant_id", "default"),
                payload["scenario_id"],
                payload["driver_name"],
                payload["driver_type"],
                payload["period"],
                payload["value"],
                payload.get("version", 1),
            ),
        )