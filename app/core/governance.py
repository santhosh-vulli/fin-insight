# core/governance.py

from core.db import execute
from core.rule_engine import validate_financial_edit
from core.workflow import WorkflowEngine
from core.sla import SLAEngine
from core.audit import AuditLogger


class GovernanceOrchestrator:

    def __init__(
        self,
        workflow: WorkflowEngine,
        sla: SLAEngine,
        audit: AuditLogger,
    ):
        self.workflow = workflow
        self.sla = sla
        self.audit = audit

    def execute_financial_action(
        self,
        entity_id: str,
        entity_type: str,
        payload: dict,
        user_context: dict,
    ):

        execute("BEGIN")

        try:
            # ─────────────────────────────
            # 1️⃣ RULE VALIDATION
            # ─────────────────────────────
            rule_result = validate_financial_edit(
                payload=payload,
                user_context=user_context,
            )

            if not rule_result["passed"]:
                execute("ROLLBACK")
                return rule_result

            # ─────────────────────────────
            # 2️⃣ WORKFLOW TRANSITION
            # ─────────────────────────────
            new_state = self.workflow.transition(
                entity_id=entity_id,
                action=rule_result.get("action_required"),
                user_context=user_context,
            )

            # ─────────────────────────────
            # 3️⃣ START SLA FOR NEW STATE
            # ─────────────────────────────
            if new_state:
                self.sla.start(
                    entity_id=entity_id,
                    entity_type=entity_type,
                    state=new_state,
                    tenant_id=user_context.get("tenant_id", "default"),
                )

            # ─────────────────────────────
            # 4️⃣ AUDIT LOG
            # ─────────────────────────────
            self.audit.log_user_action(
                action="governance_action_executed",
                description=f"{entity_type}:{entity_id} moved to {new_state}",
                user_id=user_context.get("user_id"),
                user_name=user_context.get("user_name"),
                severity="info",
            )

            execute("COMMIT")

            return {"status": "success", "state": new_state}

        except Exception as e:
            execute("ROLLBACK")

            self.audit.log_user_action(
                action="governance_failure",
                description=str(e),
                user_id=user_context.get("user_id"),
                user_name=user_context.get("user_name"),
                severity="critical",
            )

            raise