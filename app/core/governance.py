# app/core/governance.py

from app.database.db import execute
from .rule_engine import FinancialRuleEngine
from .workflow import FinancialWorkflowEngine
from .sla import SLAEngine
from .audit import AuditLogger


class GovernanceOrchestrator:

    def __init__(
        self,
        workflow: FinancialWorkflowEngine,
        sla: SLAEngine,
        audit: AuditLogger,
    ):
        self.workflow = workflow
        self.sla = sla
        self.audit = audit
        self.rule_engine = FinancialRuleEngine()

    # ─────────────────────────────────────────────
    # MAIN GOVERNANCE EXECUTION PIPELINE
    # ─────────────────────────────────────────────

    def execute_financial_action(
        self,
        entity_id: str,
        entity_type: str,
        action_type: str,   # edit / submit / approve
        payload: dict,
        user_context: dict,
    ):

        execute("BEGIN")

        try:

            # ─────────────────────────────
            # 1️⃣ RULE VALIDATION
            # ─────────────────────────────

            if action_type == "edit":
                rule_result = self.rule_engine.validate_financial_edit(
                    user=user_context,
                    slice_data=payload,
                    context=payload,
                )

            elif action_type == "submit":
                rule_result = self.rule_engine.validate_financial_submission(
                    user=user_context,
                    context=payload,
                )

            elif action_type == "approve":
                rule_result = self.rule_engine.validate_financial_approval(
                    user=user_context,
                    context=payload,
                )

            else:
                execute("ROLLBACK")
                return {"status": "invalid_action"}

            # If validation fails → stop
            if not rule_result.get("passed"):
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
            # 3️⃣ START / RESET SLA
            # ─────────────────────────────

            if new_state:
                self.sla.start(
                    entity_id=entity_id,
                    entity_type=entity_type,
                    state=new_state,
                    tenant_id=user_context.get("tenant_id", "default"),
                )

            # ─────────────────────────────
            # 4️⃣ AUDIT LOGGING
            # ─────────────────────────────

            self.audit.log_user_action(
                action="governance_action_executed",
                description=f"{entity_type}:{entity_id} moved to {new_state}",
                user_id=user_context.get("user_id"),
                user_name=user_context.get("user_name"),
                severity="info",
            )

            execute("COMMIT")

            return {
                "status": "success",
                "state": new_state,
                "validation": rule_result,
            }

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