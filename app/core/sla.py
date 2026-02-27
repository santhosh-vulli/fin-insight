# core/sla.py

from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from app.core.workflow import WorkflowState, WorkflowAction
from app.core.audit import AuditLogger
from app.database.db import execute


class SLAEngine:
    """
    Enterprise Financial SLA Engine (DB-Driven Policy)
    --------------------------------------------------
    - SLA policies stored in DB (sla_policy_matrix)
    - Persistent
    - Restart-safe
    - Multi-tenant
    - Concurrency-safe
    - Fully enterprise governed
    """

    def __init__(self, workflow_engine, audit_logger: AuditLogger):
        self.workflow = workflow_engine
        self.audit = audit_logger

    # ─────────────────────────────────────────────
    # SLA CREATION (Policy from DB)
    # ─────────────────────────────────────────────

    def start(
        self,
        entity_id: str,
        entity_type: str,
        state: str,
        tenant_id: str = "default",
    ) -> None:

        policy = self._get_policy_from_db(tenant_id, state)
        if not policy:
            return

        now = datetime.utcnow()
        due = now + timedelta(hours=policy["hours"])

        execute(
            """
            INSERT INTO public.sla_instances (
                tenant_id,
                entity_type,
                entity_id,
                state,
                due_at,
                action_on_breach,
                breached,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,FALSE,NOW(),NOW())
            """,
            (
                tenant_id,
                entity_type,
                entity_id,
                state,
                due,
                policy["action_on_breach"],
            ),
        )

        self.audit.log_user_action(
            action="sla_started",
            description=(
                f"SLA started for {entity_type}:{entity_id} "
                f"(tenant={tenant_id}, state={state}, due={due.isoformat()})"
            ),
            user_id="system",
            user_name="SLA Engine",
            severity="info",
        )

    # ─────────────────────────────────────────────
    # SLA STOP
    # ─────────────────────────────────────────────

    def stop(self, entity_id: str) -> None:

        execute(
            """
            DELETE FROM public.sla_instances
            WHERE entity_id = %s
            AND breached = FALSE
            """,
            (entity_id,),
        )

        self.audit.log_user_action(
            action="sla_stopped",
            description=f"SLA stopped for entity {entity_id}",
            user_id="system",
            user_name="SLA Engine",
            severity="info",
        )

    # ─────────────────────────────────────────────
    # BREACH PROCESSOR
    # ─────────────────────────────────────────────

    def process_breaches(self) -> None:

        rows = execute(
            """
            SELECT id
            FROM public.sla_instances
            WHERE breached = FALSE
            AND due_at <= NOW()
            FOR UPDATE SKIP LOCKED
            """,
            fetch=True,
        )

        for row in rows:
            self._handle_breach(row["id"])

    # ─────────────────────────────────────────────
    # BREACH HANDLER
    # ─────────────────────────────────────────────

    def _handle_breach(self, sla_id: str) -> None:

        execute("BEGIN")

        try:
            sla = execute(
                """
                SELECT *
                FROM public.sla_instances
                WHERE id = %s
                FOR UPDATE
                """,
                (sla_id,),
                fetchone=True,
            )

            if not sla or sla["breached"]:
                execute("ROLLBACK")
                return

            entity_id = str(sla["entity_id"])
            entity_type = sla["entity_type"]
            action = sla["action_on_breach"]

            wf_meta = self.workflow.get_metadata(entity_id)
            if not wf_meta:
                execute("ROLLBACK")
                return

            current_state = wf_meta.get("state")
            current_level = wf_meta.get("approval_level", 0)

            # ─────────────────────────────
            # Execute Action
            # ─────────────────────────────

            if action == "advance_level":
                new_state = self.workflow.force_advance_level(entity_id)

                self.audit.log_user_action(
                    action="sla_level_escalation",
                    description=(
                        f"SLA escalated approval level "
                        f"from L{current_level} "
                        f"for {entity_type}:{entity_id}"
                    ),
                    user_id="system",
                    user_name="SLA Engine",
                    severity="error",
                )

            else:
                new_state = self._execute_action(
                    entity_id,
                    current_state,
                    action,
                )

            # ─────────────────────────────
            # Mark breached
            # ─────────────────────────────

            execute(
                """
                UPDATE public.sla_instances
                SET breached = TRUE,
                    breached_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (sla_id,),
            )

            execute("COMMIT")

        except Exception as e:
            execute("ROLLBACK")

            self.audit.log_user_action(
                action="sla_execution_error",
                description=str(e),
                user_id="system",
                user_name="SLA Engine",
                severity="critical",
            )

    # ─────────────────────────────────────────────
    # WORKFLOW ACTION EXECUTION
    # ─────────────────────────────────────────────

    def _execute_action(
        self,
        entity_id: str,
        current_state: str,
        action: str,
    ) -> Optional[str]:

        if action == WorkflowAction.ESCALATE.value:
            return self.workflow.escalate(
                entity_id=entity_id,
                current_state=current_state,
                user_role="system",
                user_id="system",
                user_name="SLA Engine",
                reason="SLA auto-escalation",
            )

        elif action == WorkflowAction.APPROVE.value:
            return self.workflow.human_decision(
                entity_id=entity_id,
                current_state=current_state,
                decision=WorkflowAction.APPROVE.value,
                reason="SLA auto-approval",
                user_role="system",
                user_id="system",
                user_name="SLA Engine",
            )

        elif action == WorkflowAction.REJECT.value:
            return self.workflow.human_decision(
                entity_id=entity_id,
                current_state=current_state,
                decision=WorkflowAction.REJECT.value,
                reason="SLA auto-rejection",
                user_role="system",
                user_id="system",
                user_name="SLA Engine",
            )

        return current_state

    # ─────────────────────────────────────────────
    # POLICY FROM DATABASE
    # ─────────────────────────────────────────────

    def _get_policy_from_db(
        self,
        tenant_id: str,
        state: str,
    ) -> Optional[Dict[str, Any]]:

        row = execute(
            """
            SELECT hours, action_on_breach
            FROM public.sla_policy_matrix
            WHERE tenant_id = %s
            AND state = %s
            """,
            (tenant_id, state),
            fetchone=True,
        )

        if not row:
            return None

        return {
            "hours": row["hours"],
            "action_on_breach": row["action_on_breach"],
        }