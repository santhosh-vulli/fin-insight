# core/workflow.py

from enum import Enum
from typing import Dict, Optional, Any, List
from core.audit import AuditLogger
from core.db import execute
import uuid
import json


# ─────────────────────────────────────────────
# ENUMS (Single Source of Truth)
# ─────────────────────────────────────────────

class WorkflowState(str, Enum):
    DRAFT = "draft"
    UNDER_REVIEW = "under_review"
    ESCALATED = "escalated"
    APPROVED = "approved"
    REJECTED = "rejected"


class WorkflowAction(str, Enum):
    APPROVE = "approve"
    REVIEW = "review"
    REJECT = "reject"
    ESCALATE = "escalate"


class EntityType(str, Enum):
    INVOICE = "invoice"
    FPA_VERSION = "fpa_version"
    BUDGET = "budget"


# ─────────────────────────────────────────────
# ENTERPRISE FINANCIAL WORKFLOW ENGINE
# ─────────────────────────────────────────────

class FinancialWorkflowEngine:

    # Deterministic state transitions
    TRANSITIONS: Dict[str, Dict[str, str]] = {
        WorkflowState.DRAFT.value: {
            WorkflowAction.REVIEW.value: WorkflowState.UNDER_REVIEW.value,
            WorkflowAction.REJECT.value: WorkflowState.REJECTED.value,
        },
        WorkflowState.UNDER_REVIEW.value: {
            WorkflowAction.APPROVE.value: WorkflowState.UNDER_REVIEW.value,
            WorkflowAction.REJECT.value: WorkflowState.REJECTED.value,
            WorkflowAction.ESCALATE.value: WorkflowState.ESCALATED.value,
        },
        WorkflowState.ESCALATED.value: {
            WorkflowAction.APPROVE.value: WorkflowState.UNDER_REVIEW.value,
            WorkflowAction.REJECT.value: WorkflowState.REJECTED.value,
        },
    }

    # Static hierarchy order reference (NOT fixed chain)
    HIERARCHY_ORDER = ["manager", "fpna_head", "cfo"]

    def __init__(self, audit_logger: AuditLogger):
        self.audit = audit_logger
        

    # ─────────────────────────────────────────────
    # INITIALIZATION (Now Dynamic)
    # ─────────────────────────────────────────────

    def initialize(self, entity_id, entity_type, tenant_id, context, user_id, user_name):

        existing = execute(
            "SELECT * FROM workflow_instances WHERE entity_id = %s",
            (entity_id,),
            fetch=True
        )

        if existing:
            return existing[0]["state"]

        chain = self._resolve_approval_chain(context)

        execute(
            """
            INSERT INTO workflow_instances
            (entity_id, entity_type, tenant_id, state, approval_level, approval_chain, context)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                entity_id,
                entity_type,
                tenant_id,
                WorkflowState.DRAFT.value,
                0,
                json.dumps(chain),
                json.dumps(context)
            )
        )

        return WorkflowState.DRAFT.value
    # ─────────────────────────────────────────────
    # DYNAMIC APPROVAL MATRIX
    # ─────────────────────────────────────────────

    def _resolve_approval_chain(self, context: Dict[str, Any]) -> List[str]:

        amount = context.get("amount", 0)
        variance = context.get("variance_pct", 0)
        risk = context.get("cost_center_risk", "low")

        chain: List[str] = []

        # Monetary Threshold Logic
        if amount >= 10_000_000:
            chain = ["cfo"]
        elif amount >= 1_000_000:
            chain = ["manager", "fpna_head"]
        else:
            chain = ["manager"]

        # Cost Center Risk Adjustment
        if risk == "high":
            if "fpna_head" not in chain:
                chain.append("fpna_head")
            if amount >= 5_000_000 and "cfo" not in chain:
                chain.append("cfo")

        # Variance Adjustment
        if variance >= 0.30:
            chain = ["cfo"]
        elif variance >= 0.20:
            if "fpna_head" not in chain:
                chain.append("fpna_head")

        # Maintain hierarchy order
        chain = sorted(set(chain), key=lambda x: self.HIERARCHY_ORDER.index(x))

        return chain

    # ─────────────────────────────────────────────
    # CORE SAFE TRANSITION
    # ─────────────────────────────────────────────

    def _safe_transition(
        self,
        entity_id: str,
        current_state: str,
        action: str,
        user_role: str,
        user_id: str,
        user_name: str,
        reason: str,
    ) -> str:

        allowed = self.TRANSITIONS.get(current_state, {})

        if action not in allowed:
            self.audit.log_user_action(
                action="workflow_invalid_transition",
                description=f"Invalid transition: {current_state} → {action}",
                user_id=user_id,
                user_name=user_name,
                severity="critical",
            )
            return current_state

        if action == WorkflowAction.APPROVE.value:
            return self._handle_approval(
                entity_id,
                current_state,
                user_role,
                user_id,
                user_name,
                reason,
            )

        new_state = allowed[action]
        execute(
        
            """
            UPDATE workflow_instances
            SET state = %s,
                updated_at = NOW()
            WHERE entity_id = %s
           """,
        (new_state, entity_id)
   )

        self.audit.log_user_action(
            action="workflow_transition",
            description=f"{current_state} → {new_state} ({reason})",
            user_id=user_id,
            user_name=user_name,
            severity="info",
        )

        return new_state

    # ─────────────────────────────────────────────
    # APPROVAL ENGINE (Now Per-Entity Chain)
    # ─────────────────────────────────────────────

    def _handle_approval(
        self,
        entity_id: str,
        current_state: str,
        user_role: str,
        user_id: str,
        user_name: str,
        reason: str,
    ) -> str:

       records = execute(
            "SELECT * FROM workflow_instances WHERE entity_id = %s",
            (entity_id,),
            fetch=True
        )
    
        if not records:
            return current_state  # safe fallback

        record = records[0]

        level = record["approval_level"]
        chain = json.loads(record["approval_chain"])

        if level >= len(chain):
            return WorkflowState.APPROVED.value

        required_role = chain[level]

        if user_role != required_role:
            self.audit.log_user_action(
                action="workflow_unauthorized_approval",
                description=f"{user_role} attempted L{level+1} approval (requires {required_role})",
                user_id=user_id,
                user_name=user_name,
                severity="critical",
            )
            return current_state

        level += 1

    # Final approval
        if level >= len(chain):
            new_state = WorkflowState.APPROVED.value
        else:
            new_state = WorkflowState.UNDER_REVIEW.value

        execute(
            """
            UPDATE workflow_instances
            SET state = %s,
                approval_level = %s,
                updated_at = NOW()
            WHERE entity_id = %s
           """,
            (new_state, level, entity_id)
        )

        

        # Continue review
        self.audit.log_user_action(
            action="workflow_level_approval",
            description=f"L{level} approval by {user_role} ({reason})",
            user_id=user_id,
            user_name=user_name,
            severity="info",
        )

        return new_state

    # ─────────────────────────────────────────────
    # POST VALIDATION ROUTING
    # ─────────────────────────────────────────────

    def after_validation(
        self,
        entity_id: str,
        current_state: str,
        validation_result: dict,
        user_role: str,
        user_id: str,
        user_name: str,
    ) -> str:

        severity = (validation_result.get("severity") or "").lower()

        if severity == "critical":
            action = WorkflowAction.ESCALATE.value
        elif severity in ("high", "medium", "low"):
            action = WorkflowAction.REVIEW.value
        else:
            action = WorkflowAction.APPROVE.value

        return self._safe_transition(
            entity_id,
            current_state,
            action,
            user_role,
            user_id,
            user_name,
            "Post validation routing",
        )

    # ─────────────────────────────────────────────
    # HUMAN DECISION
    # ─────────────────────────────────────────────

    def human_decision(
        self,
        entity_id: str,
        current_state: str,
        decision: str,
        reason: str,
        user_role: str,
        user_id: str,
        user_name: str,
    ) -> str:

        return self._safe_transition(
            entity_id,
            current_state,
            decision.lower(),
            user_role,
            user_id,
            user_name,
            reason,
        )

    # ─────────────────────────────────────────────
    # ESCALATION
    # ─────────────────────────────────────────────

    def escalate(
        self,
        entity_id: str,
        current_state: str,
        user_role: str,
        user_id: str,
        user_name: str,
        reason: str,
    ) -> str:

        return self._safe_transition(
            entity_id,
            current_state,
            WorkflowAction.ESCALATE.value,
            user_role,
            user_id,
            user_name,
            reason,
        )