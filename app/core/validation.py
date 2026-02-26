import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.invoice import InvoiceSchema, MSASchema
from core.audit import AuditLogger, get_logger
from core.workflow import InvoiceWorkflowEngine, WorkflowState
from core.sla import SLAEngine
from core.rule_engine import FinancialRuleEngine
from datetime import datetime

try:
    from pydantic import ValidationError
except ModuleNotFoundError:
    from pydantic_stub import ValidationError




class ValidationService:

    def __init__(
        self,
        config: dict | None = None,
        audit_file: str | None = None,
        audit_logger: AuditLogger | None = None,
    ):
        self.engine = FinancialRuleEngine(config)

        if audit_logger is not None:
            self.audit_logger = audit_logger
        else:
            file = audit_file or "audit_trail.jsonl"
            self.audit_logger = get_logger(file)

        # Workflow engine initialization
        self.workflow = InvoiceWorkflowEngine(self.audit_logger)
         # SLA Engine
        self.sla = SLAEngine(self.workflow, self.audit_logger)

    # ─────────────────────────────────────────────
    # PUBLIC METHODS
    # ─────────────────────────────────────────────

    def validate_invoice(
        self,
        raw_invoice: dict,
        raw_msa: dict,
        historical: list | None = None,
        user_id: str = "system",
        user_name: str = "System",
        current_state: str | None = None,
    ) -> dict:

        invoice_id = raw_invoice.get("invoice_id")

        # 1️⃣ Initialize workflow if first time
        if current_state is None:
            current_state = self.workflow.initialize(
                invoice_id=invoice_id,
                user_id=user_id,
                user_name=user_name,
            )

        # 2️⃣ Schema validation
        invoice_obj, msa_obj, schema_err = self._validate_schemas(
            raw_invoice, raw_msa, user_id, user_name,
        )

        if schema_err:
            new_state = self.workflow.human_decision(
                invoice_id=invoice_id,
                current_state=current_state,
                decision="reject",
                reason="Schema validation failure",
                user_id=user_id,
                user_name=user_name,
            )

            schema_err["workflow_state"] = new_state
            return schema_err

        # 3️⃣ Normalize
        invoice = invoice_obj.model_dump()
        msa     = msa_obj.model_dump()

        # 4️⃣ Rule engine
        result = self.engine.validate_invoice(invoice, msa, historical or [])

        # 5️⃣ Audit validation
        self.audit_logger.log_invoice_validation(
            invoice_id=invoice_id,
            result=result,
            user_id=user_id,
            user_name=user_name,
        )

        # 6️⃣ Audit violations
        for violation in result.get("violations", []):
            self.audit_logger.log_rule_violation(
                invoice_id=invoice_id,
                violation=violation,
                user_id=user_id,
                user_name=user_name,
            )

        # 7️⃣ Workflow routing
        new_state = self.workflow.after_validation(
            invoice_id=invoice_id,
            current_state=current_state,
            validation_result=result,
            user_id=user_id,
            user_name=user_name,
        )
        # 8️⃣ SLA evaluation
        self.sla.start(
            invoice_id=invoice_id,
            state=new_state,
            tenant_id="default",  #  dynamic per invoice
        )
        
        # 9️⃣ Stop SLA if terminal
        if new_state in (
           WorkflowState.APPROVED.value,
           WorkflowState.REJECTED.value,
        ):
           self.sla.stop(invoice_id)

        result["workflow_state"] = new_state

        return result


    def apply_human_decision(
        self,
        invoice_id: str,
        current_state: str,
        decision: str,
        reason: str,
        user_id: str,
        user_name: str,
    ) -> str:
        new_state = self.workflow.human_decision(
            invoice_id=invoice_id,
            current_state=current_state,
            decision=decision,
            reason=reason,
            user_id=user_id,
            user_name=user_name,
        )

        # Restart SLA for new state
        self.sla.start(
            invoice_id=invoice_id,
            state=new_state,
            tenant_id="default",
        )

        # Stop SLA if terminal
        if new_state in (
            WorkflowState.APPROVED.value,
            WorkflowState.REJECTED.value,
        ):
            self.sla.stop(invoice_id)

        return new_state


    def check_sla(self):
        """
        Trigger SLA breach scan.
        In production this will be called by scheduler.
        """
        self.sla.check_breaches()
    
    # ─────────────────────────────────────────────
    # PRIVATE METHODS
    # ─────────────────────────────────────────────

    def _validate_schemas(self, raw_invoice, raw_msa, user_id, user_name):
        violations = []
        invoice_obj = msa_obj = None

        try:
            invoice_obj = InvoiceSchema(**raw_invoice)
        except Exception as e:
            violations.append(self._schema_violation("SCH-001", "invoice", str(e)))

        try:
            msa_obj = MSASchema(**raw_msa)
        except Exception as e:
            violations.append(self._schema_violation("SCH-002", "msa", str(e)))

        if violations:
            failure_result = {
                "passed": False,
                "violations": violations,
                "severity": "critical",
                "action_required": "reject",
                "invoice_id": raw_invoice.get("invoice_id"),
                "vendor_id": raw_invoice.get("vendor_id"),
            }

            return None, None, failure_result

        return invoice_obj, msa_obj, None

    @staticmethod
    def _schema_violation(rule_id: str, scope: str, message: str) -> dict:
        return {
            "rule_id": rule_id,
            "rule_name": f"Schema Validation Error ({scope})",
            "severity": "critical",
            "description": message,
            "field": scope,
            "expected_value": "Valid schema",
            "actual_value": "Invalid input",
            "remediation": "Fix input data and resubmit",
            "timestamp": datetime.now().isoformat(),
        }