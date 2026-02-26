import unittest
import tempfile
import os

from core.workflow import (
    InvoiceWorkflowEngine,
    WorkflowState,
    InvalidTransition,
)
from core.audit import AuditLogger


class TestInvoiceWorkflowEngine(unittest.TestCase):

    def setUp(self):
        # Windows-safe temporary file
        temp = tempfile.NamedTemporaryFile(delete=False)
        self.audit_file = temp.name
        temp.close()  # 🔥 critical on Windows

        self.logger = AuditLogger(self.audit_file)
        self.engine = InvoiceWorkflowEngine(self.logger)

        self.invoice_id = "INV-001"
        self.user_id = "user123"
        self.user_name = "Test User"

    def tearDown(self):
        if os.path.exists(self.audit_file):
            os.remove(self.audit_file)

    # ─────────────────────────────────────────────
    # BASIC FLOW TESTS
    # ─────────────────────────────────────────────

    def test_initialize(self):
        state = self.engine.initialize(
            self.invoice_id,
            self.user_id,
            self.user_name,
        )
        self.assertEqual(state, WorkflowState.DRAFT.value)

    def test_after_validation_auto_approve(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        result = {"action_required": "approve"}

        state = self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            result,
            self.user_id,
            self.user_name,
        )

        self.assertEqual(state, WorkflowState.APPROVED.value)

    def test_after_validation_review(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        result = {"action_required": "review"}

        state = self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            result,
            self.user_id,
            self.user_name,
        )

        self.assertEqual(state, WorkflowState.UNDER_REVIEW.value)

    def test_after_validation_reject(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        result = {"action_required": "reject"}

        state = self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            result,
            self.user_id,
            self.user_name,
        )

        self.assertEqual(state, WorkflowState.REJECTED.value)

    # ─────────────────────────────────────────────
    # HUMAN DECISION TESTS
    # ─────────────────────────────────────────────

    def test_human_approve(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        # Move to review
        self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            {"action_required": "review"},
            self.user_id,
            self.user_name,
        )

        state = self.engine.human_decision(
            self.invoice_id,
            WorkflowState.UNDER_REVIEW.value,
            "approve",
            "Looks good",
            self.user_id,
            self.user_name,
        )

        self.assertEqual(state, WorkflowState.APPROVED.value)

    def test_human_reject(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            {"action_required": "review"},
            self.user_id,
            self.user_name,
        )

        state = self.engine.human_decision(
            self.invoice_id,
            WorkflowState.UNDER_REVIEW.value,
            "reject",
            "Invalid invoice",
            self.user_id,
            self.user_name,
        )

        self.assertEqual(state, WorkflowState.REJECTED.value)

    def test_human_invalid_state(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        with self.assertRaises(InvalidTransition):
            self.engine.human_decision(
                self.invoice_id,
                WorkflowState.DRAFT.value,
                "approve",
                "Invalid state",
                self.user_id,
                self.user_name,
            )

    def test_double_approval_invalid(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        # Auto-approve
        state = self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            {"action_required": "approve"},
            self.user_id,
            self.user_name,
        )

        self.assertEqual(state, WorkflowState.APPROVED.value)

        # Try approving again → should fail
        with self.assertRaises(InvalidTransition):
            self.engine.human_decision(
                self.invoice_id,
                WorkflowState.APPROVED.value,
                "approve",
                "Already approved",
                self.user_id,
                self.user_name,
            )

    # ─────────────────────────────────────────────
    # ESCALATION TESTS
    # ─────────────────────────────────────────────

    def test_escalate_valid(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        # Move to review
        self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            {"action_required": "review"},
            self.user_id,
            self.user_name,
        )

        state = self.engine.escalate(
            self.invoice_id,
            WorkflowState.UNDER_REVIEW.value,
            self.user_id,
            self.user_name,
            "SLA breach",
        )

        self.assertEqual(state, WorkflowState.ESCALATED.value)

    def test_escalate_invalid_from_draft(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        with self.assertRaises(InvalidTransition):
            self.engine.escalate(
                self.invoice_id,
                WorkflowState.DRAFT.value,
                self.user_id,
                self.user_name,
            )

    def test_escalate_invalid_double(self):
        self.engine.initialize(self.invoice_id, self.user_id, self.user_name)

        # Move to review
        self.engine.after_validation(
            self.invoice_id,
            WorkflowState.DRAFT.value,
            {"action_required": "review"},
            self.user_id,
            self.user_name,
        )

        # Escalate once
        self.engine.escalate(
            self.invoice_id,
            WorkflowState.UNDER_REVIEW.value,
            self.user_id,
            self.user_name,
        )

        # Try escalating again → should fail
        with self.assertRaises(InvalidTransition):
            self.engine.escalate(
                self.invoice_id,
                WorkflowState.ESCALATED.value,
                self.user_id,
                self.user_name,
            )


if __name__ == "__main__":
    unittest.main()