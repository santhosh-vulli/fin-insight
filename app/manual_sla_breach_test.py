from datetime import datetime, timedelta

from core.audit import AuditLogger
from core.workflow import WorkflowState
from core.validation import ValidationService


def main():
    print("=== SLA BREACH SIMULATION TEST ===\n")

    # Setup validation service
    service = ValidationService(audit_file="sla_test_audit.jsonl")

    invoice_id = "INV-SLA-001"

    raw_invoice = {
        "invoice_id": invoice_id,
        "vendor_id": "VEN-001",
        "amount": "10000",
        "currency": "USD",
        "invoice_date": datetime.now().isoformat(),
        "description": "Test invoice for SLA",
    }

    raw_msa = {
        "msa_id": "MSA-001",
        "vendor_id": "VEN-001",
        "rate_ceiling": "20000",
        "start_date": (datetime.now() - timedelta(days=1)).isoformat(),
        "end_date": (datetime.now() + timedelta(days=30)).isoformat(),
        "currency": "USD",
    }

    # Step 1: Validate invoice → should go to APPROVED or UNDER_REVIEW
    print("1️⃣ Validating invoice...")
    result = service.validate_invoice(raw_invoice, raw_msa)
    state = result["workflow_state"]

    print("Initial Workflow State:", state)

    # Force it into UNDER_REVIEW manually if auto-approved
    if state == WorkflowState.APPROVED.value:
        print("Invoice auto-approved. Forcing UNDER_REVIEW for SLA test...\n")

        state = service.workflow.after_validation(
        invoice_id=invoice_id,
        current_state=WorkflowState.DRAFT.value,
        validation_result={"action_required": "review"},
        user_id="tester",
        user_name="Tester",
    )

    print("Forced Workflow State:", state)

    # Restart SLA timer manually for UNDER_REVIEW
    print("\n2️⃣ Starting SLA timer...")
    service.sla.start(
        invoice_id=invoice_id,
        state=WorkflowState.UNDER_REVIEW.value,
        tenant_id="default",
    )

    timer = service.sla.get_timer(invoice_id)
    print("SLA Due At:", timer["due_at"])

    # 🔥 Force breach by rewinding due_at time
    print("\n3️⃣ Forcing SLA breach (simulating time passage)...")
    timer["due_at"] = datetime.now() - timedelta(seconds=1)

    # Step 4: Trigger SLA check
    print("\n4️⃣ Running SLA breach check...")
    service.check_sla()

    # Step 5: Inspect updated workflow state
    print("\n5️⃣ Checking new workflow state...")

    current_state = service.workflow.get_state(invoice_id)
    print("Expected State:", WorkflowState.ESCALATED.value)
    print("Actual State:", current_state)

    print("\n=== SLA TEST COMPLETE ===")

if __name__ == "__main__":
    main()