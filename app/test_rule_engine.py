from core.rule_engine import FinancialRuleEngine

# Initialize engine
engine = FinancialRuleEngine({
    'amount_tolerance': 0.01,
    'budget_warning_threshold': 0.10,
    'budget_critical_threshold': 0.20,
    'duplicate_lookback_days': 90
})

print("\n============================")
print("TEST 1: Duplicate Invoice")
print("============================")

invoice = {
    "invoice_id": "INV-101",
    "vendor_id": "VEN-001",
    "amount": 10000,
    "currency": "USD",
    "invoice_date": "2024-12-01",
    "description": "Cloud Service",
    "po_number": "PO-12345"
}

from datetime import datetime, timedelta

recent_date = (datetime.now() - timedelta(days=10)).isoformat()

historical = [{
    "invoice_id": "INV-090",
    "vendor_id": "VEN-001",
    "amount": 10000,
    "invoice_date": recent_date
}]
msa = {
    "rate_ceiling": 20000,
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "currency": "USD"
}

result = engine.validate_invoice(invoice, msa, historical)
print(result)

print("\n============================")
print("TEST 2: MSA Ceiling Violation")
print("============================")

invoice["amount"] = 50000
msa["rate_ceiling"] = 40000

result = engine.validate_invoice(invoice, msa, [])
print(result)