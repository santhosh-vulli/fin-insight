from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from enum import Enum
from decimal import Decimal
import re


class Severity(Enum):
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"


class RuleViolation:
    def __init__(self, rule_id, rule_name, severity, description,
                 field, expected_value, actual_value, remediation):
        self.rule_id        = rule_id
        self.rule_name      = rule_name
        self.severity       = severity
        self.description    = description
        self.field          = field
        self.expected_value = expected_value
        self.actual_value   = actual_value
        self.remediation    = remediation
        self.timestamp      = datetime.now().isoformat()

    def to_dict(self):
        return {
            "rule_id":        self.rule_id,
            "rule_name":      self.rule_name,
            "severity":       self.severity.value,
            "description":    self.description,
            "field":          self.field,
            "expected_value": str(self.expected_value),
            "actual_value":   str(self.actual_value),
            "remediation":    self.remediation,
            "timestamp":      self.timestamp,
        }


class FinancialRuleEngine:

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.amount_tolerance       = Decimal(str(self.config.get("amount_tolerance", 0.01)))
        self.budget_warning         = Decimal(str(self.config.get("budget_warning_threshold", 0.10)))
        self.budget_critical        = Decimal(str(self.config.get("budget_critical_threshold", 0.20)))
        self.duplicate_lookback     = self.config.get("duplicate_lookback_days", 90)
        # FIX F-002: date proximity window — invoices >N days apart are NOT duplicates
        self.duplicate_date_window  = self.config.get("duplicate_date_window_days", 7)

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC VALIDATORS
    # ─────────────────────────────────────────────────────────────────────────

    def validate_invoice(
        self,
        invoice: Dict[str, Any],
        msa:     Dict[str, Any],
        historical_invoices: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:

        violations: List[RuleViolation] = []

        # Duplicate detection
        if historical_invoices:
            v = self._check_duplicate_invoice(invoice, historical_invoices)
            if v: violations.append(v)

        # MSA checks (order matters — vendor match before ceiling/date)
        for rule in [
            self._check_msa_vendor_match,   # FL-001 new
            self._check_msa_rate_ceiling,   # F-001 updated
            self._check_msa_date_range,     # F-004 updated
            self._check_currency_match,
        ]:
            result = rule(invoice, msa)
            if result: violations.append(result)

        # Invoice field checks
        violations.extend(self._check_required_fields(invoice))

        po = self._check_po_format(invoice)
        if po: violations.append(po)

        # Amount integrity (zero/negative before spike)
        sign_v = self._check_invoice_amount_sign(invoice)
        if sign_v: violations.append(sign_v)

        # Spike / reasonableness (includes no-baseline advisory)
        violations.extend(self._check_amount_reasonableness(invoice, historical_invoices))

        action   = self._determine_action(violations)
        severity = self._get_max_severity(violations)

        return {
            "passed":          len(violations) == 0,
            "violations":      [v.to_dict() for v in violations],
            "severity":        severity.value if severity else None,
            "action_required": action,
            "invoice_id":      invoice.get("invoice_id"),
            "vendor_id":       invoice.get("vendor_id"),
        }

    def validate_budget(
        self,
        expense:      Dict[str, Any],
        budget:       Dict[str, Any],
        period_spend: Decimal,
    ) -> Dict[str, Any]:

        violations: List[RuleViolation] = []

        v1 = self._check_budget_overrun(
            Decimal(str(expense.get("amount", 0))),
            Decimal(str(budget.get("allocated", 0))),
            Decimal(str(period_spend)),
        )
        if v1: violations.append(v1)

        v2 = self._check_department_authorization(expense, budget)
        if v2: violations.append(v2)

        action   = self._determine_action(violations)
        severity = self._get_max_severity(violations)

        return {
            "passed":          len(violations) == 0,
            "violations":      [v.to_dict() for v in violations],
            "severity":        severity.value if severity else None,
            "action_required": action,
            "expense_id":      expense.get("expense_id"),
        }

    def validate_vendor(
        self,
        vendor:           Dict[str, Any],
        approved_vendors: List[str],
    ) -> Dict[str, Any]:

        violations: List[RuleViolation] = []

        v1 = self._check_vendor_approval(vendor, approved_vendors)
        if v1: violations.append(v1)

        v2 = self._check_vendor_status(vendor)
        if v2: violations.append(v2)

        action   = self._determine_action(violations)
        severity = self._get_max_severity(violations)

        return {
            "passed":          len(violations) == 0,
            "violations":      [v.to_dict() for v in violations],
            "severity":        severity.value if severity else None,
            "action_required": action,
            "vendor_id":       vendor.get("vendor_id"),
        }
    def validate_financial_edit(self, user, slice_data, context):
        violations = []

        v1 = self._check_version_lock(context)
        if v1: violations.append(v1)

        v2 = self._check_period_lock(context)
        if v2: violations.append(v2)

        v3 = self._check_user_scope(user, slice_data)
        if v3: violations.append(v3)

        v4 = self._check_lifecycle_edit(context)
        if v4: violations.append(v4)

        v5 = self._check_forecast_threshold(slice_data)
        if v5: violations.append(v5)

        action   = self._determine_action(violations)
        severity = self._get_max_severity(violations)

        return {
            "passed": len(violations) == 0,
            "violations": [v.to_dict() for v in violations],
            "severity": severity.value if severity else None,
            "action_required": action,
       }


    def validate_financial_submission(self, user, context):
        violations = []

        if context.get("version_status") != "draft":
            violations.append(RuleViolation(
                "GOV-100", "Invalid Submission State", Severity.HIGH,
                "Only draft versions can be submitted",
                "version_status", "draft",
                context.get("version_status"),
                "Revert version to draft before submitting",
            ))

        if user.get("role") not in ["analyst", "manager"]:
            violations.append(RuleViolation(
                "GOV-101", "Unauthorized Submission", Severity.CRITICAL,
                "User not permitted to submit",
                "role", "analyst/manager",
                user.get("role"),
                "Escalate to authorized user",
            ))

        action   = self._determine_action(violations)
        severity = self._get_max_severity(violations)

        return {
            "passed": len(violations) == 0,
            "violations": [v.to_dict() for v in violations],
            "severity": severity.value if severity else None,
            "action_required": action,
        }


    def validate_financial_approval(self, user, context):
        violations = []

        if context.get("version_status") not in ["submitted", "under_review"]:
            violations.append(RuleViolation(
                "GOV-200", "Invalid Approval State", Severity.HIGH,
                "Version must be submitted before approval",
                "version_status", "submitted",
                context.get("version_status"),
                "Submit version before approval",
            ))

        if user.get("role") not in ["manager", "fpna_head", "cfo"]:
            violations.append(RuleViolation(
                "GOV-201", "Unauthorized Approval", Severity.CRITICAL,
                "User not authorized to approve",
                "role", "manager/fpna_head/cfo",
                user.get("role"),
                "Escalate to authorized approver",
            ))

        action   = self._determine_action(violations)
        severity = self._get_max_severity(violations)

        return {
            "passed": len(violations) == 0,
            "violations": [v.to_dict() for v in violations],
            "severity": severity.value if severity else None,
            "action_required": action,
        }
    # ─────────────────────────────────────────────────────────────────────────
    # RULE IMPLEMENTATIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _check_duplicate_invoice(self, invoice, historical):
        """
        FIX F-002: Duplicate now requires BOTH amount similarity AND date proximity
        (within duplicate_date_window_days).  Monthly retainers with the same amount
        but different invoice dates are no longer flagged as duplicates.
        """
        cutoff = datetime.now() - timedelta(days=self.duplicate_lookback)

        try:
            inv_date   = datetime.fromisoformat(str(invoice.get("invoice_date")))
            inv_amount = Decimal(str(invoice.get("amount", 0)))
        except Exception:
            return None  # malformed invoice — required-field checks will catch it

        for h in historical:
            try:
                if h.get("vendor_id") != invoice.get("vendor_id"):
                    continue
                hist_date = datetime.fromisoformat(str(h.get("invoice_date")))
                if hist_date <= cutoff:
                    continue
                hist_amount = Decimal(str(h.get("amount", 0)))
            except Exception:
                continue

            amount_match = abs(hist_amount - inv_amount) <= self.amount_tolerance
            date_proximity = abs((inv_date - hist_date).days) <= self.duplicate_date_window

            if amount_match and date_proximity:
                return RuleViolation(
                    "INV-001", "Duplicate Invoice", Severity.CRITICAL,
                    f"Duplicate: same amount within {self.duplicate_date_window}-day window",
                    "invoice_id", "Unique invoice", h.get("invoice_id"),
                    "REJECT duplicate — verify with vendor",
                )
        return None

    def _check_msa_vendor_match(self, invoice, msa):
        """FIX FL-001: Invoice vendor_id must match MSA vendor_id."""
        inv_vendor = invoice.get("vendor_id")
        msa_vendor = msa.get("vendor_id")
        if inv_vendor and msa_vendor and inv_vendor != msa_vendor:
            return RuleViolation(
                "MSA-004", "MSA Vendor Mismatch", Severity.CRITICAL,
                "Invoice vendor does not match MSA vendor",
                "vendor_id", msa_vendor, inv_vendor,
                "REJECT — submit against the correct MSA",
            )
        return None

    def _check_msa_rate_ceiling(self, invoice, msa):
        """
        FIX F-001: ceiling=0 or ceiling<0 now fires MSA-003 (MEDIUM) instead of
        silently bypassing the check.
        """
        inv     = Decimal(str(invoice.get("amount", 0)))
        ceiling = Decimal(str(msa.get("rate_ceiling", 0)))

        if ceiling <= 0:
            return RuleViolation(
                "MSA-003", "Unconfigured Rate Ceiling", Severity.MEDIUM,
                f"MSA rate_ceiling is {ceiling} — ceiling check disabled",
                "rate_ceiling", "> 0", ceiling,
                "REVIEW — configure a valid ceiling in the MSA record",
            )

        if inv > ceiling:
            return RuleViolation(
                "MSA-001", "MSA Rate Ceiling Violation", Severity.CRITICAL,
                "Invoice amount exceeds MSA rate ceiling",
                "amount", ceiling, inv,
                "REJECT or renegotiate MSA ceiling",
            )
        return None

    def _check_msa_date_range(self, invoice, msa):
        """
        FIX F-004: Separates MSA config date errors (MSA-000a, HIGH) from invoice
        date errors (MSA-000b, CRITICAL).  Adds MSA-005 for inverted MSA ranges.
        """
        # 1. Validate MSA dates (config error — not the invoice's fault)
        try:
            start = datetime.fromisoformat(str(msa.get("start_date")))
            end   = datetime.fromisoformat(str(msa.get("end_date")))
        except Exception:
            return RuleViolation(
                "MSA-000a", "Invalid MSA Date Configuration", Severity.HIGH,
                "MSA start_date or end_date is not a valid ISO datetime",
                "msa.start_date / msa.end_date", "ISO format",
                f"{msa.get('start_date')} / {msa.get('end_date')}",
                "BLOCK — fix MSA record before processing invoices",
            )

        # 2. Catch inverted MSA range (schema fix should prevent this; belt-and-suspenders)
        if start >= end:
            return RuleViolation(
                "MSA-005", "Inverted MSA Date Range", Severity.HIGH,
                f"MSA start_date ({start.date()}) is not before end_date ({end.date()})",
                "msa.start_date", f"< {end.date()}", start.date(),
                "BLOCK — correct MSA date range in the contract record",
            )

        # 3. Validate invoice date (invoice error — CRITICAL)
        try:
            inv_date = datetime.fromisoformat(str(invoice.get("invoice_date")))
        except Exception:
            return RuleViolation(
                "MSA-000b", "Invalid Invoice Date", Severity.CRITICAL,
                "invoice_date is not a valid ISO datetime",
                "invoice_date", "ISO format", invoice.get("invoice_date"),
                "REJECT — correct invoice_date and resubmit",
            )

        # 4. Range check
        if not (start <= inv_date <= end):
            return RuleViolation(
                "MSA-002", "MSA Date Range Violation", Severity.HIGH,
                "Invoice date falls outside MSA validity window",
                "invoice_date", f"{start.date()} to {end.date()}", inv_date.date(),
                "REVIEW — confirm MSA is active for this period",
            )
        return None

    def _check_currency_match(self, invoice, msa):
        if (invoice.get("currency") or "").upper() != (msa.get("currency") or "").upper():
            return RuleViolation(
                "INV-002", "Currency Mismatch", Severity.MEDIUM,
                "Invoice currency does not match MSA currency",
                "currency", msa.get("currency"), invoice.get("currency"),
                "REVIEW — obtain FX approval or resubmit in correct currency",
            )
        return None

    def _check_required_fields(self, invoice):
        required = ["invoice_id", "vendor_id", "amount", "currency",
                    "invoice_date", "description"]
        violations = []
        for field in required:
            val = invoice.get(field)
            if val is None or (isinstance(val, str) and not val.strip()):
                violations.append(RuleViolation(
                    f"INV-003-{field}", "Missing Required Field", Severity.HIGH,
                    f"Required field '{field}' is missing or blank",
                    field, "Non-empty value", "Missing / blank",
                    "HOLD — provide the missing field and resubmit",
                ))
        return violations

    def _check_po_format(self, invoice):
        po = invoice.get("po_number")
        if po and not re.match(r"^PO-\d{5}$", str(po)):
            return RuleViolation(
                "INV-005", "Invalid PO Format", Severity.LOW,
                "PO number does not match required format PO-XXXXX",
                "po_number", "PO-XXXXX (5 digits)", po,
                "WARNING — verify PO number with procurement",
            )
        return None

    def _check_invoice_amount_sign(self, invoice):
        """
        FIX F-003 + F-006:
          amount == 0  → INV-007 LOW  (ghost invoice risk)
          amount <  0  → INV-009 MEDIUM  (credit note — needs separate routing)
        """
        try:
            amount = Decimal(str(invoice.get("amount", 0)))
        except Exception:
            return None  # required-field check will catch missing/invalid amount

        if amount < 0:
            return RuleViolation(
                "INV-009", "Unrouted Credit Note", Severity.MEDIUM,
                f"Negative invoice amount ({amount}) indicates a credit note",
                "amount", ">= 0", amount,
                "REVIEW — route to credit note workflow for GL treatment",
            )
        if amount == 0:
            return RuleViolation(
                "INV-007", "Zero Invoice Amount", Severity.LOW,
                "Invoice amount is zero — possible ghost/test invoice",
                "amount", "> 0", amount,
                "WARNING — confirm intentional zero-amount invoice with vendor",
            )
        return None

    def _check_amount_reasonableness(self, invoice, historical):
        """
        FIX F-005: Fires INV-008 LOW advisory when history exists but all entries
        fall outside the lookback window (no baseline to spike-check against).
        FIX F-006: spike check still runs as before for in-window history.
        Returns a LIST (may be empty, may have 1 item).
        """
        if not historical:
            return []

        cutoff = datetime.now() - timedelta(days=90)
        in_window  = []
        has_history = False

        for h in historical:
            try:
                if h.get("vendor_id") != invoice.get("vendor_id"):
                    continue
                date = datetime.fromisoformat(str(h.get("invoice_date")))
                has_history = True
                if date <= cutoff:
                    continue
                in_window.append(Decimal(str(h.get("amount", 0))))
            except Exception:
                continue

        # FIX F-005: history exists but all outside window → advisory
        if has_history and not in_window:
            return [RuleViolation(
                "INV-008", "No Recent Invoice Baseline", Severity.LOW,
                "Vendor has historical invoices but none within 90-day window; "
                "spike check skipped",
                "invoice_date", "History within 90 days", "None found",
                "INFO — review manually; consider extending lookback window",
            )]

        if not in_window:
            return []

        avg     = sum(in_window, Decimal("0")) / Decimal(len(in_window))
        current = Decimal(str(invoice.get("amount", 0)))

        if current > avg * Decimal("3"):
            return [RuleViolation(
                "INV-006", "Unusual Amount Spike", Severity.MEDIUM,
                f"Invoice ({current}) exceeds 3× vendor average ({avg:.2f})",
                "amount", avg, current,
                "REVIEW — confirm scope change or renegotiated rate with vendor",
            )]
        return []

    def _check_budget_overrun(self, amount, allocated, spent):
        if allocated <= 0:
            return RuleViolation(
                "BUD-000", "Invalid Budget Configuration", Severity.CRITICAL,
                f"Budget allocation is {allocated} — no valid budget defined",
                "allocated", "> 0", allocated,
                "BLOCK — define a valid budget before approving expenses",
            )
        new_total = spent + amount
        if new_total > allocated:
            return RuleViolation(
                "BUD-001", "Budget Overrun", Severity.CRITICAL,
                f"Expense would bring period spend to {new_total} "
                f"against budget of {allocated}",
                "amount", allocated, new_total,
                "ESCALATE — obtain CFO/budget-owner approval",
            )
        return None

    def _check_department_authorization(self, expense, budget):
        dept = expense.get("department")
        if dept not in budget.get("authorized_departments", []):
            return RuleViolation(
                "BUD-003", "Unauthorized Department", Severity.HIGH,
                f"Department '{dept}' is not authorized for this budget",
                "department", budget.get("authorized_departments"), dept,
                "HOLD — re-route to authorized department or request budget amendment",
            )
        return None

    def _check_vendor_approval(self, vendor, approved):
        if vendor.get("vendor_id") not in approved:
            return RuleViolation(
                "VEN-001", "Unapproved Vendor", Severity.CRITICAL,
                "Vendor is not on the approved vendor list",
                "vendor_id", "Approved vendor", vendor.get("vendor_id"),
                "BLOCK — complete vendor onboarding before transacting",
            )
        return None

    def _check_vendor_status(self, vendor):
        status = (vendor.get("status") or "").lower()
        if status != "active":
            severity = Severity.CRITICAL if status == "blocked" else Severity.HIGH
            return RuleViolation(
                "VEN-002", "Inactive Vendor", severity,
                f"Vendor status is '{status}' — not active",
                "status", "active", status,
                "BLOCK — resolve vendor status before processing invoices",
            )
        return None
    def _check_version_lock(self, context):
        if context.get("version_locked"):
            return RuleViolation(
                "GOV-001", "Version Locked", Severity.CRITICAL,
                "Version is locked and cannot be edited",
                "version", "unlocked", "locked",
                "Create new version to modify values",
            )
        return None


    def _check_period_lock(self, context):
        if context.get("period_locked"):
            return RuleViolation(
                "GOV-002", "Period Locked", Severity.CRITICAL,
                "Fiscal period is locked",
                "period", "open", "locked",
                "Request CFO unlock",
            )
        return None


    def _check_user_scope(self, user, slice_data):
        allowed = user.get("allowed_cost_centers", [])
        if slice_data.get("cost_center_id") not in allowed:
            return RuleViolation(
                "GOV-003", "Unauthorized Scope", Severity.CRITICAL,
                "User does not own this cost center",
                "cost_center_id", allowed,
                slice_data.get("cost_center_id"),
                "Contact admin for scope update",
            )
        return None


    def _check_lifecycle_edit(self, context):
        if context.get("version_status") != "draft":
            return RuleViolation(
                "GOV-004", "Edit Not Allowed", Severity.HIGH,
                "Only draft versions can be edited",
                "version_status", "draft",
                context.get("version_status"),
                "Create new draft version",
            )
        return None


    def _check_forecast_threshold(self, slice_data):
        old_value = Decimal(str(slice_data.get("old_value", 0)))
        new_value = Decimal(str(slice_data.get("new_value", 0)))

        if old_value == 0:
            return None

        change_ratio = abs(new_value - old_value) / old_value

        if change_ratio > Decimal("0.15"):
            return RuleViolation(
                "GOV-005", "Significant Forecast Change", Severity.HIGH,
                "Forecast change exceeds 15% threshold",
                "amount", old_value, new_value,
                "Manager review required",
            )
        return None
    # ─────────────────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _determine_action(self, violations):
        if not violations:
            return "approve"
        severity = self._get_max_severity(violations)
        if severity == Severity.CRITICAL:  return "reject"
        if severity == Severity.HIGH:      return "escalate"
        if severity == Severity.MEDIUM:    return "review"
        return "approve_with_warning"

    def _get_max_severity(self, violations):
        if not violations:
            return None
        order = {Severity.LOW: 1, Severity.MEDIUM: 2,
                 Severity.HIGH: 3, Severity.CRITICAL: 4}
        return max(violations, key=lambda v: order[v.severity]).severity