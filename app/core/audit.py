import json
import uuid
import copy
import hashlib
import threading
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import os


# ── Shared lock registry (AF-006) ────────────────────────────────────────────
_file_locks: Dict[str, threading.Lock] = {}
_registry_lock = threading.Lock()


def _get_file_lock(path: str) -> threading.Lock:
    """Return (creating if needed) a per-file threading.Lock."""
    with _registry_lock:
        if path not in _file_locks:
            _file_locks[path] = threading.Lock()
        return _file_locks[path]


# ── Shared logger registry (AF-008) ──────────────────────────────────────────
_logger_registry: Dict[str, "AuditLogger"] = {}
_logger_registry_lock = threading.Lock()


def get_logger(audit_file: str = "audit_trail.jsonl") -> "AuditLogger":
    """
    Return a shared AuditLogger for the given file path.
    Multiple callers using the same path share one instance — event_counter
    is consistent and no ID collisions arise from independent instances.
    """
    with _logger_registry_lock:
        if audit_file not in _logger_registry:
            _logger_registry[audit_file] = AuditLogger(audit_file)
        return _logger_registry[audit_file]


# ── JSON encoder (AF-004) ─────────────────────────────────────────────────────
class _AuditEncoder(json.JSONEncoder):
    """Safely serialises Decimal and datetime objects."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def _dumps(obj) -> str:
    return json.dumps(obj, cls=_AuditEncoder, sort_keys=True)


# ── Enums ─────────────────────────────────────────────────────────────────────
class AuditEventType(Enum):
    INVOICE_VALIDATED    = "invoice_validated"
    HUMAN_DECISION       = "human_decision"
    RULE_VIOLATION       = "rule_violation"
    BATCH_PROCESSED      = "batch_processed"
    WORKFLOW_STATE_CHANGE= "workflow_state_change"
    DATA_MODIFIED        = "data_modified"
    MSA_UPDATED          = "msa_updated"
    USER_ACTION          = "user_action"
    SYSTEM_EVENT         = "system_event"


class AuditSeverity(Enum):
    INFO     = "info"
    WARNING  = "warning"
    ERROR    = "error"
    CRITICAL = "critical"


# ── AuditEvent ────────────────────────────────────────────────────────────────
@dataclass
class AuditEvent:
    """
    Immutable audit record.
    Checksum now covers: event_id, timestamp, event_type, severity,
    user_id, user_name, entity_type, entity_id, action, details,
    previous_state, new_state.
    """
    event_id:       str
    timestamp:      str
    event_type:     str
    severity:       str          # AF-002: now checksummed
    user_id:        str
    user_name:      str          # AF-002: now checksummed
    entity_type:    str          # AF-002: now checksummed
    entity_id:      str
    action:         str
    details:        Dict[str, Any]
    previous_state: Optional[Dict] = None   # AF-001: now checksummed
    new_state:      Optional[Dict] = None   # AF-001: now checksummed
    previous_hash:  Optional[str] = None
    checksum:       Optional[str]  = None

    def __post_init__(self):
        if not self.checksum:
            self.checksum = self._calculate_checksum()

    def _calculate_checksum(self) -> str:
        """
        SHA-256 over all security-relevant fields.
        AF-001: includes previous_state and new_state.
        AF-002: includes severity, user_name, entity_type.
        AF-004: uses _dumps() so Decimal/datetime serialise safely.
        """
        data = {
            "event_id":       self.event_id,
            "timestamp":      self.timestamp,
            "event_type":     self.event_type,
            "severity":       self.severity,
            "user_id":        self.user_id,
            "user_name":      self.user_name,
            "entity_type":    self.entity_type,
            "entity_id":      self.entity_id,
            "action":         self.action,
            "details":        _dumps(self.details),
            "previous_state": _dumps(self.previous_state) if self.previous_state is not None else "null",
            "new_state":      _dumps(self.new_state)      if self.new_state      is not None else "null",
            "previous_hash": self.previous_hash,
        }
        return hashlib.sha256(_dumps(data).encode()).hexdigest()

    def verify_integrity(self) -> bool:
        original = self.checksum
        self.checksum = None
        calculated = self._calculate_checksum()
        self.checksum = original
        return original == calculated

    def to_dict(self) -> Dict:
        return asdict(self)


# ── AuditLogger ───────────────────────────────────────────────────────────────
class AuditLogger:
    """
    Append-only audit logger with:
    - UUID event IDs (AF-003)
    - Thread-safe writes (AF-006)
    - Graceful corrupt-line handling (AF-005)
    - Decimal/datetime serialisation (AF-004)
    """

    # System user IDs that are allowed without raising (AF-007)
    _SYSTEM_USER_IDS = frozenset({"system", "SYSTEM", "scheduler", "batch"})

    def __init__(self, audit_file: str = "audit_trail.jsonl"):
        self.audit_file  = os.path.abspath(audit_file)
        self._lock       = _get_file_lock(self.audit_file)
        # Ensure directory exists (AF edge: don't crash on missing parent)
        os.makedirs(os.path.dirname(self.audit_file) or ".", exist_ok=True)
        if not os.path.exists(self.audit_file):
            open(self.audit_file, 'w').close()
    def _get_last_hash(self) -> Optional[str]:
        try:
            with open(self.audit_file, "rb") as f:
                f.seek(0, os.SEEK_END)
                if f.tell() == 0:
                    return None

                f.seek(-2, os.SEEK_END)
                while f.read(1) != b"\n":
                    f.seek(-2, os.SEEK_CUR)

                last_line = f.readline().decode()
                last_event = json.loads(last_line)
                return last_event.get("checksum")
        except Exception:
            return None        

    # ── Validation helpers ────────────────────────────────────────────────────

    def _assert_user(self, user_id: str, context: str):
        """AF-007: Raise if a human-context event uses a system user ID."""
        if user_id in self._SYSTEM_USER_IDS:
            raise ValueError(
                f"{context} requires a real user_id — "
                f"'{user_id}' is a reserved system identifier. "
                "Pass the authenticated user's ID."
            )

    # ── Log methods ───────────────────────────────────────────────────────────

    def log_invoice_validation(self, invoice_id, result, user_id, user_name) -> AuditEvent:
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.INVOICE_VALIDATED.value,
            severity   = AuditSeverity.INFO.value if result.get("passed") else AuditSeverity.WARNING.value,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= "invoice",
            entity_id  = invoice_id,
            action     = "validated",
            details    = {
                "passed":           result.get("passed"),
                "action_required":  result.get("action_required"),
                "violations":       result.get("violations", []),
                "severity":         result.get("severity"),
            },
        )
        self._write_event(event)
        return event

    def log_human_decision(
        self, invoice_id, decision, reason, user_id, user_name,
        violations_addressed=None, previous_state=None, new_state=None,
    ) -> AuditEvent:
        # AF-007: human decisions must have a real user
        self._assert_user(user_id, "log_human_decision")
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.HUMAN_DECISION.value,
            severity   = AuditSeverity.INFO.value,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= "invoice",
            entity_id  = invoice_id,
            action     = decision.lower(),
            details    = {
                "decision":              decision,
                "reason":                reason,
                # AF-009: deep-copy so caller mutation doesn't affect stored event
                "violations_addressed":  copy.deepcopy(violations_addressed or []),
            },
            previous_state = previous_state,
            new_state      = new_state,
        )
        self._write_event(event)
        return event

    def log_rule_violation(self, invoice_id, violation, user_id, user_name) -> AuditEvent:
        severity_map = {
            "critical": AuditSeverity.CRITICAL,
            "high":     AuditSeverity.ERROR,
            "medium":   AuditSeverity.WARNING,
            "low":      AuditSeverity.INFO,
        }
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.RULE_VIOLATION.value,
            severity   = severity_map.get(
                            (violation.get("severity") or "").lower(),
                            AuditSeverity.WARNING,
                         ).value,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= "invoice",
            entity_id  = invoice_id,
            action     = "rule_violated",
            details    = violation,
        )
        self._write_event(event)
        return event

    def log_batch_processed(
        self, batch_id, total_invoices, auto_approved,
        needs_review, rejected, user_id, user_name,
    ) -> AuditEvent:
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.BATCH_PROCESSED.value,
            severity   = AuditSeverity.INFO.value,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= "batch",
            entity_id  = batch_id,
            action     = "processed",
            details    = {
                "total_invoices":        total_invoices,
                "auto_approved":         auto_approved,
                "needs_review":          needs_review,
                "rejected":              rejected,
                "processing_timestamp":  datetime.now().isoformat(),
            },
        )
        self._write_event(event)
        return event

    def log_workflow_state_change(
        self, entity_type, entity_id, from_state, to_state,
        user_id, user_name, reason="",
    ) -> AuditEvent:
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.WORKFLOW_STATE_CHANGE.value,
            severity   = AuditSeverity.INFO.value,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= entity_type,
            entity_id  = entity_id,
            action     = "state_changed",
            details    = {
                "from_state":            from_state,
                "to_state":              to_state,
                "reason":                reason,
                "transition_timestamp":  datetime.now().isoformat(),
            },
        )
        self._write_event(event)
        return event

    def log_data_modification(
        self, entity_type, entity_id, field, old_value, new_value,
        user_id, user_name, reason="",
    ) -> AuditEvent:
        # AF-007: data modifications must have a real user
        self._assert_user(user_id, "log_data_modification")
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.DATA_MODIFIED.value,
            severity   = AuditSeverity.WARNING.value,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= entity_type,
            entity_id  = entity_id,
            action     = "modified",
            details    = {
                "field":                    field,
                "old_value":                str(old_value),
                "new_value":                str(new_value),
                "reason":                   reason,
                "modification_timestamp":   datetime.now().isoformat(),
            },
        )
        self._write_event(event)
        return event

    def log_user_action(
        self, action, description, user_id, user_name,
        severity="info", entity_type="system", entity_id="SYSTEM",
    ) -> AuditEvent:
        """
        AF-010: entity_type and entity_id are now explicit parameters.
        Default entity_id is 'SYSTEM' (not 'N/A') to avoid collision with
        get_events_by_invoice queries.
        """
        event = AuditEvent(
            event_id   = self._generate_event_id(),
            timestamp  = datetime.now().isoformat(),
            event_type = AuditEventType.USER_ACTION.value,
            severity   = severity,
            user_id    = user_id,
            user_name  = user_name,
            entity_type= entity_type,
            entity_id  = entity_id,
            action     = action,
            details    = {
                "description":      description,
                "action_timestamp": datetime.now().isoformat(),
            },
        )
        self._write_event(event)
        return event

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _generate_event_id() -> str:
        """AF-003: UUID4 — globally unique, no timestamp collision."""
        return f"EVT-{uuid.uuid4().hex[:16].upper()}"

    def _write_event(self, event: AuditEvent):
        """
        Ledger-aware write.
        Must set previous_hash BEFORE calculating checksum.
        """
        with self._lock:
            
            event.previous_hash = self._get_last_hash()
          
            event.checksum = event._calculate_checksum()
            
            line = json.dumps(event.to_dict(), cls=_AuditEncoder) + "\n"
            with open(self.audit_file, 'a') as f:
                f.write(line)

    def _read_events(self) -> tuple[List[AuditEvent], List[dict]]:
        """
        AF-005: Reads all lines, skipping malformed JSON gracefully.
        Returns (valid_events, corrupt_line_reports).
        """
        events  = []
        corrupt = []
        with open(self.audit_file, 'r') as f:
            for line_no, line in enumerate(f, 1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    event_dict = json.loads(stripped)
                    events.append(AuditEvent(**event_dict))
                except (json.JSONDecodeError, TypeError, Exception) as e:
                    corrupt.append({
                        "line_number": line_no,
                        "error":       str(e),
                        "raw_snippet": stripped[:80],
                    })
        return events, corrupt

    # ── Query methods ─────────────────────────────────────────────────────────

    def get_events_by_invoice(self, invoice_id: str) -> List[AuditEvent]:
        """
        AF-010: Filters by entity_type='invoice' AND entity_id match,
        so user_action events with entity_id='SYSTEM' are never returned.
        """
        events, _ = self._read_events()
        return [
            e for e in events
            if e.entity_id == invoice_id and e.entity_type == "invoice"
        ]

    def get_events_by_user(self, user_id: str) -> List[AuditEvent]:
        events, _ = self._read_events()
        return [e for e in events if e.user_id == user_id]

    def get_events_by_type(self, event_type: AuditEventType) -> List[AuditEvent]:
        events, _ = self._read_events()
        return [e for e in events if e.event_type == event_type.value]

    def get_events_by_date_range(self, start_date: str, end_date: str) -> List[AuditEvent]:
        events, _ = self._read_events()
        return [e for e in events if start_date <= e.timestamp <= end_date]

    def get_human_decisions(self) -> List[AuditEvent]:
        return self.get_events_by_type(AuditEventType.HUMAN_DECISION)

    def get_rule_violations(self, severity: str = None) -> List[AuditEvent]:
        violations = self.get_events_by_type(AuditEventType.RULE_VIOLATION)
        if severity:
            violations = [v for v in violations if v.severity == severity]
        return violations

    # ── Report generation ─────────────────────────────────────────────────────

    def generate_audit_report(
        self, start_date: str, end_date: str, report_type: str = "full",
    ) -> Dict:
        valid_types = {"full", "summary", "violations_only", "decisions_only"}
        if report_type not in valid_types:
            raise ValueError(
                f"Unknown report_type '{report_type}'. "
                f"Valid options: {sorted(valid_types)}"
            )
        all_events, corrupt = self._read_events()
        events = [e for e in all_events if start_date <= e.timestamp <= end_date]

        report: Dict[str, Any] = {
            "report_generated":  datetime.now().isoformat(),
            "period_start":      start_date,
            "period_end":        end_date,
            "total_events":      len(events),
            "event_types":       {},
            "severity_breakdown":{},
            "user_activity":     {},
            "corrupt_lines":     corrupt,   # AF-005: surfaced in report
            "events":            [],
        }

        for event in events:
            report["event_types"][event.event_type] = (
                report["event_types"].get(event.event_type, 0) + 1
            )
            report["severity_breakdown"][event.severity] = (
                report["severity_breakdown"].get(event.severity, 0) + 1
            )
            report["user_activity"][event.user_name] = (
                report["user_activity"].get(event.user_name, 0) + 1
            )
            if report_type == "full":
                report["events"].append(event.to_dict())
            elif report_type == "violations_only":
                if event.event_type == AuditEventType.RULE_VIOLATION.value:
                    report["events"].append(event.to_dict())
            elif report_type == "decisions_only":
                if event.event_type == AuditEventType.HUMAN_DECISION.value:
                    report["events"].append(event.to_dict())
            # "summary" → events list stays empty

        return report

    def generate_invoice_audit_trail(self, invoice_id: str) -> Dict:
        events, _ = self._read_events()
        # AF-010: only invoice-typed events in the trail
        inv_events = [
            e for e in events
            if e.entity_id == invoice_id and e.entity_type == "invoice"
        ]
        inv_events.sort(key=lambda e: e.timestamp)
        return {
            "invoice_id":       invoice_id,
            "report_generated": datetime.now().isoformat(),
            "total_events":     len(inv_events),
            "timeline":         [
                {
                    "timestamp":  e.timestamp,
                    "event_type": e.event_type,
                    "action":     e.action,
                    "user":       e.user_name,
                    "details":    e.details,
                }
                for e in inv_events
            ],
        }

    def verify_audit_integrity(self) -> Dict:
        """AF-005 + Ledger chaining verification."""
        valid_events, corrupt = self._read_events()
        total    = len(valid_events)
        verified = 0
        tampered = []

        previous_hash = None  # 🔐 ledger chain start

        for event in valid_events:

            # 1️⃣ Check chain continuity
            if event.previous_hash != previous_hash:
                tampered.append({
                    "event_id":  event.event_id,
                    "timestamp": event.timestamp,
                    "entity_id": event.entity_id,
                    "reason":    "hash_chain_broken",
                })

            # 2️⃣ Check checksum integrity
            elif not event.verify_integrity():
                tampered.append({
                    "event_id":  event.event_id,
                    "timestamp": event.timestamp,
                    "entity_id": event.entity_id,
                    "reason":    "checksum_mismatch",
                })

            else:
                verified += 1

            # Move chain forward
            previous_hash = event.checksum

        return {
            "total_events":       total,
            "verified_events":    verified,
            "tampered_events":    len(tampered),
            "corrupt_lines":      len(corrupt),
            "integrity_check":    "PASS" if (len(tampered) == 0 and len(corrupt) == 0) else "FAIL",
            "tampered_event_ids": tampered,
            "corrupt_line_details": corrupt,
        }