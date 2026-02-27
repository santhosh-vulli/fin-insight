# app/core/version_engine.py

from datetime import datetime


class VersionEngine:

    allowed_transitions = {
        "draft": ["locked"],
        "locked": ["submitted"],
        "submitted": ["approved", "rejected"],
        "rejected": ["draft"],
        "approved": ["archived"]
    }

    def validate_transition(self, current_status: str, new_status: str):

        if new_status not in self.allowed_transitions.get(current_status, []):
            raise ValueError(
                f"Invalid transition from '{current_status}' to '{new_status}'"
            )

    def apply_transition_metadata(self, version_row: dict, new_status: str):

        update_fields = {
            "status": new_status
        }

        now = datetime.utcnow()

        if new_status == "locked":
            update_fields["locked_at"] = now

        if new_status == "approved":
            update_fields["approved_at"] = now

        return update_fields