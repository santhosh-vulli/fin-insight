# app/api/version_routes.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime

from app.database.db import execute
from app.core.version_engine import VersionEngine


router = APIRouter()


# ─────────────────────────────────────────────
# RESPONSE MODEL
# ─────────────────────────────────────────────

class TransitionResponse(BaseModel):
    message: str
    from_status: str
    to_status: str
class CloneResponse(BaseModel):
    message: str
    source_version_id: str
    new_version_id: str
    new_version_number: int

# ─────────────────────────────────────────────
# VERSION TRANSITION
# ─────────────────────────────────────────────

@router.post(
    "/versions/{version_id}/transition",
    response_model=TransitionResponse
)
def transition_version(version_id: str, new_status: str):

    # Fetch version
    row = execute("""
        SELECT id, status
        FROM dim_version
        WHERE id = %s
    """, (version_id.strip(),), fetch=True)

    if not row:
        raise HTTPException(status_code=404, detail="Version not found")

    current_status = row[0]["status"]

    engine = VersionEngine()

    # Validate transition
    try:
        engine.validate_transition(current_status, new_status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Prepare metadata updates
    update_fields = engine.apply_transition_metadata(row[0], new_status)

    set_clause = ", ".join([f"{k} = %s" for k in update_fields.keys()])
    values = list(update_fields.values())
    values.append(version_id.strip())

    try:
        execute(f"""
            UPDATE dim_version
            SET {set_clause}
            WHERE id = %s
        """, tuple(values))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to update version")

    return {
        "message": "Version transitioned successfully",
        "from_status": current_status,
        "to_status": new_status
    }
@router.post(
    "/versions/{version_id}/clone",
    response_model=CloneResponse
)
def clone_version(version_id: str):

    version_id = version_id.strip()

    # 1️⃣ Fetch source version
    row = execute("""
        SELECT id, scenario_id, version_number
        FROM dim_version
        WHERE id = %s
    """, (version_id,), fetch=True)

    if not row:
        raise HTTPException(status_code=404, detail="Source version not found")

    scenario_id = row[0]["scenario_id"]
    source_version_number = row[0]["version_number"]

    # 2️⃣ Determine next version_number
    max_row = execute("""
        SELECT MAX(version_number) AS max_version
        FROM dim_version
        WHERE scenario_id = %s
    """, (scenario_id,), fetch=True)

    next_version_number = (max_row[0]["max_version"] or 0) + 1

    # 3️⃣ Create new version (draft)
    new_version = execute("""
        INSERT INTO dim_version (
            scenario_id,
            version_number,
            status,
            parent_version_id
        )
        VALUES (%s, %s, 'draft', %s)
        RETURNING id
    """, (scenario_id, next_version_number, version_id), fetch=True)

    new_version_id = new_version[0]["id"]

    # 4️⃣ Copy fact rows
    execute("""
        INSERT INTO fact_financials (
            tenant_id,
            scenario_id,
            version_id,
            period_id,
            account_id,
            cost_center_id,
            amount
        )
        SELECT
            tenant_id,
            scenario_id,
            %s,
            period_id,
            account_id,
            cost_center_id,
            amount
        FROM fact_financials
        WHERE version_id = %s
    """, (new_version_id, version_id))

    return {
        "message": "Version cloned successfully",
        "source_version_id": version_id,
        "new_version_id": str(new_version_id),
        "new_version_number": next_version_number
    }    