from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime

from app.database.db import execute


router = APIRouter()


# ─────────────────────────────────────────────
# RESPONSE MODELS
# ─────────────────────────────────────────────

class VersionResponse(BaseModel):
    id: str
    version_number: int
    status: str


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
# LIST VERSIONS
# ─────────────────────────────────────────────

@router.get("/versions", response_model=List[VersionResponse])
def list_versions(scenario_code: str):

    scenario_row = execute("""
        SELECT id
        FROM dim_scenario
        WHERE code = %s
    """, (scenario_code,), fetch=True)

    if not scenario_row:
        raise HTTPException(status_code=404, detail="Scenario not found")

    scenario_id = scenario_row[0]["id"]

    rows = execute("""
        SELECT id, version_number, status
        FROM dim_version
        WHERE scenario_id = %s
        ORDER BY version_number
    """, (scenario_id,), fetch=True)

    return rows


# ─────────────────────────────────────────────
# TRANSITION VERSION
# ─────────────────────────────────────────────

@router.post("/versions/{version_id}/transition",
             response_model=TransitionResponse)
def transition_version(version_id: str, new_status: str):

    row = execute("""
        SELECT id, status
        FROM dim_version
        WHERE id = %s
    """, (version_id,), fetch=True)

    if not row:
        raise HTTPException(status_code=404, detail="Version not found")

    current_status = row[0]["status"]

    engine = VersionEngine()

    try:
        engine.validate_transition(current_status, new_status)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    update_fields = engine.apply_transition_metadata(row[0], new_status)

    set_clause = ", ".join([f"{k} = %s" for k in update_fields.keys()])
    values = list(update_fields.values())
    values.append(version_id)

    execute(f"""
        UPDATE dim_version
        SET {set_clause}
        WHERE id = %s
    """, tuple(values))

    return {
        "message": "Version transitioned successfully",
        "from_status": current_status,
        "to_status": new_status
    }


# ─────────────────────────────────────────────
# CLONE VERSION
# ─────────────────────────────────────────────

@router.post("/versions/{version_id}/clone",
             response_model=CloneResponse)
def clone_version(version_id: str):

    row = execute("""
        SELECT id, scenario_id, version_number
        FROM dim_version
        WHERE id = %s
    """, (version_id,), fetch=True)

    if not row:
        raise HTTPException(status_code=404, detail="Source version not found")

    scenario_id = row[0]["scenario_id"]

    max_row = execute("""
        SELECT MAX(version_number) AS max_version
        FROM dim_version
        WHERE scenario_id = %s
    """, (scenario_id,), fetch=True)

    next_version_number = (max_row[0]["max_version"] or 0) + 1

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