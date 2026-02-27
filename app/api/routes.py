# app/api/routes.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from app.core.fpa.fpa_workbench_engine import FPAWorkbenchEngine


router = APIRouter()
engine = FPAWorkbenchEngine()


# ─────────────────────────────────────────────
# REQUEST MODELS
# ─────────────────────────────────────────────

class UpdatePayload(BaseModel):
    scenario_code: str
    version_number: int
    sheet: str
    data: Optional[Dict[str, Any]] = None
    start_period: Optional[str] = None
    end_period: Optional[str] = None


# ─────────────────────────────────────────────
# RESPONSE MODELS
# ─────────────────────────────────────────────

class GridCell(BaseModel):
    account_id: str
    cost_center_id: str
    value: float


class LoadWorkbenchResponse(BaseModel):
    scenario_id: str
    version_number: int
    version_status: str
    period_id: str
    grid: List[GridCell]
    analytics: Dict[str, Any]


class AnalyticsResponse(BaseModel):
    scenario_id: str
    version_number: int
    version_status: str
    insights: Dict[str, Any]


class TransitionResponse(BaseModel):
    message: str
    from_status: str
    to_status: str


# ─────────────────────────────────────────────
# LOAD WORKBENCH (VERSION AWARE)
# ─────────────────────────────────────────────

@router.get(
    "/workbench/load",
    response_model=LoadWorkbenchResponse
)
def load_workbench(
    scenario_code: str,
    version_number: int,
    period_code: str
):
    result = engine.load_workbench(
        scenario_code,
        version_number,
        period_code
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


# ─────────────────────────────────────────────
# UPDATE CELL (LIFECYCLE ENFORCED)
# ─────────────────────────────────────────────

@router.post("/workbench/update")
def update_cell(payload: UpdatePayload):

    user_context = {
        "tenant_id": "default",
        "user_id": "system"
    }

    result = engine.update_cell(payload.dict(), user_context)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


# ─────────────────────────────────────────────
# LOAD ANALYTICS (VERSION AWARE)
# ─────────────────────────────────────────────

@router.get(
    "/workbench/analytics",
    response_model=AnalyticsResponse
)
def load_analytics(
    scenario_code: str,
    version_number: int
):
    result = engine.load_analytics(
        scenario_code,
        version_number
    )

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result