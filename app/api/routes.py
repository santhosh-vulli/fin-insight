from app.fpa.fpa_workbench_engine import FPAWorkbenchEngine

engine = FPAWorkbenchEngine()


@router.get("/workbench/load")
def load_workbench(scenario_id: str, period: str):
    return engine.load_workbench(scenario_id, period)


@router.post("/workbench/update")
def update_cell(payload: dict):
    user_context = {"tenant_id": "default"}
    return engine.update_cell(payload, user_context)


@router.get("/workbench/analytics")
def load_analytics(scenario_id: str):
    return engine.load_analytics(scenario_id)