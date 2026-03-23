import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as workbench_router
from app.api.version_routes import router as version_router
from app.api.ingest_router import router as ingest_router
from app.api.chat_router import router as chat_router

app = FastAPI(
    title="FinsightAI API",
    version="1.0.0",
    description="FP&A Workbench — governed financial planning backend",
)

# ── CORS ─────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "http://localhost:3000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(workbench_router)   # /workbench/load  /workbench/update
app.include_router(version_router)     # /versions  /versions/{id}/transition  /versions/{id}/clone
app.include_router(ingest_router)      # /ingest/upload  /ingest/promote/{id}
app.include_router(chat_router)        # /chat/message  /chat/starters


@app.get("/health")
def health():
    return {"status": "ok"}