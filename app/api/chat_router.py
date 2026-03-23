# app/api/chat_router.py

import os
import json
import asyncio
import logging
from typing import Any

from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

from google import genai

router = APIRouter(prefix="/chat", tags=["chat"])
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────

class SheetRow(BaseModel):
    account_id: str
    values: list[float | str | None]

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []
    active_sheet: str = "Plan"
    sheet_headers: list[str] = []
    sheet_data: list[list[Any]] = []
    version_status: str = "draft"


# ─────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────

def _build_system(req: ChatRequest) -> str:
    sheet_text = ""

    if req.sheet_headers:
        sheet_text += "  " + " | ".join(str(h) for h in req.sheet_headers[:8]) + "\n"
        sheet_text += "  " + "-" * 60 + "\n"

    for row in req.sheet_data[:20]:
        if any(v not in (None, "", 0) for v in row):
            sheet_text += "  " + " | ".join(
                "" if v is None or v == "" else
                f"{v:,.0f}" if isinstance(v, (int, float)) else str(v)
                for v in row[:8]
            ) + "\n"

    return f"""
You are FinsightAI, an expert FP&A analyst.

ACTIVE SHEET: {req.active_sheet}
VERSION: {req.version_status}

DATA:
{sheet_text if sheet_text else "No data"}

INSTRUCTIONS:
- Answer using ONLY this data
- Be concise (3–4 sentences)
- Use numbers from data
- Format numbers with commas
- If not found, say "Not available in current sheet"
"""


# ─────────────────────────────────────────────
# STARTERS
# ─────────────────────────────────────────────

STARTERS_BY_SHEET = {
    "Plan": [
        "What's EBITDA trend?",
        "Which month has highest revenue?",
        "Is COGS improving?",
    ],
    "Forecast": [
        "How does H2 compare to H1?",
        "What is growth rate?",
    ],
}

DEFAULT_STARTERS = [
    "Summarise financials",
    "Any risks?",
]


@router.get("/starters")
def get_starters(sheet: str = "Plan"):
    return {
        "starters": STARTERS_BY_SHEET.get(sheet, DEFAULT_STARTERS)
    }


# ─────────────────────────────────────────────
# CHAT ENDPOINT
# ─────────────────────────────────────────────

@router.post("/message")
async def chat_message(req: ChatRequest):

    api_key = os.environ.get("GEMINI_API_KEY", "")

    if not api_key:
        return JSONResponse(
            status_code=503,
            content={"error": "GEMINI_API_KEY not set"}
        )

    system = _build_system(req)

    async def _stream():
        try:
            client = genai.Client(api_key=api_key)

            # Build conversation manually
            conversation = system + "\n\n"

            for msg in req.history[-6:]:
                role = "Assistant" if msg.role == "assistant" else "User"
                conversation += f"{role}: {msg.content}\n"

            conversation += f"User: {req.message}\nAssistant:"

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=conversation,
                config={
                    "temperature": 0.1,
                    "max_output_tokens": 512,
                }
            )

            text = response.text or ""

            # Simulated streaming
            for token in text.split(" "):
                yield f"data: {json.dumps({'token': token + ' '})}\n\n"
                await asyncio.sleep(0.01)

            yield f"data: {json.dumps({'done': True})}\n\n"

        except Exception as e:
            log.error("Chat error: %s", e)
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        },
    )