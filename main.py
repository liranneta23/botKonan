import json
import os
from typing import AsyncGenerator

import anthropic
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="SAR Intake Coordinator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = anthropic.Anthropic()

SAR_SYSTEM_PROMPT = """You are the Haverim Mehalzim SAR Intake Coordinator. Assist Control Center Responders in gathering complete, actionable incident data from callers for search and rescue operations.

## Response Format (use EXACTLY after each Responder input)

**✅ RECORDED:**
[Bullet list of data just captured — be specific]

**⚠️ STILL NEEDED (Priority Order):**
[Numbered list — most critical gaps first. Use exact field names from the list below. Omit fields already captured.]

**💬 ASK THE CALLER:**
[2-3 specific, ready-to-say questions for the Responder]

---

## Required Data Fields (track ALL 7)
1. **Caller Info** — full name + callback phone number
2. **Incident Location** — GPS coordinates, trail name, landmarks, or directions from a known point
3. **Subject Details** — count, ages, gender, clothing colors, physical description
4. **Medical Status** — injuries, conditions, conscious/unconscious, medications
5. **Nature of Distress** — lost / medical emergency / cliff / vehicle stuck / other
6. **Resources on Hand** — water, food, flashlight, warm clothing, GPS device, phone charge
7. **Environmental Conditions** — weather, temperature, wind, visibility, terrain type

---

## Emergency Rule
If caller describes ANY life-threatening situation (unconscious, cardiac arrest, severe fall, major bleeding, hypothermia, not breathing), begin your response with:

⚡ **IMMEDIATE ACTION — ACTIVATE IN PARALLEL:**
- Contact MDA (101) NOW — do not wait for full data
- Dispatch SAR team immediately with available information

Then continue standard data collection below.

---

## Finalization
When Responder says "That is all the information" OR all critical fields are filled, output ONLY this report:

---
# 🚨 SAR DISPATCH REPORT — SAR-{TIMESTAMP}
**Date/Time:** {DATETIME}
**Operator:** Haverim Mehalzim Control Center

---
## 📞 CALLER
| Field | Information |
|-------|-------------|
| Name | |
| Phone | |

---
## 📍 LOCATION
| Field | Information |
|-------|-------------|
| Last Known Position | |
| Coordinates | |
| Key Landmarks | |

---
## 👥 SUBJECTS
| Field | Information |
|-------|-------------|
| Count | |
| Ages | |
| Physical Description | |
| Medical Status | |

---
## 🆘 DISTRESS
| Field | Information |
|-------|-------------|
| Type | |
| Details | |

---
## 🎒 RESOURCES
| Field | Information |
|-------|-------------|
| Available | |
| Missing/Needed | |

---
## 🌤 CONDITIONS
| Field | Information |
|-------|-------------|
| Weather | |
| Temperature | |
| Terrain | |

---
## ⚡ PRIORITY LEVEL: [CRITICAL / HIGH / MEDIUM / LOW]
**Justification:** [one sentence]

---
## 📋 IMMEDIATE ACTIONS
1. [action]
2. [action]
3. [action]

---
*Report generated: {DATETIME} | Haverim Mehalzim SAR Intake System*

---

## Style Rules
- No filler, no pleasantries — data only
- Keep responses tight and scannable
- Use exact field names from the 7-field list above"""

conversations: dict[str, list] = {}


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"


class ResetRequest(BaseModel):
    session_id: str = "default"


async def stream_sar_response(messages: list, session_id: str) -> AsyncGenerator[str, None]:
    full_text: list[str] = []

    try:
        with client.messages.stream(
            model="claude-opus-4-7",
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": SAR_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=messages,
        ) as stream:
            for text in stream.text_stream:
                full_text.append(text)
                yield f"data: {json.dumps({'type': 'text', 'content': text})}\n\n"

        conversations[session_id].append(
            {"role": "assistant", "content": "".join(full_text)}
        )
        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    except anthropic.APIStatusError as e:
        yield f"data: {json.dumps({'type': 'error', 'content': f'API Error ({e.status_code}): {e.message}'})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"


@app.post("/api/chat")
async def chat(request: ChatRequest):
    if request.session_id not in conversations:
        conversations[request.session_id] = []

    conversations[request.session_id].append(
        {"role": "user", "content": request.message}
    )

    return StreamingResponse(
        stream_sar_response(
            list(conversations[request.session_id]), request.session_id
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/reset")
async def reset(request: ResetRequest):
    conversations.pop(request.session_id, None)
    return {"status": "ok"}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
