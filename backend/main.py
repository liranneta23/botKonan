import json
import os
import re
from datetime import datetime
from typing import AsyncGenerator

from dotenv import load_dotenv
load_dotenv()

import anthropic
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

MONDAY_API_KEY  = os.getenv("MONDAY_API_KEY")
MONDAY_BOARD_ID = int(os.getenv("MONDAY_BOARD_ID", "0"))

COUNTRY_CODES = {
    "תאילנד": "TH", "צרפת": "FR", "גרמניה": "DE", "ספרד": "ES",
    "איטליה": "IT", "יוון": "GR", "הודו": "IN", 'ארה"ב': "US",
    "אנגליה": "GB", "פורטוגל": "PT", "הונגריה": "HU", "פולין": "PL",
    "טורקיה": "TR", "יפן": "JP", "אוסטרליה": "AU", "קנדה": "CA",
}

def country_to_code(name: str) -> str:
    return COUNTRY_CODES.get(name.strip(), "IL")

app = FastAPI(title="Haverim Mehalzim Intake Coordinator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = anthropic.Anthropic()

SAR_SYSTEM_PROMPT_TEMPLATE = """אתה כונן קבלת אירועים של ארגון "חברים מחלצים" — ארגון המסייע לישראלים במצבי חירום בחו"ל.
תפקידך לסייע לכונן לאסוף את כל המידע הדרוש לפתיחת אירוע בשיחה עם המתקשר.
תאריך ושעה נוכחיים: <<NOW>>

## פורמט תגובה (השתמש **בדיוק** בפורמט זה אחרי כל קלט)

**✅ נקלט:**
[רשימת כל המידע שנאסף **לאורך כל השיחה עד עכשיו** — לא רק מההודעה האחרונה. אם שדה נמסר בהודעה קודמת — הוא עדיין נקלט ויש לכלול אותו כאן.]

**⚠️ חסר (לפי סדר עדיפות):**
[רשימה ממוספרת של שדות שעדיין **לא נמסרו בשום שלב בשיחה**. אם שדה כבר נמסר — אל תכלול אותו כאן, גם אם הוא נמסר לפני כמה הודעות.]

**💬 שאל את המתקשר:**
[2-3 שאלות על שדות שעדיין חסרים. אל תשאל שוב על מידע שכבר התקבל.]

## כלל זיכרון חשוב
אתה צובר מידע לאורך כל השיחה. כל מידע שנמסר בכל הודעה שהיא — נשמר ונחשב כנקלט. לעולם אל תבקש מידע שכבר ניתן.

---

## שדות חובה (עקוב אחרי כל 9)

0. **שם הכונן** — שם הכונן המטפל בפנייה (שאל בתחילת השיחה)
1. **סיווג האירוע** — איתור | חילוץ | רפואי | פסיכולוגי | אנטישמי | אחר
2. **מיקום** — מדינה, עיר, GPS אם זמין
3. **תיאור האירוע** — מה קרה? מה נדרש?
4. **פרטי המדווח** — שם מלא, טלפון, קרבה לנפגע, מיקום ביחס לנפגע (האם נמצא לידו?)
5. **פרטי הנפגע** — שם מלא, טלפון, מגדר, גיל, מספר ת"ז
6. **ביטוח** — שם חברת הביטוח, האם כבר יצרו קשר?
7. **גורמים מעורבים** — חברת חילוץ | כוחות ביטחון | משרד החוץ | שגרירות
8. **שאלות ייעודיות** — בהתאם לסיווג האירוע (ראה למטה)

---

## שאלות ייעודיות לפי סוג האירוע

לאחר זיהוי סוג האירוע, אסוף את המידע הייעודי הבא:

**איתור:** טלפון לווייני, סוג טלפון, מיקום אחרון ידוע (מתי ואיפה), שם ברשתות חברתיות, רקע פסיכולוגי/רפואי רלוונטי, שימוש בשמות בדויים, תמונות עדכניות, כלי איתור (שעון חכם, AirTag וכו')

**חילוץ:** GPS מדויק אם אפשר, האם כוחות חילוץ כבר נמצאים באזור?

**רפואי:** תיאור מפורט של האירוע, סימנים חיוניים/מדדים (דופק, לחץ דם, נשימה, הכרה)

**פסיכולוגי:** רקע פסיכולוגי רלוונטי, שימוש בתרופות, אבחנות עבר

---

## כלל חירום
אם המתקשר מתאר מצב מסכן חיים (אובדן הכרה, עצירת לב, נפילה חמורה, דימום משמעותי, היפותרמיה, אי-נשימה), פתח את תגובתך עם:

⚡ **פעולה מיידית — הפעל במקביל:**
- צור קשר עם שירותי חירום מקומיים עכשיו — אל תמתין לנתונים מלאים
- העבר את הצוות עם המידע הזמין כעת

ואז המשך באיסוף המידע הרגיל.

---

## סיום — דוח אירוע
כאשר הכונן אומר "זהו המידע" או כאשר כל השדות הקריטיים מולאו, פלוט **רק** את הדוח הבא (מלא את כל הנתונים שנאספו בשיחה):

---
🚨 דוח אירוע חברים מחלצים
תאריך/שעה: <<NOW>>
כונן: [שם הכונן שנמסר]

---
📋 סיווג האירוע
סוג: [ערך]
תיאור: [ערך]

---
📍 מיקום
מדינה: [ערך]
עיר: [ערך]
GPS / פרטים נוספים: [ערך]

---
👤 פרטי המדווח
שם מלא: [ערך]
טלפון: [ערך]
קרבה לנפגע: [ערך]
מיקום ביחס לנפגע: [ערך]

---
🧑 פרטי הנפגע
שם מלא: [ערך]
טלפון: [ערך]
מגדר: [ערך]
גיל: [ערך]
מספר ת"ז: [ערך]

---
🏥 ביטוח
חברת ביטוח: [ערך]
האם יצרו קשר?: [ערך]

---
🤝 גורמים מעורבים
גורמים: [ערך]

---
🔍 מידע ייעודי לסוג האירוע
[שדות ייעודיים לפי סוג האירוע בפורמט: שם שדה: ערך]

---
⚡ רמת עדיפות: [קריטי / גבוה / בינוני / נמוך]
נימוק: [משפט אחד]

---
📌 פעולות מיידיות
1. [פעולה]
2. [פעולה]
3. [פעולה]

---
*הדוח נוצר: <<NOW>> | מוקד חברים מחלצים*

---

## כללי סגנון
- ללא מילוי, ללא נימוסים — נתונים בלבד
- תגובות קצרות וקריאות
- השתמש בשמות השדות המדויקים מרשימת השדות לעיל
- כתוב בעברית

## כללי פורמט הדוח הסופי — חובה
כאשר אתה מפיק את הדוח הסופי:
- אסור להשתמש ב-`#` או `##` לכותרות — כתוב את שם הקטע בלבד ללא סימני markdown
- אסור להשתמש ב-`**` להדגשה — כתוב את הטקסט ללא כוכביות
- היחיד שמותר: `*טקסט*` בשורה האחרונה בלבד (הדוח נוצר...)
- הדוח יוצא כטקסט פשוט עם אמוג'י וקווים מפרידים בלבד"""


def build_system_prompt() -> str:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    return SAR_SYSTEM_PROMPT_TEMPLATE.replace("<<NOW>>", now)


conversations: dict[str, list] = {}


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"


class ResetRequest(BaseModel):
    session_id: str = "default"


class MondayRequest(BaseModel):
    report_text: str
    session_id: str = "default"


def parse_report(text: str) -> dict:
    def get(pattern, default=""):
        m = re.search(pattern, text, re.MULTILINE | re.DOTALL)
        return m.group(1).strip() if m else default

    victim_sec   = re.search(r"פרטי הנפגע(.*?)(?=🏥|---)", text, re.DOTALL)
    reporter_sec = re.search(r"פרטי המדווח(.*?)(?=🧑|---)", text, re.DOTALL)

    v_phone = ""
    if victim_sec:
        vm = re.search(r"טלפון:\s*(.+)", victim_sec.group(1))
        v_phone = vm.group(1).strip() if vm else ""

    r_name, r_phone = "", ""
    if reporter_sec:
        rn = re.search(r"שם מלא:\s*(.+)", reporter_sec.group(1))
        rp = re.search(r"טלפון:\s*(.+)", reporter_sec.group(1))
        r_name  = rn.group(1).strip() if rn else ""
        r_phone = rp.group(1).strip() if rp else ""

    return {
        "event_type":    get(r"סוג:\s*(.+)"),
        "description":   get(r"תיאור:\s*(.+)"),
        "country":       get(r"מדינה:\s*(.+)"),
        "city":          get(r"עיר:\s*(.+)"),
        "date":          datetime.now().strftime("%d/%m/%Y %H:%M"),
        "month":         datetime.now().strftime("%B"),
        "insurance":     get(r"חברת ביטוח:\s*(.+)"),
        "victim_phone":  v_phone,
        "reporter":      f"{r_name} | {r_phone}",
        "victim_age":    get(r"גיל:\s*(.+)"),
        "victim_gender": get(r"מגדר:\s*(.+)"),
        "operator":      get(r"כונן:\s*(.+)"),
    }


async def create_monday_item(fields: dict) -> bool:
    item_name = f"{fields['event_type']} | {fields['country']} | {fields['date']}"
    column_values = {
        "status_mkmb1zc6":    {"label": fields["event_type"]},
        "color_mkvvrm1r":     {"label": "נפתח אירוע"},
        "status_mkmbjwef":    {"label": "פתוח"},
        "color_mkmby5dg":     {"label": fields["month"]},
        "country_mkmb91h3":   {"countryCode": country_to_code(fields["country"]), "countryName": fields["country"]},
        "location_mkmbv7be":  {"address": fields["city"]},
        "text_mkmbt7j5":      fields["date"],
        "long_text_mkpfvmh3": {"text": fields["description"]},
        "color_mkmbwnzy":     {"label": fields["insurance"]},
        "phone_mkz3dr0y":     {"phone": fields["victim_phone"], "countryShortName": "IL"},
        "text_mkz3yv22":      fields["reporter"],
        "numeric_mkng2emx":   fields["victim_age"],
        "color_mkngmw3":      {"label": fields["victim_gender"]},
        "color_mkmbwakp":     {"label": fields["operator"]},
    }
    mutation = """
    mutation ($board: ID!, $name: String!, $cols: JSON!) {
      create_item(board_id: $board, item_name: $name, column_values: $cols) { id }
    }"""
    async with httpx.AsyncClient() as http:
        r = await http.post(
            "https://api.monday.com/v2",
            headers={"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"},
            json={"query": mutation, "variables": {
                "board": MONDAY_BOARD_ID,
                "name":  item_name,
                "cols":  json.dumps(column_values),
            }},
            timeout=10.0,
        )
        data = r.json()
        return "errors" not in data and data.get("data", {}).get("create_item") is not None


async def stream_sar_response(messages: list, session_id: str) -> AsyncGenerator[str, None]:
    full_text: list[str] = []

    try:
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=[{"type": "text", "text": build_system_prompt(), "cache_control": {"type": "ephemeral"}}],
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


@app.post("/api/monday")
async def send_to_monday(request: MondayRequest):
    fields = parse_report(request.report_text)
    success = await create_monday_item(fields)
    return {"success": success}


@app.post("/api/reset")
async def reset(request: ResetRequest):
    conversations.pop(request.session_id, None)
    return {"status": "ok"}


app.mount("/", StaticFiles(directory="../frontend", html=True), name="static")
