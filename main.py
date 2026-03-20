"""
tg-scraper/main.py
Telethon scraper + FastAPI server.
- Fetches last 5 days of messages only
- Deduplicates using message IDs (no repeats on refresh)
- Extracts form start, last date, exam date from message text
- Exposes /messages endpoint for Netlify to fetch
"""

import asyncio
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.sessions import StringSession
import uvicorn

# ── Config ────────────────────────────────────────────────────────────────────
API_ID   = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION  = os.environ["TELEGRAM_SESSION"]
CHANNELS = os.environ.get(
    "TELEGRAM_CHANNELS",
    "sarkariresult,freejobalerti,sarkariexam"
).split(",")

MESSAGES_PER_CHANNEL = 50
REFRESH_EVERY        = 300
MAX_AGE_DAYS         = 5

# ── In-memory store ───────────────────────────────────────────────────────────
store: list[dict] = []
seen_ids: set     = set()
last_fetched: Optional[str] = None

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/messages")
def get_messages():
    return {"success": True, "items": store, "fetchedAt": last_fetched, "count": len(store)}

@app.get("/health")
def health():
    return {"status": "ok", "items": len(store)}


# ── Date extraction ───────────────────────────────────────────────────────────
DATE_PATTERNS = [
    r"\b(\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+\d{4})\b",
    r"\b(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})\b",
]

def find_dates(text: str) -> dict:
    result = {"formStart": None, "lastDate": None, "examDate": None}
    for line in text.split("\n"):
        ll = line.lower()
        date = _first_date(line)
        if not date:
            continue
        if any(k in ll for k in ["last date", "अंतिम तिथि", "closing date", "last day"]):
            result["lastDate"] = date
        elif any(k in ll for k in ["exam date", "परीक्षा तिथि", "exam on", "written exam"]):
            result["examDate"] = date
        elif any(k in ll for k in ["form start", "apply from", "start date", "शुरू", "ऑनलाइन आवेदन"]):
            result["formStart"] = date
    if not any(result.values()):
        d = _first_date(text)
        if d:
            result["lastDate"] = d
    return result

def _first_date(text: str) -> Optional[str]:
    for pattern in DATE_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


# ── Classifiers ───────────────────────────────────────────────────────────────
def classify_type(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["upsc","ias","ifs"]):                  return "UPSC"
    if any(k in t for k in ["ssc","cgl","chsl","mts"]):            return "SSC"
    if any(k in t for k in ["bank","rbi","ibps","sbi","nabard"]):  return "Banking"
    if any(k in t for k in ["railway","rrb","ntpc"]):              return "Railway"
    if any(k in t for k in ["defence","army","navy","crpf","bsf"]): return "Defence"
    if any(k in t for k in ["teacher","ugc","net","tet","ctet"]):  return "Teaching"
    if any(k in t for k in ["psc","state"]):                       return "State PSC"
    return "Other"

def classify_status(text: str, date: datetime) -> str:
    age_days = (datetime.now(timezone.utc) - date).days
    t = text.lower()
    if "last date" in t or "closing" in t: return "Last Few Days"
    if age_days < 2:                        return "New"
    return "Ongoing"

def extract_org(text: str) -> str:
    for org in ["UPSC","SSC","IBPS","SBI","RBI","SEBI","LIC","DRDO","ISRO","AIIMS","Railway","NABARD"]:
        if org.upper() in text.upper():
            return org
    return "Government"

def extract_posts(text: str) -> str:
    m = re.search(r"(\d[\d,]*)\s*(post|vacanc|seat)", text, re.I)
    if m: return f"{m.group(1)} {m.group(2)}s"
    m = re.search(r"(Officer|Inspector|Constable|Clerk|Engineer|Teacher|Assistant|Manager)", text, re.I)
    if m: return m.group(1)
    return "Various Posts"

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


# ── Scraper ───────────────────────────────────────────────────────────────────
async def scrape_once(client: TelegramClient):
    global store, last_fetched
    cutoff    = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
    new_items = []

    for channel in CHANNELS:
        channel = channel.strip().lstrip("@")
        try:
            entity   = await client.get_entity(channel)
            messages = await client.get_messages(entity, limit=MESSAGES_PER_CHANNEL)
            for msg in messages:
                if not msg.text or msg.date < cutoff:
                    continue
                uid = f"{channel}:{msg.id}"
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)

                text  = clean(msg.text)
                dates = find_dates(msg.text)

                new_items.append({
                    "title":        text[:120],
                    "desc":         text[:300],
                    "link":         f"https://t.me/{channel}/{msg.id}",
                    "source":       f"@{channel}",
                    "body":         extract_org(text),
                    "posts":        extract_posts(text),
                    "eligibility":  "See notification",
                    "postedOn":     msg.date.strftime("%Y-%m-%d"),
                    "formStart":    dates["formStart"],
                    "lastDate":     dates["lastDate"] or "Check notification",
                    "examDate":     dates["examDate"],
                    "type":         classify_type(text),
                    "status":       classify_status(text, msg.date),
                    "fromTelegram": True,
                })
        except Exception as e:
            print(f"[scraper] Failed for @{channel}: {e}")

    if new_items:
        store = sorted(store + new_items, key=lambda x: x["postedOn"], reverse=True)
        print(f"[scraper] Added {len(new_items)} new. Total: {len(store)}")
    else:
        print("[scraper] No new messages.")

    last_fetched = datetime.now(timezone.utc).isoformat()


async def scraper_loop():
    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        while True:
            await scrape_once(client)
            await asyncio.sleep(REFRESH_EVERY)


@app.on_event("startup")
async def startup():
    asyncio.create_task(scraper_loop())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)