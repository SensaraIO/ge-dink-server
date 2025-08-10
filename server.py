from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from datetime import datetime
import os, json

app = FastAPI()
templates = Jinja2Templates(directory="templates")

EVENTS_FILE = os.environ.get("GE_EVENTS_FILE", "events.json")

if not os.path.exists(EVENTS_FILE):
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:
        json.dump([], f)

def load_events():
    try:
        with open(EVENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def save_event(ev):
    events = load_events()
    events.insert(0, ev)
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(events, f)

@app.post("/dink/{token}")
async def dink_webhook(token: str, request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    event = {
        "token": token,
        "time": datetime.utcnow().isoformat() + "Z",
        "payload": payload
    }
    save_event(event)
    return {"ok": True}

@app.get("/recent", response_class=HTMLResponse)
async def recent_events(request: Request, token: str = ""):
    events = [e for e in load_events() if not token or e.get("token") == token]
    return templates.TemplateResponse("recent.html", {"request": request, "events": events})
