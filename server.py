# server.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from datetime import datetime
from urllib.parse import parse_qs
from starlette.datastructures import UploadFile
import os, json, re, time, secrets

app = FastAPI()
templates = Jinja2Templates(directory="templates")

EVENTS_FILE = os.environ.get("GE_EVENTS_FILE", "events.json")
MAX_EVENTS = 2000  # simple cap so the file doesn't grow forever

UPLOAD_DIR = os.environ.get("GE_UPLOADS_DIR", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

if not os.path.exists(EVENTS_FILE):
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:
        json.dump([], f)

def load_events():
    try:
        with open(EVENTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _safe_name(fn: str) -> str:
    fn = re.sub(r"[^A-Za-z0-9._-]+", "_", fn or "upload.bin")
    return fn or "upload.bin"

async def _save_upload(file: UploadFile, token: str) -> str:
    orig = _safe_name(getattr(file, "filename", "") or "upload.bin")
    stamp = int(time.time() * 1000)
    rand = secrets.token_hex(4)
    name = f"{token[:8]}_{stamp}_{rand}_{orig}"
    path = os.path.join(UPLOAD_DIR, name)
    data = await file.read()
    with open(path, "wb") as f:
        f.write(data)
    return name

def _patch_attachment_urls(payload: dict, mapping: dict, base_url: str) -> dict:
    def repl(u: str) -> str:
        if not isinstance(u, str):
            return u
        low = u.lower()
        if low.startswith("attachment://"):
            key = low.split("://", 1)[1]
            saved = mapping.get(key) or mapping.get(key.lower())
            if saved:
                return f"{base_url.rstrip('/')}/uploads/{saved}"
        return u
    embeds = (payload.get("embeds") or [])
    for emb in embeds:
        img = emb.get("image") or {}
        if "url" in img:
            img["url"] = repl(img.get("url"))
        thumb = emb.get("thumbnail") or {}
        if "url" in thumb:
            thumb["url"] = repl(thumb.get("url"))
    if "screenshot_url" in payload:
        payload["screenshot_url"] = repl(payload.get("screenshot_url"))
    return payload

def save_event(ev):
    events = load_events()
    events.insert(0, ev)
    if len(events) > MAX_EVENTS:
        events = events[:MAX_EVENTS]
    with open(EVENTS_FILE, "w", encoding="utf-8") as f:
        json.dump(events, f)

def _try_json(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None

async def extract_payload(request: Request) -> dict:
    """
    Accept:
      - application/json               -> raw JSON body
      - application/x-www-form-urlencoded -> look for payload_json / payload (stringified JSON)
      - multipart/form-data            -> same as above (ignore file parts)
    Fallback: try to JSON-decode the raw body anyway.
    """
    ctype = (request.headers.get("content-type") or "").lower()

    # JSON direct
    if "application/json" in ctype:
        try:
            return await request.json()
        except Exception:
            pass  # fall through

    # form-urlencoded
    if "application/x-www-form-urlencoded" in ctype:
        try:
            raw = (await request.body()).decode("utf-8", "replace")
            form = parse_qs(raw)
            fields = {k: (v[0] if isinstance(v, list) and v else "") for k, v in form.items()}
            for key in ("payload_json", "payload"):
                if key in fields:
                    obj = _try_json(fields[key])
                    if isinstance(obj, dict):
                        return obj
            return fields  # not JSON, but at least record fields
        except Exception:
            pass  # fall through

    # multipart
    if "multipart/form-data" in ctype:
        try:
            form = await request.form()
            fields = {}
            # form.multi_items() preserves duplicates; we only need first
            for k, v in form.multi_items():
                if isinstance(v, UploadFile):
                    # ignore files for this simple server (Discord-style attachments etc.)
                    continue
                fields.setdefault(k, str(v))
            for key in ("payload_json", "payload"):
                if key in fields:
                    obj = _try_json(fields[key])
                    if isinstance(obj, dict):
                        return obj
            return fields
        except Exception:
            pass  # fall through

    # Last-ditch: try to JSON-decode raw body regardless of content-type
    try:
        raw = (await request.body()).decode("utf-8", "replace")
        obj = _try_json(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    return {}

@app.get("/")
async def root():
    return {"ok": True, "service": "ge-dink-server", "time": datetime.utcnow().isoformat() + "Z"}

@app.post("/dink/{token}")
async def dink_webhook(token: str, request: Request):
    ctype = (request.headers.get("content-type") or "").lower()
    base_url = str(request.base_url)
    payload = {}
    # Handle multipart with file uploads
    if "multipart/form-data" in ctype:
        try:
            form = await request.form()
            fields = {}
            saved = {}  # original filename (lower) -> saved basename
            for k, v in form.multi_items():
                if isinstance(v, UploadFile):
                    saved_name = await _save_upload(v, token)
                    orig = (getattr(v, "filename", "") or "").strip()
                    if orig:
                        saved[orig.lower()] = saved_name
                else:
                    fields.setdefault(k, str(v))
            # Parse JSON payload from fields
            raw = fields.get("payload_json") or fields.get("payload") or "{}"
            try:
                payload = json.loads(raw)
            except Exception:
                payload = fields
            # Rewrite attachment:// URLs to server /uploads/ URLs
            payload = _patch_attachment_urls(payload, saved, base_url)
        except Exception:
            payload = await extract_payload(request)
    else:
        payload = await extract_payload(request)

    event = {
        "token": token,
        "time": datetime.utcnow().isoformat() + "Z",
        "payload": payload,
        "ip": request.client.host if request.client else None,
    }
    save_event(event)
    return {"ok": True}

@app.get("/recent", response_class=HTMLResponse)
async def recent_events(request: Request, token: str = ""):
    events = [e for e in load_events() if not token or e.get("token") == token]
    return templates.TemplateResponse("recent.html", {"request": request, "events": events})

@app.get("/recent.json")
async def recent_json(token: str = "", limit: int = 50):
    events = [e for e in load_events() if not token or e.get("token") == token]
    return JSONResponse(events[: max(1, min(int(limit), 500))])
