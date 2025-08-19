# server.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse
from starlette.datastructures import UploadFile

import os, json, re, time, secrets, asyncio
from typing import Optional, Dict, Any, List

from motor.motor_asyncio import AsyncIOMotorClient
from google.cloud import storage  # Firebase Storage is GCS under the hood
from google.oauth2 import service_account

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
MONGODB_URI = os.environ.get(
    "MONGODB_URI",
    # NOTE: set this in your environment; avoid hardcoding credentials in code
    # "mongodb+srv://<user>:<pass>@cluster0.mongodb.net/<dbname>?retryWrites=true&w=majority"
    ""
)
MONGODB_DB  = os.environ.get("MONGODB_DB", "")
MONGODB_COL = os.environ.get("MONGODB_COL", "events")

FIREBASE_PROJECT_ID   = os.environ.get("FIREBASE_PROJECT_ID", "")
FIREBASE_STORAGE_BUCKET = os.environ.get("FIREBASE_STORAGE_BUCKET", "")
USE_FIREBASE = os.environ.get("USE_FIREBASE", "1") == "1"  # default on if bucket provided
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

UPLOAD_DIR  = os.environ.get("GE_UPLOADS_DIR", "uploads")
TEMPLATES_DIR = os.environ.get("GE_TEMPLATES_DIR", "templates")

# Ensure upload dir exists; fall back to /tmp/uploads if not writable (e.g., no mounted volume)
def _resolve_upload_dir(path: str) -> str:
    try:
        os.makedirs(path, exist_ok=True)
        return path
    except PermissionError:
        fallback = "/tmp/uploads"
        os.makedirs(fallback, exist_ok=True)
        print(f"[startup] GE_UPLOADS_DIR '{path}' not writable; falling back to {fallback}")
        return fallback

UPLOAD_DIR = _resolve_upload_dir(UPLOAD_DIR)
os.makedirs(TEMPLATES_DIR, exist_ok=True)

# ------------------------------------------------------------------------------
# App + CORS
# ------------------------------------------------------------------------------
app = FastAPI(title="ge-dink-server (Mongo)")

# Adjust allow_origins for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount /uploads after resolving upload dir
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# ------------------------------------------------------------------------------
# Mongo client + helpers
# ------------------------------------------------------------------------------
mongo_client: Optional[AsyncIOMotorClient] = None
events_col = None

storage_client: Optional[storage.Client] = None
storage_bucket = None

def _db_name_from_uri(uri: str) -> str:
    """
    Pull DB name from the URI path if provided; fall back to 'eventsdb'.
   
    """
    try:
        parsed = urlparse(uri)
        # path like '/salesemailer'
        if parsed.path and parsed.path != "/":
            return parsed.path.lstrip("/") or "eventsdb"
    except Exception:
        pass
    return "eventsdb"

@app.on_event("startup")
async def startup_event():
    global mongo_client, events_col
    if not MONGODB_URI:
        raise RuntimeError("MONGODB_URI is not set. Set it in your environment.")

    mongo_client = AsyncIOMotorClient(MONGODB_URI)
    db_name = MONGODB_DB or _db_name_from_uri(MONGODB_URI)
    db = mongo_client[db_name]
    events_col = db[MONGODB_COL]

    # Indexes for fast querying by user + type + time
    await events_col.create_index([("token", 1), ("eventType", 1), ("time_dt", -1)])
    await events_col.create_index([("token", 1), ("time_dt", -1)])
    # Backward-compat: token+time string
    await events_col.create_index([("token", 1), ("time", -1)])

    # Initialize Firebase Storage (GCS) if enabled
    global storage_client, storage_bucket
    if USE_FIREBASE and FIREBASE_STORAGE_BUCKET:
        try:
            creds = None
            if GOOGLE_CREDENTIALS_JSON:
                try:
                    info = json.loads(GOOGLE_CREDENTIALS_JSON)
                    creds = service_account.Credentials.from_service_account_info(info)
                    # If project id not explicitly set, prefer the one from credentials
                    project = FIREBASE_PROJECT_ID or info.get("project_id")
                except Exception as ce:
                    print(f"[startup] Failed to load GOOGLE_CREDENTIALS_JSON: {ce}")
                    project = FIREBASE_PROJECT_ID or None
            else:
                # Fall back to ADC / GOOGLE_APPLICATION_CREDENTIALS file if present
                project = FIREBASE_PROJECT_ID or None

            storage_client = storage.Client(project=project, credentials=creds)
            storage_bucket = storage_client.bucket(FIREBASE_STORAGE_BUCKET)
        except Exception as e:
            # Disable Firebase uploads if initialization fails
            storage_client = None
            storage_bucket = None
            print(f"[startup] Firebase Storage init failed: {e}")

# ------------------------------------------------------------------------------
# Utilities (uploads, payload parsing, url patching)
# ------------------------------------------------------------------------------
def _safe_name(fn: str) -> str:
    fn = re.sub(r"[^A-Za-z0-9._-]+", "_", fn or "upload.bin")
    return fn or "upload.bin"

async def _save_upload(file: UploadFile, token: str, base_url: str) -> str:
    data = await file.read()
    orig = _safe_name(getattr(file, "filename", "") or "upload.bin")
    stamp = int(time.time() * 1000)
    rand = secrets.token_hex(4)
    # key path for both GCS and local
    key = f"screenshots/{token[:8]}/{stamp}_{rand}_{orig}"

    # If Firebase Storage is configured, upload there and return public URL
    if USE_FIREBASE and storage_bucket is not None:
        try:
            blob = storage_bucket.blob(key)
            content_type = getattr(file, "content_type", None) or "application/octet-stream"
            blob.upload_from_string(data, content_type=content_type)
            # Make public (simple MVP). For stricter security, switch to signed URLs.
            blob.make_public()
            return blob.public_url
        except Exception as e:
            # Fall back to local if something goes wrong
            print(f"[upload] Firebase upload failed, falling back to local: {e}")
            # continue to local path

    # Local fallback (persist if GE_UPLOADS_DIR is a mounted volume)
    name = key.split("/", 1)[1] if "/" in key else key
    path = os.path.join(UPLOAD_DIR, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    return f"{base_url.rstrip('/')}/uploads/{name}"

def _patch_attachment_urls(payload: dict, mapping: dict, base_url: str) -> dict:
    """
    Rewrite attachment://filename to absolute /uploads/<saved> or full URL if already absolute.
    """
    def repl(u: str) -> str:
        if not isinstance(u, str):
            return u
        low = u.lower()
        if low.startswith("attachment://"):
            key = low.split("://", 1)[1]
            saved = mapping.get(key) or mapping.get(key.lower())
            if saved:
                # If saved is already an absolute URL (e.g., Firebase public URL), return it directly
                if isinstance(saved, str) and (saved.startswith("http://") or saved.startswith("https://")):
                    return saved
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

def _try_json(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None

async def extract_payload(request: Request) -> dict:
    """
    Accept:
      - application/json
      - application/x-www-form-urlencoded (payload_json / payload)
      - multipart/form-data (files saved, payload in fields)
    Fallback: try to JSON-decode raw body.
    """
    ctype = (request.headers.get("content-type") or "").lower()

    # JSON direct
    if "application/json" in ctype:
        try:
            return await request.json()
        except Exception:
            pass

    # x-www-form-urlencoded
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
            return fields
        except Exception:
            pass

    # multipart/form-data
    if "multipart/form-data" in ctype:
        try:
            form = await request.form()
            fields = {}
            saved = {}  # original filename (lower) -> saved basename
            token = request.path_params.get("token", "anon")
            for k, v in form.multi_items():
                if isinstance(v, UploadFile):
                    saved_name = await _save_upload(v, token, str(request.base_url))
                    orig = (getattr(v, "filename", "") or "").strip()
                    if orig:
                        saved[orig.lower()] = saved_name
                else:
                    fields.setdefault(k, str(v))
            raw = fields.get("payload_json") or fields.get("payload") or "{}"
            try:
                payload = json.loads(raw)
            except Exception:
                payload = fields
            payload = _patch_attachment_urls(payload, saved, str(request.base_url))
            return payload
        except Exception:
            pass

    # last-ditch: decode raw
    try:
        raw = (await request.body()).decode("utf-8", "replace")
        obj = _try_json(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}

def _parse_time_param(val: Optional[str]) -> Optional[str]:
    """
    Accept unix seconds or ISO8601; return ISO-UTC string.
    """
    if not val:
        return None
    s = str(val).strip()
    if s.isdigit():  # unix seconds
        return datetime.fromtimestamp(int(s), tz=timezone.utc).isoformat()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

def _parse_time_param_dt(val: Optional[str]):
    """Return timezone-aware datetime (UTC) or None."""
    if not val:
        return None
    s = str(val).strip()
    try:
        if s.isdigit():
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@app.get("/healthz", response_class=PlainTextResponse)
async def healthz():
    return "ok"

@app.get("/")
async def root():
    mode = "Firebase Storage" if (USE_FIREBASE and storage_bucket is not None) else "Local uploads"
    creds_mode = "env-json" if GOOGLE_CREDENTIALS_JSON else "adc/file"
    return {"ok": True, "service": "ge-dink-server", "time": datetime.utcnow().isoformat() + "Z", "storage_mode": mode, "credentials_mode": creds_mode}

@app.post("/dink/{token}")
async def dink_webhook(token: str, request: Request):
    """
    Ingest an event for a specific user token.
    - Accepts JSON, x-www-form-urlencoded (payload_json/payload), or multipart with files.
    - Rewrites attachment:// URIs to your /uploads base.
    - Stores into MongoDB.
    """
    payload = await extract_payload(request)
    now = datetime.now(timezone.utc)
    event_type = str(payload.get("type", "")).upper() if isinstance(payload, dict) else ""
    doc = {
        "token": token,
        # Keep string ISO (with trailing Z) for backward compatibility
        "time": now.isoformat().replace("+00:00", "Z"),
        # Store real BSON datetime for efficient range scans
        "time_dt": now,
        "ip": request.client.host if request.client else None,
        "eventType": event_type,
        "payload": payload,
        "createdAt": now.isoformat(),
    }
    await events_col.insert_one(doc)
    return {"ok": True}

@app.get("/recent", response_class=HTMLResponse)
async def recent_events(request: Request, token: str = ""):
    """
    Tiny debug view of the newest 200 docs.
    """
    q: Dict[str, Any] = {}
    if token:
        q["token"] = token
    cur = events_col.find(q, sort=[("time_dt", -1), ("time", -1)], limit=200)
    events = []
    async for row in cur:
        events.append({
            "id": str(row.get("_id")),
            "token": row.get("token"),
            "time": (row.get("time_dt") or row.get("time")),
            "eventType": row.get("eventType"),
            "ip": row.get("ip"),
            "payload": row.get("payload") or {},
        })
    html = "<html><body><h3>Recent (debug)</h3><pre>" + json.dumps(events, indent=2) + "</pre></body></html>"
    return HTMLResponse(html)

@app.get("/recent.json")
async def recent_json(
    token: str = "",
    limit: int = 500,
    offset: int = 0,
    since: Optional[str] = None,
    until: Optional[str] = None,
    type: Optional[str] = None
):
    """
    Paged JSON API:
      ?token=...&limit=500&offset=0
      ?since=unix_or_ISO&until=unix_or_ISO  (filters by 'time_dt' BSON datetime field)
      ?type=TYPE (filters by eventType)
    Returns:
      { total, limit, offset, next_offset, events: [...] }
    """
    limit = max(1, min(int(limit), 10000))
    offset = max(0, int(offset))

    query: Dict[str, Any] = {}
    if token:
        query["token"] = token
    if type:
        query["eventType"] = str(type).upper()

    # time window on the BSON datetime `time_dt` (falls back to string if missing)
    since_dt = _parse_time_param_dt(since)
    until_dt = _parse_time_param_dt(until)
    if since_dt or until_dt:
        tw: Dict[str, Any] = {}
        if since_dt: tw["$gte"] = since_dt
        if until_dt: tw["$lte"] = until_dt
        query["time_dt"] = tw

    total = await events_col.count_documents(query)

    # newest-first
    cursor = events_col.find(query, sort=[("time_dt", -1), ("time", -1)], skip=offset, limit=limit)
    out: List[Dict[str, Any]] = []
    async for row in cursor:
        out.append({
            "id": str(row.get("_id")),
            "token": row.get("token"),
            "time": row.get("time") or (row.get("time_dt").isoformat() if row.get("time_dt") else None),
            "eventType": row.get("eventType"),
            "ip": row.get("ip"),
            "payload": row.get("payload") or {},
        })

    next_offset = offset + len(out)
    if next_offset >= total:
        next_offset = None

    return JSONResponse({
        "total": total,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "events": out
    })