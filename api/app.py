# --- keep imports at top ---
import os, io, time, hashlib
from typing import Optional
from datetime import datetime, timezone
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
import boto3, psycopg
from psycopg.types import json as psyjson  # (optional, handy if you use psyjson.Json)
import json as _json

# --- create the app ONCE, before any @app.* decorators ---
app = FastAPI()

# --------- config/env ----------
def load_keys():
    raw = os.getenv("API_KEYS", "")
    d = {}
    for pair in [p.strip() for p in raw.split(",") if p.strip()]:
        dev, key = pair.split(":", 1)
        d[dev] = key
    return d

API_KEYS = load_keys()
DATABASE_URL = os.environ["DATABASE_URL"]

# MinIO client
S3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT"),
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
)
BKT = os.getenv("S3_BUCKET_IMAGES", "images")
DBURL = DATABASE_URL

def db():
    return psycopg.connect(DBURL)

# --------- models/auth ----------
class Telemetry(BaseModel):
    device_id: str
    ts: Optional[datetime] = None
    seq: Optional[int] = None
    metrics: dict[str, float] = Field(default_factory=dict)

class ChatReq(BaseModel):
    user_id: str
    message: str
    session_id: str | None = None

def auth_device(authorization: Optional[str], device_id: str):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    provided = authorization.split(" ", 1)[1]
    expected = API_KEYS.get(device_id)
    if not expected or provided != expected:
        raise HTTPException(status_code=403, detail="Invalid API key for device")

# --------- startup ----------
@app.on_event("startup")
def init_db():
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
          CREATE TABLE IF NOT EXISTS images(
            id BIGSERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            key TEXT NOT NULL UNIQUE,
            sha256 TEXT NOT NULL,
            mime TEXT,
            width INT, height INT,
            created_at TIMESTAMPTZ DEFAULT now()
          );""")

# --------- routes ----------
@app.get("/api/health")
def health():
    return {"ok": True, "time": time.time()}

@app.post("/api/ingest")
def ingest(body: Telemetry, authorization: Optional[str] = Header(None)):
    auth_device(authorization, body.device_id)
    ts = body.ts or datetime.now(timezone.utc)
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO telemetry (device_id, ts_utc, seq, metrics_json)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (device_id, seq) DO NOTHING
                """,
                (body.device_id, ts, body.seq, psycopg.types.json.Json(body.metrics))
            )
            stored = cur.rowcount == 1
    return {"ok": True, "stored": stored}

@app.get("/api/last")
def last(device_id: str = Query(...), n: int = Query(10, ge=1, le=100)):
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_utc, seq, metrics_json
                FROM telemetry
                WHERE device_id = %s
                ORDER BY ts_utc DESC
                LIMIT %s
                """,
                (device_id, n)
            )
            rows = cur.fetchall()
    return [{"ts": r[0].isoformat(), "seq": r[1], "metrics": r[2]} for r in rows]

class ImgRow(BaseModel):
    id:int; device_id:str; ts:str; key:str; sha256:str
    mime:str|None=None; width:int|None=None; height:int|None=None

@app.post("/api/upload_image", response_model=ImgRow)
async def upload_image(file: UploadFile = File(...), device_id: str = Form("cam01")):
    data = await file.read()
    sha = hashlib.sha256(data).hexdigest()
    ext = os.path.splitext(file.filename or "")[-1].lower() or ".jpg"
    day = time.strftime("%Y/%m/%d")
    key = f"{device_id}/{day}/{sha}{ext}"
    S3.put_object(Bucket=BKT, Key=key, Body=data, ContentType=file.content_type or "application/octet-stream")
    w = h = None
    try:
        from PIL import Image
        import io as _io
        im = Image.open(_io.BytesIO(data))
        w, h = im.size
    except Exception:
        pass
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO images(device_id,key,sha256,mime,width,height)
               VALUES (%s,%s,%s,%s,%s,%s)
               RETURNING id, extract(epoch from ts)::text""",
            (device_id, key, sha, file.content_type, w, h),
        )
        row_id, ts = cur.fetchone()
    return ImgRow(id=row_id, device_id=device_id, ts=ts, key=key, sha256=sha,
                  mime=file.content_type, width=w, height=h)

@app.get("/api/list", response_model=list[ImgRow])
def list_images(device_id: str | None = None, limit: int = 20):
    q = """SELECT id, device_id, extract(epoch from ts)::text, key, sha256, mime, width, height
           FROM images"""
    p = []
    if device_id:
        q += " WHERE device_id=%s"
        p.append(device_id)
    q += " ORDER BY ts DESC LIMIT %s"
    p.append(limit)
    with db() as conn, conn.cursor() as cur:
        cur.execute(q, p)
        rows = cur.fetchall()
    return [ImgRow(id=r[0], device_id=r[1], ts=r[2], key=r[3], sha256=r[4],
                   mime=r[5], width=r[6], height=r[7]) for r in rows]

@app.post("/api/chat")
async def chat(req: ChatReq, authorization: str | None = Header(None)):
    # optional: reuse your API_KEYS auth (or leave open for now)
    # auth_device(authorization, "kit-001")
    from rag_conv.rag_agent import chat_once
    return await run_in_threadpool(
        chat_once,
        user_id=req.user_id,
        user_message=req.message,
        session_id=req.session_id
    )