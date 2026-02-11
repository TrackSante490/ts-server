# --- keep imports at top ---
import os, io, time, hashlib
import base64, secrets
import httpx
from typing import Optional
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Query, Request, Depends, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import boto3, psycopg
from psycopg.types import json as psyjson  # (optional, handy if you use psyjson.Json)
import json as _json
from urllib.parse import urlsplit, urlunsplit
import jwt

# --- create the app ONCE, before any @app.* decorators ---
app = FastAPI()

# ---- CORS (so web frontend can call the API) ----
WEB_ORIGIN = os.getenv("WEB_ORIGIN", "https://tracksante.com")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[WEB_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Authentik / OIDC config ----
# IMPORTANT: use the *container-internal* issuer for backend-to-authentik calls
# If your API is inside docker compose, talk to authentik-server by service name.
AUTH_INTERNAL_ISSUER = os.getenv(
    "AUTH_INTERNAL_ISSUER",
    "http://authentik-server:9000/application/o/tracksante/",
).rstrip("/") + "/"

AUTH_PUBLIC_BASE = os.getenv("AUTH_PUBLIC_BASE", "http://localhost:9002").rstrip("/")

AUTH_CLIENT_ID = os.getenv("AUTH_CLIENT_ID", "")
AUTH_CLIENT_SECRET = os.getenv("AUTH_CLIENT_SECRET", "")  # optional (public client can be empty)

AUTH_REDIRECT_URI = os.getenv(
    "AUTH_REDIRECT_URI",
    "https://api.tracksante.com/auth/oidc/callback",
)

AUTH_SUCCESS_REDIRECT = os.getenv(
    "AUTH_SUCCESS_REDIRECT",
    "https://tracksante.com/",
)

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

JWT_SECRET = os.getenv("JWT_SECRET", "dev-change-me")
JWT_ISSUER = os.getenv("JWT_ISSUER", "tracksante-api")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "tracksante")

ACCESS_TTL_SECONDS = int(os.getenv("ACCESS_TTL_SECONDS", "900"))        # 15 min
REFRESH_TTL_SECONDS = int(os.getenv("REFRESH_TTL_SECONDS", "2592000"))  # 30 days

AUTH_ENROLLMENT_FLOW_SLUG = os.getenv("AUTH_ENROLLMENT_FLOW_SLUG", "").strip()

REFRESH_COOKIE = "ts_refresh"


app.mount("/assets", StaticFiles(directory="/app/assets"), name="assets")

def _utcnow():
    return datetime.now(timezone.utc)

def make_access_token(user_id: str) -> str:
    now = _utcnow()
    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": user_id,
        "typ": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=ACCESS_TTL_SECONDS)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def make_refresh_token(user_id: str) -> str:
    now = _utcnow()
    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": user_id,
        "typ": "refresh",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=REFRESH_TTL_SECONDS)).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def db():
    return psycopg.connect(DBURL)

# --------- models/auth ----------
class CreateUserResp(BaseModel):
    user_id: str

class DeviceRegisterReq(BaseModel):
    user_id: str
    device_external_id: str
    platform: str | None = None
    capabilities: dict = Field(default_factory=dict)

class DeviceRegisterResp(BaseModel):
    device_id: str

class StartSessionReq(BaseModel):
    user_id: str
    device_external_id: str
    meta: dict = Field(default_factory=dict)

class StartSessionResp(BaseModel):
    session_id: str
    device_id: str

class SensorEventReq(BaseModel):
    device_external_id: str
    session_id: str | None = None
    ts: datetime | None = None
    kind: str
    seq: int | None = None
    data: dict = Field(default_factory=dict)

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
## This being done by migrations now
# @app.on_event("startup")
# def init_db():
#     with db() as conn, conn.cursor() as cur:
#         cur.execute("""
#           CREATE TABLE IF NOT EXISTS images(
#             id BIGSERIAL PRIMARY KEY,
#             device_id TEXT NOT NULL,
#             ts TIMESTAMPTZ NOT NULL DEFAULT now(),
#             key TEXT NOT NULL UNIQUE,
#             sha256 TEXT NOT NULL,
#             mime TEXT,
#             width INT, height INT,
#             created_at TIMESTAMPTZ DEFAULT now()
#           );""")


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")

def _pkce_pair():
    verifier = _b64url(secrets.token_bytes(32))
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge

async def _fetch_oidc_config(issuer: str):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(issuer + ".well-known/openid-configuration")
        r.raise_for_status()
        return r.json()

async def _exchange_code_for_tokens(token_endpoint: str, code: str, code_verifier: str):
    data = {
        "grant_type": "authorization_code",
        "client_id": AUTH_CLIENT_ID,
        "code": code,
        "redirect_uri": AUTH_REDIRECT_URI,
        "code_verifier": code_verifier,
    }

    # If the provider is "confidential", include client_secret
    if AUTH_CLIENT_SECRET:
        data["client_secret"] = AUTH_CLIENT_SECRET

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(token_endpoint, data=data)
        r.raise_for_status()
        return r.json()

async def _userinfo(userinfo_endpoint: str, access_token: str):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            userinfo_endpoint,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        r.raise_for_status()
        return r.json()
    

def upsert_user_from_oidc(issuer: str, sub: str, email: str | None, name: str | None) -> str:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (auth_issuer, auth_sub, email, name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (auth_issuer, auth_sub)
            DO UPDATE SET email=EXCLUDED.email, name=EXCLUDED.name
            RETURNING id::text;
            """,
            (issuer, sub, email, name),
        )
        return cur.fetchone()[0]


# ---------------- OIDC routes ----------------
@app.get("/auth/register")
async def auth_register():
    if not AUTH_ENROLLMENT_FLOW_SLUG:
        raise HTTPException(500, "Missing AUTH_ENROLLMENT_FLOW_SLUG")

    # Public base is what the user's BROWSER can reach (localhost:9002 in your dev setup)
    # Flow URLs are under /if/flow/<slug>/
    register_url = f"{AUTH_PUBLIC_BASE}/if/flow/{AUTH_ENROLLMENT_FLOW_SLUG}/"
    return RedirectResponse(url=register_url, status_code=302)


@app.get("/auth/login")
async def auth_login():
    if not AUTH_CLIENT_ID:
        raise HTTPException(500, "Missing AUTH_CLIENT_ID")

    # 1) Fetch discovery from INTERNAL address (API container can reach this)
    oidc = await _fetch_oidc_config(AUTH_INTERNAL_ISSUER)
    authorize_endpoint = oidc["authorization_endpoint"]

    # 2) Rewrite authorize URL host for the BROWSER (browser must use localhost:9002)
    pub = urlsplit(AUTH_PUBLIC_BASE)         # e.g. http://localhost:9002
    auth = urlsplit(authorize_endpoint)      # e.g. http://authentik-server:9000/...

    authorize_endpoint = urlunsplit((
        pub.scheme,      # http
        pub.netloc,      # localhost:9002
        auth.path,       # /application/o/track...
        auth.query,
        auth.fragment,
    ))

    state = secrets.token_urlsafe(24)
    verifier, challenge = _pkce_pair()

    resp = RedirectResponse(
        url=(
            f"{authorize_endpoint}"
            f"?client_id={AUTH_CLIENT_ID}"
            f"&response_type=code"
            f"&redirect_uri={httpx.URL(AUTH_REDIRECT_URI)}"
            f"&scope=openid%20email%20profile"
            f"&state={state}"
            f"&code_challenge={challenge}"
            f"&code_challenge_method=S256"
        ),
        status_code=302,
    )
    resp.set_cookie("oidc_state", state, httponly=True, samesite="lax")
    resp.set_cookie("oidc_verifier", verifier, httponly=True, samesite="lax")
    return resp


@app.post("/auth/logout")
async def auth_logout(response: Response):
    # Clear refresh cookie
    response.delete_cookie(
        key=REFRESH_COOKIE,
        path="/",
        samesite="lax",     # keep consistent with how you set it
        secure=True,       # localhost dev; set True on https
    )
    return {"ok": True}


@app.get("/auth/oidc/callback")
async def auth_oidc_callback(request: Request, code: str | None = None, state: str | None = None):
    if not code or not state:
        raise HTTPException(400, "Missing code or state")

    cookie_state = request.cookies.get("oidc_state")
    verifier = request.cookies.get("oidc_verifier")
    if not cookie_state or not verifier or cookie_state != state:
        raise HTTPException(400, "Invalid state")

    oidc = await _fetch_oidc_config(AUTH_INTERNAL_ISSUER)
    tokens = await _exchange_code_for_tokens(oidc["token_endpoint"], code, verifier)

    access_token = tokens.get("access_token")
    if not access_token:
        raise HTTPException(400, "No access_token returned")

    info = await _userinfo(oidc["userinfo_endpoint"], access_token)

    sub = info.get("sub")
    if not sub:
        raise HTTPException(400, "No sub in userinfo")

    email = info.get("email")
    name = info.get("name") or info.get("preferred_username")

    issuer = oidc["issuer"]
    user_id = upsert_user_from_oidc(issuer=issuer, sub=sub, email=email, name=name)

    # Create refresh token (stored in HttpOnly cookie)
    refresh_jwt = make_refresh_token(user_id)

    resp = RedirectResponse(url=AUTH_SUCCESS_REDIRECT, status_code=302)

    resp.set_cookie(
        key=REFRESH_COOKIE,
        value=refresh_jwt,
        httponly=True,
        samesite="lax",
        secure=True,  # set True when behind HTTPS in production
        path="/",
    )

    resp.delete_cookie("oidc_state")
    resp.delete_cookie("oidc_verifier")
    return resp

@app.post("/auth/refresh")
def auth_refresh(request: Request):
    token = request.cookies.get(REFRESH_COOKIE)
    if not token:
        raise HTTPException(401, "Missing refresh token")

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
    except Exception:
        raise HTTPException(401, "Invalid refresh token")

    if payload.get("typ") != "refresh":
        raise HTTPException(401, "Wrong token type")

    user_id = payload["sub"]
    access = make_access_token(user_id)
    return {"access_token": access}


bearer = HTTPBearer(auto_error=False)

@app.get("/auth/me")
def auth_me(creds: HTTPAuthorizationCredentials = Depends(bearer)):
    if not creds:
        raise HTTPException(401, "Missing bearer token")

    try:
        payload = jwt.decode(
            creds.credentials,
            JWT_SECRET,
            algorithms=["HS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
        )
    except Exception:
        raise HTTPException(401, "Invalid token")

    if payload.get("typ") != "access":
        raise HTTPException(401, "Wrong token type")

    return {"user_id": payload["sub"]}


# --------- routes ----------
@app.get("/api/health")
def health():
    return {"ok": True, "time": time.time()}

@app.get("/callback")
async def callback(request: Request):
    return {"query": dict(request.query_params)}

@app.post("/api/users", response_model=CreateUserResp)
def create_user():
    with db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO users DEFAULT VALUES RETURNING id::text;")
        (user_id,) = cur.fetchone()
    return {"user_id": user_id}

@app.post("/api/devices/register", response_model=DeviceRegisterResp)
def register_device(req: DeviceRegisterReq):
    with db() as conn, conn.cursor() as cur:
        # upsert device
        cur.execute(
            """
            INSERT INTO devices (user_id, device_external_id, platform, last_seen_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (device_external_id)
            DO UPDATE SET user_id=EXCLUDED.user_id,
                          platform=COALESCE(EXCLUDED.platform, devices.platform),
                          last_seen_at=now()
            RETURNING id::text;
            """,
            (req.user_id, req.device_external_id, req.platform),
        )
        (device_id,) = cur.fetchone()

        # overwrite capabilities snapshot
        cur.execute(
            """
            INSERT INTO device_capabilities (device_id, capabilities, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (device_id)
            DO UPDATE SET capabilities=EXCLUDED.capabilities, updated_at=now();
            """,
            (device_id, psyjson.Json(req.capabilities)),
        )

    return {"device_id": device_id}


@app.post("/api/sessions/start", response_model=StartSessionResp)
def start_session(req: StartSessionReq):
    with db() as conn, conn.cursor() as cur:
        # lookup device_id by external id
        cur.execute(
            "SELECT id::text FROM devices WHERE device_external_id=%s;",
            (req.device_external_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Device not registered")
        (device_id,) = row

        cur.execute(
            """
            INSERT INTO user_sessions (user_id, device_id, started_at, meta)
            VALUES (%s, %s, now(), %s::jsonb)
            RETURNING id::text;
            """,
            (req.user_id, device_id, _json.dumps(req.meta)),
        )
        (session_id,) = cur.fetchone()

    return {"session_id": session_id, "device_id": device_id}

@app.post("/api/sensors/events")
def sensor_event(req: SensorEventReq):
    ts = req.ts or datetime.now(timezone.utc)

    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT id::text FROM devices WHERE device_external_id=%s;", (req.device_external_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Device not registered")
        (device_id,) = row

        if req.session_id:
            cur.execute(
                "SELECT 1 FROM user_sessions WHERE id=%s AND (device_id=%s OR device_id IS NULL) LIMIT 1;",
                (req.session_id, device_id),
            )
            if not cur.fetchone():
                raise HTTPException(400, "Invalid session_id for this device")

        cur.execute(
            """
            INSERT INTO sensor_events (session_id, device_id, ts, kind, seq, data)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (device_id, kind, seq) DO NOTHING
            """,
            (req.session_id, device_id, ts, req.kind, req.seq, _json.dumps(req.data)),
        )
        stored = cur.rowcount == 1

    return {"ok": True, "stored": stored}

@app.get("/api/sensors/last")
def last(device_external_id: str, kind: str, n: int = Query(10, ge=1, le=100)):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts, seq, data
            FROM sensor_events
            WHERE device_id = (SELECT id FROM devices WHERE device_external_id=%s)
              AND kind = %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (device_external_id, kind, n),
        )
        rows = cur.fetchall()

    return [{"ts": r[0].isoformat(), "seq": r[1], "data": r[2]} for r in rows]

class ImgRow(BaseModel):
    id: int
    device_id: str | None = None
    session_id: str | None = None
    ts: str
    bucket: str
    key: str
    sha256: str
    mime: str | None = None
    width: int | None = None
    height: int | None = None

@app.post("/api/images/upload")
async def upload_image(
    file: UploadFile = File(...),
    device_external_id: str = Form(...),
    session_id: str | None = Form(None),
):
    data = await file.read()
    sha = hashlib.sha256(data).hexdigest()
    ext = os.path.splitext(file.filename or "")[-1].lower() or ".jpg"
    day = time.strftime("%Y/%m/%d")
    key = f"{device_external_id}/{day}/{sha}{ext}"

    # lookup device_id
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT id::text FROM devices WHERE device_external_id=%s;", (device_external_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Device not registered")
        (device_id,) = row

    S3.put_object(
        Bucket=BKT,
        Key=key,
        Body=data,
        ContentType=file.content_type or "application/octet-stream",
    )

    w = h = None
    try:
        from PIL import Image
        im = Image.open(io.BytesIO(data))
        w, h = im.size
    except Exception:
        pass

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO images(session_id, device_id, bucket, key, sha256, mime, width, height)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id::text, ts;
            """,
            (session_id, device_id, BKT, key, sha, file.content_type, w, h),
        )
        img_id, ts = cur.fetchone()

    return {"id": img_id, "device_id": device_id, "session_id": session_id, "key": key, "ts": ts.isoformat()}


@app.get("/api/images/list", response_model=list[ImgRow])
def list_images(device_external_id: str | None = None, session_id: str | None = None, limit: int = 20):
    q = """
      SELECT i.id,
             i.device_id::text,
             i.session_id::text,
             i.ts,
             i.bucket,
             i.key,
             i.sha256,
             i.mime,
             i.width,
             i.height
      FROM images i
    """
    where = []
    params = []

    if device_external_id:
        where.append("i.device_id = (SELECT id FROM devices WHERE device_external_id = %s)")
        params.append(device_external_id)

    if session_id:
        where.append("i.session_id = %s")
        params.append(session_id)

    if where:
        q += " WHERE " + " AND ".join(where)

    q += " ORDER BY i.ts DESC LIMIT %s"
    params.append(limit)

    with db() as conn, conn.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()

    return [
        ImgRow(
            id=r[0],
            device_id=r[1],
            session_id=r[2],
            ts=r[3].isoformat(),
            bucket=r[4],
            key=r[5],
            sha256=r[6],
            mime=r[7],
            width=r[8],
            height=r[9],
        )
        for r in rows
    ]

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