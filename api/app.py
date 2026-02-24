# --- keep imports at top ---
import os, io, time, hashlib
import base64, secrets
import httpx
import uuid
from typing import Optional, Any
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
from psycopg.errors import UniqueViolation
import json as _json
from urllib.parse import urlsplit, urlunsplit, urlencode
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

AUTH_REDIRECT_URI_WEB = os.getenv("AUTH_REDIRECT_URI_WEB", "https://api.tracksante.com/auth/oidc/callback")
AUTH_REDIRECT_URI_MOBILE = os.getenv("AUTH_REDIRECT_URI_MOBILE", "https://tracksante.com/mobile/callback")

REFRESH_COOKIE = "ts_refresh"
PROFILE_FILE_MAX_MB = int(os.getenv("PROFILE_FILE_MAX_MB", "20"))


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

class MobileExchangeReq(BaseModel):
    code: str
    code_verifier: str

class UserProfileUpdateReq(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    sex: str | None = None
    age: int | None = Field(default=None, ge=0, le=130)
    weight_kg: float | None = Field(default=None, gt=0, le=500)
    height_cm: float | None = Field(default=None, gt=0, le=300)
    medical_history: str | None = None
    medical_history_file_key: str | None = None

class UserProfileResp(BaseModel):
    user_id: str
    profile: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime | None = None

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

async def _exchange_code_for_tokens(token_endpoint: str, code: str, code_verifier: str, redirect_uri: str):
    data = {
        "grant_type": "authorization_code",
        "client_id": AUTH_CLIENT_ID,
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }
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
        # 1) First: if we already have this OIDC identity, update profile fields and return.
        cur.execute(
            """
            SELECT id::text
            FROM users
            WHERE auth_issuer = %s AND auth_sub = %s
            """,
            (issuer, sub),
        )
        row = cur.fetchone()
        if row:
            user_id = row[0]
            cur.execute(
                """
                UPDATE users
                SET email = COALESCE(%s, email),
                    name  = COALESCE(%s, name)
                WHERE id = %s
                RETURNING id::text
                """,
                (email, name, user_id),
            )
            return cur.fetchone()[0]

        # 2) If no (issuer, sub) match, try to link by email if present.
        if email:
            cur.execute(
                """
                SELECT id::text
                FROM users
                WHERE lower(email) = lower(%s)
                """,
                (email,),
            )
            row = cur.fetchone()
            if row:
                user_id = row[0]
                # Link this existing user to this OIDC identity
                cur.execute(
                    """
                    UPDATE users
                    SET auth_issuer = %s,
                        auth_sub    = %s,
                        name        = COALESCE(%s, name),
                        email       = COALESCE(%s, email)
                    WHERE id = %s
                    RETURNING id::text
                    """,
                    (issuer, sub, name, email, user_id),
                )
                return cur.fetchone()[0]

        # 3) Otherwise insert fresh.
        try:
            cur.execute(
                """
                INSERT INTO users (auth_issuer, auth_sub, email, name)
                VALUES (%s, %s, %s, %s)
                RETURNING id::text
                """,
                (issuer, sub, email, name),
            )
            return cur.fetchone()[0]
        except UniqueViolation:
            # Very rare race / edge case: email became taken between our check and insert.
            # Re-run the email-link path once.
            conn.rollback()
            if not email:
                raise
            with conn.cursor() as cur2:
                cur2.execute(
                    "SELECT id::text FROM users WHERE lower(email) = lower(%s)",
                    (email,),
                )
                row = cur2.fetchone()
                if not row:
                    raise
                user_id = row[0]
                cur2.execute(
                    """
                    UPDATE users
                    SET auth_issuer = %s,
                        auth_sub    = %s,
                        name        = COALESCE(%s, name)
                    WHERE id = %s
                    RETURNING id::text
                    """,
                    (issuer, sub, name, user_id),
                )
                return cur2.fetchone()[0]


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
async def auth_login(
    client: str = Query("web", pattern="^(web|mobile)$"),
    next: str | None = None,

    # ✅ allow mobile app to provide these
    state: str | None = None,
    code_challenge: str | None = None,
    code_challenge_method: str = "S256",
):
    if not AUTH_CLIENT_ID:
        raise HTTPException(500, "Missing AUTH_CLIENT_ID")

    redirect_uri = AUTH_REDIRECT_URI_WEB if client == "web" else AUTH_REDIRECT_URI_MOBILE

    oidc = await _fetch_oidc_config(AUTH_INTERNAL_ISSUER)
    authorize_endpoint = oidc["authorization_endpoint"]

    # rewrite to public base for browser
    pub = urlsplit(AUTH_PUBLIC_BASE)
    auth = urlsplit(authorize_endpoint)
    authorize_endpoint = urlunsplit((pub.scheme, pub.netloc, auth.path, auth.query, auth.fragment))

    if client == "mobile":
        # ✅ MUST use the app-provided values
        if not state or not code_challenge:
            raise HTTPException(400, "Missing state or code_challenge for mobile login")

        url = (
            f"{authorize_endpoint}"
            f"?client_id={AUTH_CLIENT_ID}"
            f"&response_type=code"
            f"&redirect_uri={httpx.URL(redirect_uri)}"
            f"&scope=openid%20email%20profile"
            f"&state={state}"
            f"&code_challenge={code_challenge}"
            f"&code_challenge_method={code_challenge_method}"
        )
        return RedirectResponse(url=url, status_code=302)

    # ---- web flow: backend generates state+pkce and uses cookies ----
    web_state = secrets.token_urlsafe(24)
    verifier, challenge = _pkce_pair()

    resp = RedirectResponse(
        url=(
            f"{authorize_endpoint}"
            f"?client_id={AUTH_CLIENT_ID}"
            f"&response_type=code"
            f"&redirect_uri={httpx.URL(redirect_uri)}"
            f"&scope=openid%20email%20profile"
            f"&state={web_state}"
            f"&code_challenge={challenge}"
            f"&code_challenge_method=S256"
        ),
        status_code=302,
    )

    resp.set_cookie("oidc_state", web_state, httponly=True, samesite="none", secure=True)
    resp.set_cookie("oidc_verifier", verifier, httponly=True, samesite="none", secure=True)
    resp.set_cookie("oidc_client", "web", httponly=True, samesite="none", secure=True)
    if next:
        resp.set_cookie("oidc_next", next, httponly=True, samesite="none", secure=True)

    return resp


@app.post("/auth/mobile/exchange")
async def auth_mobile_exchange(req: MobileExchangeReq):

    oidc = await _fetch_oidc_config(AUTH_INTERNAL_ISSUER)

    tokens = await _exchange_code_for_tokens(
        oidc["token_endpoint"],
        req.code,
        req.code_verifier,
        AUTH_REDIRECT_URI_MOBILE,
    )

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

    # your own app tokens
    refresh_jwt = make_refresh_token(user_id)
    access_jwt = make_access_token(user_id)

    # Return JSON to the app (store refresh securely in flutter)
    return {
        "user_id": user_id,
        "access_token": access_jwt,
        "refresh_token": refresh_jwt,
    }


@app.post("/auth/logout")
async def auth_logout(response: Response):
    # Clear refresh cookie
    response.delete_cookie(
        key=REFRESH_COOKIE,
        path="/",
        samesite="none",     # keep consistent with how you set it
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

    client_kind = request.cookies.get("oidc_client", "web")
    redirect_uri = AUTH_REDIRECT_URI_WEB if client_kind == "web" else AUTH_REDIRECT_URI_MOBILE

    tokens = await _exchange_code_for_tokens(oidc["token_endpoint"], code, verifier, redirect_uri)

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

    # set refresh cookie for api.tracksante.com
    refresh_jwt = make_refresh_token(user_id)

    # decide where to send the browser next
    if client_kind == "mobile":
        # redirect to the App Link page so Android opens the app
        # (pass code/state through so the app can finish login if you want)
        next_url = f"https://tracksante.com/mobile/callback?{urlencode({'code': code, 'state': state})}"
    else:
        # optional "next" support
        next_url = request.cookies.get("oidc_next") or AUTH_SUCCESS_REDIRECT

    resp = RedirectResponse(url=next_url, status_code=302)

    resp.set_cookie(
        key=REFRESH_COOKIE,
        value=refresh_jwt,
        httponly=True,
        samesite="none",
        secure=True,
        path="/",
    )

    # cleanup
    resp.delete_cookie("oidc_state")
    resp.delete_cookie("oidc_verifier")
    resp.delete_cookie("oidc_client")
    resp.delete_cookie("oidc_next")

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

def require_access_user_id(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> str:
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

    return payload["sub"]

@app.get("/auth/me")
def auth_me(user_id: str = Depends(require_access_user_id)):
    return {"user_id": user_id}


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

@app.get("/api/profile", response_model=UserProfileResp)
def get_profile(user_id: str = Depends(require_access_user_id)):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT profile, updated_at
            FROM user_profile
            WHERE user_id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row:
        return {"user_id": user_id, "profile": {}, "updated_at": None}

    return {"user_id": user_id, "profile": row[0] or {}, "updated_at": row[1]}

@app.patch("/api/profile", response_model=UserProfileResp)
def update_profile(req: UserProfileUpdateReq, user_id: str = Depends(require_access_user_id)):
    patch = req.model_dump(exclude_unset=True)
    if not patch:
        return get_profile(user_id)

    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id=%s LIMIT 1;", (user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "User not found")

        cur.execute(
            """
            INSERT INTO user_profile (user_id, profile, updated_at)
            VALUES (%s, %s::jsonb, now())
            ON CONFLICT (user_id)
            DO UPDATE SET
              profile = COALESCE(user_profile.profile, '{}'::jsonb) || EXCLUDED.profile,
              updated_at = now()
            RETURNING profile, updated_at
            """,
            (user_id, _json.dumps(patch)),
        )
        profile, updated_at = cur.fetchone()

    return {"user_id": user_id, "profile": profile or {}, "updated_at": updated_at}

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

class ProfileFileUploadResp(BaseModel):
    id: str
    session_id: str | None = None
    key: str
    mime: str
    size_bytes: int
    sha256: str
    ts: str

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

@app.post("/api/files/upload", response_model=ProfileFileUploadResp)
async def upload_profile_file(
    file: UploadFile = File(...),
    session_id: str | None = Form(None),
    user_id: str = Depends(require_access_user_id),
):
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")

    max_bytes = PROFILE_FILE_MAX_MB * 1024 * 1024
    if len(data) > max_bytes:
        raise HTTPException(413, f"File too large (max {PROFILE_FILE_MAX_MB} MB)")

    # Profile history uploads are intentionally strict: PDF only.
    ext = os.path.splitext(file.filename or "")[-1].lower()
    mime = (file.content_type or "").lower()
    is_pdf = ext == ".pdf" or mime == "application/pdf"
    if not is_pdf:
        raise HTTPException(415, "Only PDF files are allowed")

    safe_name = os.path.basename(file.filename or "history.pdf")
    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"

    if session_id:
        try:
            uuid.UUID(session_id)
        except ValueError:
            raise HTTPException(400, "session_id must be a UUID")

    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id=%s LIMIT 1;", (user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "User not found")

        if session_id:
            cur.execute(
                "SELECT 1 FROM user_sessions WHERE id=%s AND user_id=%s LIMIT 1;",
                (session_id, user_id),
            )
            if not cur.fetchone():
                raise HTTPException(400, "Invalid session_id for this user")

    sha = hashlib.sha256(data).hexdigest()
    day = time.strftime("%Y/%m/%d")
    key = f"profiles/{user_id}/{day}/{sha}-{safe_name}"

    S3.put_object(
        Bucket=BKT,
        Key=key,
        Body=data,
        ContentType="application/pdf",
    )

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO images(session_id, device_id, bucket, key, sha256, mime, width, height)
            VALUES (%s, NULL, %s, %s, %s, %s, NULL, NULL)
            RETURNING id::text, ts;
            """,
            (session_id, BKT, key, sha, "application/pdf"),
        )
        file_id, ts = cur.fetchone()

    return {
        "id": file_id,
        "session_id": session_id,
        "key": key,
        "mime": "application/pdf",
        "size_bytes": len(data),
        "sha256": sha,
        "ts": ts.isoformat(),
    }


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

    try:
        uuid.UUID(req.user_id)
    except ValueError:
        raise HTTPException(400, "user_id must be a valid UUID from users.id")

    session_id = req.session_id
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id=%s LIMIT 1;", (req.user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "User not found")

        # Validate/create DB-backed session so chat rows always link to a user.
        if session_id:
            try:
                uuid.UUID(session_id)
            except ValueError:
                # Backward compatibility: older clients may send timestamp-style session IDs.
                # Treat those as "no session" and create a fresh DB-backed UUID session.
                session_id = None

            if session_id:
                cur.execute(
                    "SELECT 1 FROM user_sessions WHERE id=%s AND user_id=%s LIMIT 1;",
                    (session_id, req.user_id),
                )
                if not cur.fetchone():
                    raise HTTPException(400, "Invalid session_id for this user")
        else:
            cur.execute(
                """
                INSERT INTO user_sessions (user_id, started_at, meta)
                VALUES (%s, now(), %s::jsonb)
                RETURNING id::text;
                """,
                (req.user_id, _json.dumps({"source": "api_chat"})),
            )
            (session_id,) = cur.fetchone()

    chat_result = await run_in_threadpool(
        chat_once,
        user_id=req.user_id,
        user_message=req.message,
        session_id=session_id
    )

    assistant_message = chat_result.get("assistant_message")
    if not isinstance(assistant_message, str):
        assistant_message = _json.dumps(chat_result, ensure_ascii=False)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO session_messages (session_id, role, content, meta)
            VALUES (%s, 'user', %s, %s::jsonb)
            """,
            (session_id, req.message, _json.dumps({})),
        )
        cur.execute(
            """
            INSERT INTO session_messages (session_id, role, content, meta)
            VALUES (%s, 'assistant', %s, %s::jsonb)
            """,
            (session_id, assistant_message, _json.dumps(chat_result)),
        )

        recommended_sensor = chat_result.get("recommended_sensor")
        suggested_next_step = chat_result.get("suggested_next_step")
        recommended_measurements = chat_result.get("recommended_measurements")
        if not isinstance(recommended_measurements, list):
            recommended_measurements = []
        sensor_confidence = chat_result.get("sensor_confidence")
        is_actionable_sensor = (
            recommended_sensor in {
                "Humidity Sensor",
                "CO2 Sensor",
                "Temperature Sensor",
                "Heart Rate Sensor",
                "Blood Oxygen Sensor",
                # Backward compatibility for older prompt/output naming.
                "Pulse Oxygen Sensor",
            }
            and suggested_next_step == "proceed_to_measurements"
        )

        session_signal = {
            "last_recommended_sensor": recommended_sensor,
            "last_suggested_next_step": suggested_next_step,
            "last_recommended_measurements": recommended_measurements,
            "last_sensor_confidence": sensor_confidence,
            "last_is_actionable_sensor": is_actionable_sensor,
            "last_assistant_message": assistant_message,
            "last_chat_at": _utcnow().isoformat(),
        }
        cur.execute(
            """
            UPDATE user_sessions
            SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
            WHERE id = %s AND user_id = %s
            """,
            (_json.dumps(session_signal), session_id, req.user_id),
        )

        collected_facts = chat_result.get("collected_facts")
        if isinstance(collected_facts, dict) and collected_facts:
            cur.execute(
                """
                INSERT INTO user_memory (user_id, memory, updated_at)
                VALUES (%s, %s::jsonb, now())
                ON CONFLICT (user_id)
                DO UPDATE SET
                  memory = user_memory.memory || EXCLUDED.memory,
                  updated_at = now();
                """,
                (req.user_id, _json.dumps(collected_facts)),
            )

    chat_result["session_id"] = session_id
    return chat_result
