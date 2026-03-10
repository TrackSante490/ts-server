# --- keep imports at top ---
import os, io, time, hashlib
import secrets
import logging
import base64
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
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from urllib.parse import urlsplit, urlunsplit, urlencode
import jwt
from observability import (
    observe_chat_request,
    observe_dependency_probe,
    observe_http_request,
    observe_rag_chat_turn,
    observe_rag_error,
    observe_sensor_event,
)

# ---- logging ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("tracksante.api")

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
REPORT_MODEL_DEFAULT = os.getenv("REPORT_MODEL", os.getenv("OLLAMA_CHAT_MODEL", "llama3.1"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
METRICS_PROBE_TIMEOUT_SECONDS = float(os.getenv("METRICS_PROBE_TIMEOUT_SECONDS", "2.0"))

ACCESS_TTL_SECONDS = int(os.getenv("ACCESS_TTL_SECONDS", "900"))        # 15 min
REFRESH_TTL_SECONDS = int(os.getenv("REFRESH_TTL_SECONDS", "2592000"))  # 30 days

AUTH_ENROLLMENT_FLOW_SLUG = os.getenv("AUTH_ENROLLMENT_FLOW_SLUG", "").strip()

DEFAULT_MOBILE_REDIRECT_URI = "tracksante://auth/callback"
AUTH_REDIRECT_URI_WEB = os.getenv("AUTH_REDIRECT_URI_WEB", "https://api.tracksante.com/auth/oidc/callback")
AUTH_REDIRECT_URI_MOBILE = os.getenv("AUTH_REDIRECT_URI_MOBILE", DEFAULT_MOBILE_REDIRECT_URI)
AUTH_ALLOWED_MOBILE_REDIRECT_URIS = tuple(
    dict.fromkeys(
        uri.strip()
        for uri in os.getenv(
            "AUTH_ALLOWED_MOBILE_REDIRECT_URIS",
            f"{DEFAULT_MOBILE_REDIRECT_URI},{AUTH_REDIRECT_URI_MOBILE},https://tracksante.com/mobile/callback",
        ).split(",")
        if uri.strip()
    )
)

REFRESH_COOKIE = "ts_refresh"
PROFILE_FILE_MAX_MB = int(os.getenv("PROFILE_FILE_MAX_MB", "20"))


app.mount("/assets", StaticFiles(directory="/app/assets"), name="assets")


@app.middleware("http")
async def prometheus_http_middleware(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    start = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        observe_http_request(
            method=request.method,
            route=route_path,
            status=status_code,
            duration_seconds=time.perf_counter() - start,
        )

def _build_url_with_query(base_url: str, params: dict[str, str]) -> str:
    return f"{base_url}?{urlencode(params)}"

def _validate_mobile_redirect_uri(redirect_uri: str | None) -> str:
    candidate = (redirect_uri or "").strip()
    if not candidate:
        raise HTTPException(400, "Missing redirect_uri")
    if candidate not in AUTH_ALLOWED_MOBILE_REDIRECT_URIS:
        raise HTTPException(400, "Unsupported redirect_uri")
    return candidate

def _default_mobile_redirect_uri() -> str:
    if DEFAULT_MOBILE_REDIRECT_URI in AUTH_ALLOWED_MOBILE_REDIRECT_URIS:
        return DEFAULT_MOBILE_REDIRECT_URI
    return AUTH_REDIRECT_URI_MOBILE

def _legacy_auth_callback_redirect_uri() -> str:
    legacy_suffix = "/auth/oidc/callback"
    if AUTH_REDIRECT_URI_WEB.endswith(legacy_suffix):
        return AUTH_REDIRECT_URI_WEB[: -len(legacy_suffix)] + "/auth/callback"
    return AUTH_REDIRECT_URI_WEB

def _utcnow():
    return datetime.now(timezone.utc)

def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None

def build_report_payload(user_id: str, limit_sessions: int, limit_events: int) -> dict[str, Any]:
    # Build a compact payload for report generation.
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, email, name, created_at
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        user_row = cur.fetchone()
        if not user_row:
            raise HTTPException(404, "User not found")

        cur.execute(
            """
            SELECT profile, updated_at
            FROM user_profile
            WHERE user_id = %s
            """,
            (user_id,),
        )
        profile_row = cur.fetchone()
        profile = profile_row[0] if profile_row else {}
        profile_updated_at = profile_row[1] if profile_row else None

        profile_data: dict[str, Any] = profile if isinstance(profile, dict) else {}
        nested_profile = profile_data.get("profile") if isinstance(profile_data, dict) else None
        if isinstance(nested_profile, dict):
            # Some clients may nest fields under "profile"; flatten with nested taking precedence.
            profile_data = {**profile_data, **nested_profile}

        first_name = None
        last_name = None
        if isinstance(profile_data, dict):
            first_name = profile_data.get("first_name")
            last_name = profile_data.get("last_name")
        full_name = " ".join([p for p in [first_name, last_name] if p]) or (user_row[2] or None)

        def _coalesce(*vals):
            for v in vals:
                if v is None:
                    continue
                if isinstance(v, str) and not v.strip():
                    continue
                if isinstance(v, list) and len(v) == 0:
                    continue
                if isinstance(v, dict) and len(v) == 0:
                    continue
                return v
            return None

        def _as_number(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return v
            if isinstance(v, str):
                try:
                    return float(v)
                except ValueError:
                    return None
            return None

        def _extract_from_data(data: dict, keys: list[str]):
            if not isinstance(data, dict):
                return None
            for k in keys:
                if k in data:
                    n = _as_number(data.get(k))
                    if n is not None:
                        return n
            return None

        def _latest_measurement(events: list[dict], kinds: set[str], keys: list[str]):
            for ev in events:
                if ev.get("kind") in kinds:
                    n = _extract_from_data(ev.get("data") or {}, keys)
                    if n is not None:
                        return n
                n = _extract_from_data(ev.get("data") or {}, keys)
                if n is not None:
                    return n
            return None

        def _latest_bp(events: list[dict]):
            for ev in events:
                data = ev.get("data") or {}
                if not isinstance(data, dict):
                    continue
                if "bp" in data and isinstance(data.get("bp"), str):
                    return data.get("bp")
                sys = _extract_from_data(data, ["systolic", "sys", "bp_systolic"])
                dia = _extract_from_data(data, ["diastolic", "dia", "bp_diastolic"])
                if sys is not None and dia is not None:
                    return f"{int(sys)}/{int(dia)}"
            return None

        patient_info = {
            "user_id": user_row[0],
            "email": user_row[1],
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
            "sex": profile_data.get("sex") if isinstance(profile_data, dict) else None,
            "age": profile_data.get("age") if isinstance(profile_data, dict) else None,
            "weight_kg": profile_data.get("weight_kg") if isinstance(profile_data, dict) else None,
            "height_cm": profile_data.get("height_cm") if isinstance(profile_data, dict) else None,
            "medical_history": profile_data.get("medical_history") if isinstance(profile_data, dict) else None,
            "medical_history_file_key": profile_data.get("medical_history_file_key") if isinstance(profile_data, dict) else None,
        }

        cur.execute(
            """
            SELECT memory, updated_at
            FROM user_memory
            WHERE user_id = %s
            """,
            (user_id,),
        )
        memory_row = cur.fetchone()
        memory = memory_row[0] if memory_row else {}
        memory_updated_at = memory_row[1] if memory_row else None

        cur.execute(
            """
            SELECT id::text, started_at, ended_at, meta
            FROM user_sessions
            WHERE user_id = %s
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (user_id, limit_sessions),
        )
        sessions = [
            {
                "id": r[0],
                "started_at": _iso(r[1]),
                "ended_at": _iso(r[2]),
                "meta": r[3] or {},
            }
            for r in cur.fetchall()
        ]

        sensor_events: list[dict] = []
        if limit_events > 0:
            cur.execute(
                """
                SELECT se.id,
                       se.session_id::text,
                       se.device_id::text,
                       se.ts,
                       se.kind,
                       se.seq,
                       se.data
                FROM sensor_events se
                JOIN user_sessions us ON us.id = se.session_id
                WHERE us.user_id = %s
                ORDER BY se.ts DESC
                LIMIT %s
                """,
                (user_id, limit_events),
            )
            sensor_events = [
                {
                    "id": r[0],
                    "session_id": r[1],
                    "device_id": r[2],
                    "ts": _iso(r[3]),
                    "kind": r[4],
                    "seq": r[5],
                    "data": r[6] or {},
                }
                for r in cur.fetchall()
            ]

        sensor_vitals = {}
        sensor_summary = {}
        if sensor_events:
            def _collect_values(kinds: set[str], keys: list[str]):
                vals = []
                for ev in sensor_events:
                    if ev.get("kind") in kinds:
                        n = _extract_from_data(ev.get("data") or {}, keys)
                        if n is not None:
                            vals.append(n)
                        continue
                    n = _extract_from_data(ev.get("data") or {}, keys)
                    if n is not None:
                        vals.append(n)
                return vals

            def _range(vals: list[float], min_allowed: float | None = None, max_allowed: float | None = None):
                cleaned = []
                for v in vals:
                    if v is None:
                        continue
                    if min_allowed is not None and v < min_allowed:
                        continue
                    if max_allowed is not None and v > max_allowed:
                        continue
                    cleaned.append(v)
                if not cleaned:
                    return None
                return {"min": min(cleaned), "max": max(cleaned)}

            hr = _latest_measurement(
                sensor_events,
                {"hr", "heart_rate", "pulse"},
                ["hr", "heart_rate", "pulse", "bpm", "value"],
            )
            spo2 = _latest_measurement(
                sensor_events,
                {"spo2", "pulse_ox", "pulse_oxygen", "oxygen"},
                ["spo2", "oxygen_saturation", "oxygen", "o2", "value"],
            )
            temp = _latest_measurement(
                sensor_events,
                {"temp", "temperature", "body_temp"},
                ["temp", "temperature", "celsius", "value"],
            )
            rr = _latest_measurement(
                sensor_events,
                {"rr", "resp_rate", "respiratory_rate"},
                ["rr", "resp_rate", "respiratory_rate", "value"],
            )
            bp = _latest_bp(sensor_events)
            if hr is not None:
                sensor_vitals["hr"] = hr
            if spo2 is not None:
                sensor_vitals["spo2"] = spo2
            if temp is not None:
                sensor_vitals["temp"] = temp
            if rr is not None:
                sensor_vitals["rr"] = rr
            if bp is not None:
                sensor_vitals["bp"] = bp

            hr_vals = _collect_values({"hr", "heart_rate", "pulse"}, ["hr", "heart_rate", "pulse", "bpm", "value"])
            spo2_vals = _collect_values({"spo2", "pulse_ox", "pulse_oxygen", "oxygen"}, ["spo2", "oxygen_saturation", "oxygen", "o2", "value"])
            temp_vals = _collect_values({"temp", "temperature", "body_temp"}, ["temp", "temperature", "celsius", "value"])
            rr_vals = _collect_values({"rr", "resp_rate", "respiratory_rate"}, ["rr", "resp_rate", "respiratory_rate", "value"])

            hr_range = _range(hr_vals, min_allowed=1)
            spo2_range = _range(spo2_vals, min_allowed=1, max_allowed=100)
            temp_range = _range(temp_vals, min_allowed=1)
            rr_range = _range(rr_vals, min_allowed=1)

            if hr_range:
                sensor_summary["hr"] = hr_range
            if spo2_range:
                sensor_summary["spo2"] = spo2_range
            if temp_range:
                sensor_summary["temp"] = temp_range
            if rr_range:
                sensor_summary["rr"] = rr_range

        # Attachments (images) for report context
        cur.execute(
            """
            SELECT i.key, i.ts
            FROM images i
            JOIN user_sessions us ON us.id = i.session_id
            WHERE us.user_id = %s
            ORDER BY i.ts DESC
            LIMIT 20
            """,
            (user_id,),
        )
        attachments = [r[0] for r in cur.fetchall()]

        # Top-level keys expected by report_gen templates
        name = _coalesce(
            patient_info.get("full_name"),
            patient_info.get("first_name"),
            user_row[2],
        )
        age = _coalesce(patient_info.get("age"), memory.get("age") if isinstance(memory, dict) else None)
        sex = _coalesce(patient_info.get("sex"), memory.get("sex") if isinstance(memory, dict) else None)
        allergies = _coalesce(
            profile_data.get("allergies") if isinstance(profile_data, dict) else None,
            memory.get("allergies") if isinstance(memory, dict) else None,
        )
        medications = _coalesce(
            profile_data.get("medications") if isinstance(profile_data, dict) else None,
            memory.get("medications") if isinstance(memory, dict) else None,
        )
        chief_complaint = _coalesce(
            profile_data.get("chief_complaint") if isinstance(profile_data, dict) else None,
            memory.get("chief_complaint") if isinstance(memory, dict) else None,
        )
        history_of_present_illness = _coalesce(
            profile_data.get("history_of_present_illness") if isinstance(profile_data, dict) else None,
            memory.get("history_of_present_illness") if isinstance(memory, dict) else None,
            memory.get("hpi") if isinstance(memory, dict) else None,
        )
        past_medical_history = _coalesce(
            profile_data.get("past_medical_history") if isinstance(profile_data, dict) else None,
            memory.get("past_medical_history") if isinstance(memory, dict) else None,
            profile_data.get("medical_history") if isinstance(profile_data, dict) else None,
            memory.get("medical_history") if isinstance(memory, dict) else None,
        )
        family_history = _coalesce(
            profile_data.get("family_history") if isinstance(profile_data, dict) else None,
            memory.get("family_history") if isinstance(memory, dict) else None,
        )
        social_history = _coalesce(
            profile_data.get("social_history") if isinstance(profile_data, dict) else None,
            memory.get("social_history") if isinstance(memory, dict) else None,
        )
        vitals = _coalesce(
            profile_data.get("vitals") if isinstance(profile_data, dict) else None,
            memory.get("vitals") if isinstance(memory, dict) else None,
            sensor_vitals,
        )
        exam = _coalesce(
            profile_data.get("exam") if isinstance(profile_data, dict) else None,
            memory.get("exam") if isinstance(memory, dict) else None,
        )

    return {
        # Only include the keys the report generator expects to reduce prompt noise.
        "patient_id": user_row[0],
        "name": name,
        "age": age,
        "sex": sex,
        "allergies": allergies,
        "medications": medications,
        "chief_complaint": chief_complaint,
        "history_of_present_illness": history_of_present_illness,
        "past_medical_history": past_medical_history,
        "family_history": family_history,
        "social_history": social_history,
        "vitals": vitals,
        "exam": exam,
        "attachments": attachments,
        # Optional extra context (compact)
        "sensor_summary": sensor_summary,
    }

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


def _probe_db() -> tuple[bool, float]:
    start = time.perf_counter()
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        return True, time.perf_counter() - start
    except Exception:
        return False, time.perf_counter() - start


def _probe_minio() -> tuple[bool, float]:
    start = time.perf_counter()
    try:
        S3.list_buckets()
        return True, time.perf_counter() - start
    except Exception:
        return False, time.perf_counter() - start


def _probe_authentik() -> tuple[bool, float]:
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=METRICS_PROBE_TIMEOUT_SECONDS, follow_redirects=True) as client:
            response = client.get(AUTH_INTERNAL_ISSUER + ".well-known/openid-configuration")
        return response.status_code < 500, time.perf_counter() - start
    except Exception:
        return False, time.perf_counter() - start


def _probe_ollama() -> tuple[bool, float]:
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=METRICS_PROBE_TIMEOUT_SECONDS) as client:
            response = client.get(f"{OLLAMA_BASE_URL.rstrip('/')}/api/tags")
        return response.status_code < 500, time.perf_counter() - start
    except Exception:
        return False, time.perf_counter() - start


def refresh_dependency_metrics() -> None:
    for service, probe in (
        ("db", _probe_db),
        ("minio", _probe_minio),
        ("authentik", _probe_authentik),
        ("ollama", _probe_ollama),
    ):
        up, duration = probe()
        observe_dependency_probe(service=service, up=up, duration_seconds=duration)

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

class EndSessionReq(BaseModel):
    session_id: str
    ended_at: datetime | None = None

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
    redirect_uri: str | None = None

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

class ReportGenerateReq(BaseModel):
    limit_sessions: int = Field(default=5, ge=1, le=50)
    limit_events: int = Field(default=200, ge=0, le=2000)

class ReportRunReq(ReportGenerateReq):
    model: str | None = None

class ReportListItem(BaseModel):
    report_id: str
    created_at: datetime
    model: str
    status: str
    payload_hash: str | None = None

class ReportDetail(ReportListItem):
    report_md: str

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
    redirect_uri: str | None = None,

    # ✅ allow mobile app to provide these
    state: str | None = None,
    code_challenge: str | None = None,
    code_challenge_method: str = "S256",
):
    if not AUTH_CLIENT_ID:
        raise HTTPException(500, "Missing AUTH_CLIENT_ID")

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
        mobile_redirect_uri = _validate_mobile_redirect_uri(redirect_uri or _default_mobile_redirect_uri())

        url = _build_url_with_query(
            authorize_endpoint,
            {
                "client_id": AUTH_CLIENT_ID,
                "response_type": "code",
                "redirect_uri": mobile_redirect_uri,
                "scope": "openid email profile",
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": code_challenge_method,
            },
        )
        return RedirectResponse(url=url, status_code=302)

    # ---- web flow: backend generates state+pkce and uses cookies ----
    web_state = secrets.token_urlsafe(24)
    verifier, challenge = _pkce_pair()

    resp = RedirectResponse(
        url=_build_url_with_query(
            authorize_endpoint,
            {
                "client_id": AUTH_CLIENT_ID,
                "response_type": "code",
                "redirect_uri": AUTH_REDIRECT_URI_WEB,
                "scope": "openid email profile",
                "state": web_state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            },
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
    redirect_uri = _validate_mobile_redirect_uri(req.redirect_uri or _default_mobile_redirect_uri())

    tokens = await _exchange_code_for_tokens(
        oidc["token_endpoint"],
        req.code,
        req.code_verifier,
        redirect_uri,
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


@app.get("/auth/callback")
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
    if client_kind == "web" and request.url.path == "/auth/callback":
        redirect_uri = _legacy_auth_callback_redirect_uri()
    else:
        redirect_uri = AUTH_REDIRECT_URI_WEB if client_kind == "web" else _default_mobile_redirect_uri()

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
    return {"access_token": access, "user_id": user_id}


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
@app.get("/metrics")
async def metrics():
    await run_in_threadpool(refresh_dependency_metrics)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/health")
def health():
    return {"ok": True, "time": time.time()}

@app.get("/api/health/db")
def health_db():
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1;")
        return {"ok": True}
    except Exception:
        logger.exception("health_db failed")
        raise HTTPException(500, {"code": "DB_UNREACHABLE", "message": "Database unreachable"})

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

@app.post("/api/reports/generate")
def generate_report(
    req: ReportGenerateReq,
    user_id: str = Depends(require_access_user_id),
):
    payload = build_report_payload(
        user_id=user_id,
        limit_sessions=req.limit_sessions,
        limit_events=req.limit_events,
    )
    payload_json = _json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload_hash = hashlib.sha256(payload_json).hexdigest()
    return {
        "user_id": user_id,
        "generated_at": _utcnow().isoformat(),
        "payload_hash": payload_hash,
        "payload": payload,
    }

@app.post("/api/reports/run")
async def run_report(
    req: ReportRunReq,
    user_id: str = Depends(require_access_user_id),
):
    payload = build_report_payload(
        user_id=user_id,
        limit_sessions=req.limit_sessions,
        limit_events=req.limit_events,
    )
    model_name = req.model or REPORT_MODEL_DEFAULT

    def _run_llm(data: dict, model: str) -> str:
        from langchain_ollama import ChatOllama
        from report_gen.ollama import generate_report as _generate_report

        chat_model = ChatOllama(model=model, temperature=0, base_url=OLLAMA_BASE_URL)
        return _generate_report(data, chat_model)

    try:
        report_md = await run_in_threadpool(_run_llm, payload, model_name)
    except Exception as e:
        logger.exception("report generation failed")
        raise HTTPException(502, f"Report generation failed: {e}")

    payload_json = _json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload_hash = hashlib.sha256(payload_json).hexdigest()

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO reports (user_id, model, payload_hash, report_md, status)
            VALUES (%s, %s, %s, %s, 'generated')
            RETURNING id::text, created_at
            """,
            (user_id, model_name, payload_hash, report_md),
        )
        report_id, created_at = cur.fetchone()

    return {
        "user_id": user_id,
        "report_id": report_id,
        "generated_at": created_at.isoformat(),
        "model": model_name,
        "payload_hash": payload_hash,
        "report_md": report_md,
    }

@app.get("/api/reports", response_model=list[ReportListItem])
def list_reports(
    limit: int = Query(20, ge=1, le=100),
    user_id: str = Depends(require_access_user_id),
):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, created_at, model, status, payload_hash
            FROM reports
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (user_id, limit),
        )
        rows = cur.fetchall()
    return [
        {
            "report_id": r[0],
            "created_at": r[1],
            "model": r[2],
            "status": r[3],
            "payload_hash": r[4],
        }
        for r in rows
    ]

@app.get("/api/reports/{report_id}", response_model=ReportDetail)
def get_report(
    report_id: str,
    user_id: str = Depends(require_access_user_id),
):
    try:
        uuid.UUID(report_id)
    except ValueError:
        raise HTTPException(400, "report_id must be a valid UUID")

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, created_at, model, status, payload_hash, report_md
            FROM reports
            WHERE id = %s AND user_id = %s
            """,
            (report_id, user_id),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Report not found")

    return {
        "report_id": row[0],
        "created_at": row[1],
        "model": row[2],
        "status": row[3],
        "payload_hash": row[4],
        "report_md": row[5],
    }

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

@app.post("/api/sessions/end")
def end_session(req: EndSessionReq):
    end_ts = req.ended_at or datetime.now(timezone.utc)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT started_at, ended_at
            FROM user_sessions
            WHERE id=%s
            """,
            (req.session_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Session not found")

        started_at, existing_ended_at = row

        if existing_ended_at is not None:
            return {
                "ok": True,
                "session_id": req.session_id,
                "ended_at": existing_ended_at.isoformat(),
                "already_ended": True,
            }

        if end_ts < started_at:
            raise HTTPException(400, "ended_at cannot be before started_at")

        cur.execute(
            """
            UPDATE user_sessions
            SET ended_at = %s
            WHERE id=%s
            RETURNING ended_at
            """,
            (end_ts, req.session_id),
        )
        (stored_ended_at,) = cur.fetchone()

    return {
        "ok": True,
        "session_id": req.session_id,
        "ended_at": stored_ended_at.isoformat(),
        "already_ended": False,
    }

@app.post("/api/sensors/events")
def sensor_event(req: SensorEventReq):
    ts = req.ts or datetime.now(timezone.utc)

    logger.info(
        "sensor_event received device_external_id=%s session_id=%s kind=%s seq=%s ts=%s data_keys=%s",
        req.device_external_id,
        req.session_id,
        req.kind,
        req.seq,
        ts.isoformat(),
        len(req.data or {}),
    )

    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT id::text FROM devices WHERE device_external_id=%s;", (req.device_external_id,))
            row = cur.fetchone()
            if not row:
                logger.warning("sensor_event device_not_registered device_external_id=%s", req.device_external_id)
                observe_sensor_event(req.kind, "device_not_registered")
                raise HTTPException(404, {"code": "DEVICE_NOT_REGISTERED", "message": "Device not registered"})
            (device_id,) = row

            if req.session_id:
                cur.execute(
                    "SELECT 1 FROM user_sessions WHERE id=%s AND (device_id=%s OR device_id IS NULL) LIMIT 1;",
                    (req.session_id, device_id),
                )
                if not cur.fetchone():
                    logger.warning(
                        "sensor_event invalid_session session_id=%s device_id=%s",
                        req.session_id,
                        device_id,
                    )
                    observe_sensor_event(req.kind, "invalid_session")
                    raise HTTPException(
                        400,
                        {"code": "INVALID_SESSION", "message": "Invalid session_id for this device"},
                    )

            cur.execute(
                """
                INSERT INTO sensor_events (session_id, device_id, ts, kind, seq, data)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (device_id, kind, seq) WHERE seq IS NOT NULL DO NOTHING
                """,
                (req.session_id, device_id, ts, req.kind, req.seq, _json.dumps(req.data)),
            )
            stored = cur.rowcount == 1
            observe_sensor_event(req.kind, "stored" if stored else "duplicate")

    except HTTPException:
        raise
    except psycopg.Error:
        observe_sensor_event(req.kind, "db_error")
        logger.exception(
            "sensor_event db_error device_external_id=%s session_id=%s kind=%s seq=%s",
            req.device_external_id,
            req.session_id,
            req.kind,
            req.seq,
        )
        raise HTTPException(500, {"code": "DB_ERROR", "message": "Database error"})
    except Exception:
        observe_sensor_event(req.kind, "internal_error")
        logger.exception(
            "sensor_event unexpected_error device_external_id=%s session_id=%s kind=%s seq=%s",
            req.device_external_id,
            req.session_id,
            req.kind,
            req.seq,
        )
        raise HTTPException(500, {"code": "INTERNAL_ERROR", "message": "Internal server error"})

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

    start = time.perf_counter()
    rag_invoked = False
    result_label = "error"
    next_step = None
    recommended_sensor = None
    try:
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

        try:
            rag_invoked = True
            chat_result = await run_in_threadpool(
                chat_once,
                user_id=req.user_id,
                user_message=req.message,
                session_id=session_id
            )
        except Exception:
            observe_rag_error("chat_once")
            raise

        assistant_message = chat_result.get("assistant_message")
        if not isinstance(assistant_message, str):
            assistant_message = _json.dumps(chat_result, ensure_ascii=False)

        try:
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
                next_step = chat_result.get("suggested_next_step")
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
                    and next_step == "proceed_to_measurements"
                )

                session_signal = {
                    "last_recommended_sensor": recommended_sensor,
                    "last_suggested_next_step": next_step,
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
        except Exception:
            observe_rag_error("chat_persistence")
            raise

        chat_result["session_id"] = session_id
        result_label = "ok"
        return chat_result
    finally:
        duration_seconds = time.perf_counter() - start
        observe_chat_request(
            result=result_label,
            duration_seconds=duration_seconds,
            next_step=next_step,
            recommended_sensor=recommended_sensor,
        )
        if rag_invoked:
            observe_rag_chat_turn(
                result=result_label,
                duration_seconds=duration_seconds,
                next_step=next_step,
                recommended_sensor=recommended_sensor,
            )
    
