# --- keep imports at top ---
import os, io, time, hashlib, statistics, math, colorsys, asyncio
import secrets
import logging
import base64
import httpx
import uuid
from typing import Optional, Any
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Query, Request, Depends, Response, Body
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from fastapi.responses import RedirectResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ConfigDict
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps, ImageStat
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
from sensor_public_model import load_public_model, score_public_model

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
AUTH_SUCCESS_REDIRECT_PATIENT = os.getenv(
    "AUTH_SUCCESS_REDIRECT_PATIENT",
    AUTH_SUCCESS_REDIRECT,
).strip() or AUTH_SUCCESS_REDIRECT
AUTH_SUCCESS_REDIRECT_DOCTOR = os.getenv(
    "AUTH_SUCCESS_REDIRECT_DOCTOR",
    f"{AUTH_SUCCESS_REDIRECT_PATIENT.rstrip('/')}/doctor" if AUTH_SUCCESS_REDIRECT_PATIENT.rstrip("/") else AUTH_SUCCESS_REDIRECT,
).strip() or AUTH_SUCCESS_REDIRECT_PATIENT
AUTH_OIDC_SCOPES = " ".join(
    part.strip()
    for part in os.getenv("AUTH_OIDC_SCOPES", "openid email profile").split()
    if part.strip()
).strip() or "openid email profile"
AUTH_DOCTOR_GROUPS = frozenset(
    " ".join(part.strip().lower().split())
    for part in os.getenv("AUTH_DOCTOR_GROUPS", "doctor,doctors").split(",")
    if part.strip()
)
AUTH_ROLE_CLAIM_KEYS = tuple(
    dict.fromkeys(
        part.strip()
        for part in os.getenv("AUTH_ROLE_CLAIM_KEYS", "role,roles,group,groups,ak_groups").split(",")
        if part.strip()
    )
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
JWT_LEEWAY_SECONDS = int(os.getenv("JWT_LEEWAY_SECONDS", "60"))
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
CONSULT_ATTACHMENT_MAX_MB = int(os.getenv("CONSULT_ATTACHMENT_MAX_MB", "15"))
CONSULT_ATTACHMENT_PREVIEW_TEXT_MAX_CHARS = max(120, int(os.getenv("CONSULT_ATTACHMENT_PREVIEW_TEXT_MAX_CHARS", "500")))
PATIENT_REQUEST_WAIT_MINUTES_PER_POSITION = max(1, int(os.getenv("PATIENT_REQUEST_WAIT_MINUTES_PER_POSITION", "15")))
PATIENT_REQUEST_MIN_WAIT_MINUTES = max(1, int(os.getenv("PATIENT_REQUEST_MIN_WAIT_MINUTES", "5")))
SENSOR_ANALYSIS_WINDOW_MINUTES = int(os.getenv("SENSOR_ANALYSIS_WINDOW_MINUTES", "10"))
SENSOR_ANALYZER_VERSION = os.getenv("SENSOR_ANALYZER_VERSION", "hybrid-v2")
SENSOR_MODEL_VERSION = os.getenv("SENSOR_MODEL_VERSION", "adaptive-baseline-v1")
SENSOR_MODEL_HISTORY_LIMIT = int(os.getenv("SENSOR_MODEL_HISTORY_LIMIT", "50"))
SENSOR_MODEL_MIN_SAMPLES = int(os.getenv("SENSOR_MODEL_MIN_SAMPLES", "5"))
SENSOR_PERSONALIZATION_ENABLED = os.getenv("SENSOR_PERSONALIZATION_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
SENSOR_PERSONALIZATION_MIN_SAMPLES = max(1, int(os.getenv("SENSOR_PERSONALIZATION_MIN_SAMPLES", str(SENSOR_MODEL_MIN_SAMPLES))))
SENSOR_PERSONALIZATION_ALERT_Z = float(os.getenv("SENSOR_PERSONALIZATION_ALERT_Z", "3.2"))
SENSOR_PERSONALIZATION_MAX_SCORE_REDUCTION = max(0.0, min(0.4, float(os.getenv("SENSOR_PERSONALIZATION_MAX_SCORE_REDUCTION", "0.18"))))
SENSOR_PERSONALIZATION_MAX_CONFIDENCE_BOOST = max(0.0, min(0.25, float(os.getenv("SENSOR_PERSONALIZATION_MAX_CONFIDENCE_BOOST", "0.12"))))
IMAGE_ANALYSIS_LINK_WINDOW_MINUTES = int(os.getenv("IMAGE_ANALYSIS_LINK_WINDOW_MINUTES", "15"))
IMAGE_ANALYSIS_RETENTION_DAYS = max(0, int(os.getenv("IMAGE_ANALYSIS_RETENTION_DAYS", "365")))
IMAGE_ANALYSIS_RETENTION_POLICY = os.getenv("IMAGE_ANALYSIS_RETENTION_POLICY", "standard").strip() or "standard"
IMAGE_PREVIEW_MAX_SIDE = max(96, int(os.getenv("IMAGE_PREVIEW_MAX_SIDE", "384")))
CONSULT_ATTACHMENT_PREVIEW_MAX_SIDE = max(96, int(os.getenv("CONSULT_ATTACHMENT_PREVIEW_MAX_SIDE", "320")))
STREAM_DEFAULT_POLL_SECONDS = max(1.0, float(os.getenv("STREAM_DEFAULT_POLL_SECONDS", "5.0")))
STREAM_MAX_POLL_SECONDS = max(STREAM_DEFAULT_POLL_SECONDS, float(os.getenv("STREAM_MAX_POLL_SECONDS", "30.0")))
STREAM_HEARTBEAT_SECONDS = max(5.0, float(os.getenv("STREAM_HEARTBEAT_SECONDS", "20.0")))
OPTICAL_WAVEFORM_DEFAULT_MAX_POINTS = max(200, int(os.getenv("OPTICAL_WAVEFORM_DEFAULT_MAX_POINTS", "2000")))
OPTICAL_WAVEFORM_MAX_POINTS = max(OPTICAL_WAVEFORM_DEFAULT_MAX_POINTS, int(os.getenv("OPTICAL_WAVEFORM_MAX_POINTS", "20000")))
OPTICAL_MEASUREMENT_MODE = "advanced_optical"
OPTICAL_MEASUREMENT_ID = "advanced_optical"
OPTICAL_MEASUREMENT_LABEL = "Optical waveform capture"
STANDARD_OPTICAL_MEASUREMENT_MODE = "standard_optical"
OPTICAL_RUN_STATUS_ACTIVE = "active"
OPTICAL_RUN_STATUS_STOPPED = "stopped"
OPTICAL_RUN_STATUS_FAILED = "failed"
OPTICAL_ANALYSIS_STATUS_PROCESSING = "processing"
OPTICAL_ANALYSIS_STATUS_COMPLETED = "completed"
OPTICAL_ANALYSIS_STATUS_FAILED = "failed"
OPTICAL_FINAL_OPTICAL_QUALITY_GOOD = "good"
OPTICAL_FINAL_OPTICAL_QUALITY_FAIR = "fair"
OPTICAL_FINAL_OPTICAL_QUALITY_POOR = "poor"
OPTICAL_FINAL_OPTICAL_QUALITY_INSUFFICIENT = "insufficient_signal"
OPTICAL_FINAL_OPTICAL_ALGORITHM_VERSION = os.getenv("OPTICAL_FINAL_OPTICAL_ALGORITHM_VERSION", "raw-ppg-final-v1").strip() or "raw-ppg-final-v1"
OPTICAL_PPG_SERIES_KEYS = ("ppg_ir", "ppg_red")
OPTICAL_PPG_SERIES_ALIASES: dict[str, tuple[str, ...]] = {
    "ppg_ir": ("ppg_ir", "ir", "infrared", "ir_value", "ir_sample", "infrared_value"),
    "ppg_red": ("ppg_red", "red", "red_value", "red_sample"),
}
OPTICAL_TS1_NORMALIZED_FIELDS: tuple[str, ...] = (
    "timestamp_ms",
    "sht_temp_c",
    "sht_rh_percent",
    "ens_iaq",
    "ens_eco2_ppm",
    "ens_temp_c",
    "mlx_ambient_c",
    "mlx_object_c",
    "heart_rate_bpm",
    "spo2_percent",
    "red_dc",
    "quality",
)
OPTICAL_TS1_SERIES_FIELDS: tuple[str, ...] = tuple(
    field for field in OPTICAL_TS1_NORMALIZED_FIELDS if field != "timestamp_ms"
)
OPTICAL_TS1_SERIES_LABELS: dict[str, str] = {
    "ts1_sht_temp_c": "TS1 SHT Temperature",
    "ts1_sht_rh_percent": "TS1 SHT Humidity",
    "ts1_ens_iaq": "TS1 IAQ",
    "ts1_ens_eco2_ppm": "TS1 eCO2",
    "ts1_ens_temp_c": "TS1 ENS Temperature",
    "ts1_mlx_ambient_c": "TS1 MLX Ambient Temperature",
    "ts1_mlx_object_c": "TS1 MLX Object Temperature",
    "ts1_heart_rate_bpm": "TS1 Heart Rate",
    "ts1_spo2_percent": "TS1 SpO2",
    "ts1_red_dc": "TS1 Red DC",
    "ts1_quality": "TS1 Quality",
}
SENSOR_MODEL_ANOMALY_Z = float(os.getenv("SENSOR_MODEL_ANOMALY_Z", "3.0"))
SENSOR_PUBLIC_MODEL_PATH = os.getenv("SENSOR_PUBLIC_MODEL_PATH", "/app/models/bidmc_public_model.pt")
SENSOR_PUBLIC_MODEL_DIR = os.getenv("SENSOR_PUBLIC_MODEL_DIR", "/app/models")
SENSOR_PUBLIC_MODEL_PATHS = tuple(
    path.strip()
    for path in os.getenv("SENSOR_PUBLIC_MODEL_PATHS", "").split(",")
    if path.strip()
)


app.mount("/assets", StaticFiles(directory="/app/assets"), name="assets")


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("unhandled server error path=%s", request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error": "server_error",
        },
    )

_PUBLIC_SENSOR_MODEL_CACHE: dict[str, Any] = {
    "paths": (),
    "mtimes": (),
    "artifacts": [],
}


def _public_sensor_model_paths() -> list[str]:
    if SENSOR_PUBLIC_MODEL_PATHS:
        return list(dict.fromkeys(SENSOR_PUBLIC_MODEL_PATHS))

    model_dir = SENSOR_PUBLIC_MODEL_DIR.strip()
    if model_dir:
        directory = Path(model_dir)
        if directory.is_dir():
            candidates = sorted(str(path) for path in directory.glob("*.pt") if path.is_file())
            if candidates:
                return candidates

    return [SENSOR_PUBLIC_MODEL_PATH]


def _get_public_sensor_models() -> list[dict[str, Any]]:
    paths = _public_sensor_model_paths()
    mtimes: list[float | None] = []
    for path in paths:
        try:
            mtimes.append(os.path.getmtime(path))
        except OSError:
            mtimes.append(None)

    if (
        tuple(paths) == tuple(_PUBLIC_SENSOR_MODEL_CACHE.get("paths") or ())
        and tuple(mtimes) == tuple(_PUBLIC_SENSOR_MODEL_CACHE.get("mtimes") or ())
    ):
        return list(_PUBLIC_SENSOR_MODEL_CACHE.get("artifacts") or [])

    artifacts: list[dict[str, Any]] = []
    for path, mtime in zip(paths, mtimes):
        if mtime is None:
            continue
        try:
            artifact = load_public_model(path)
        except Exception:
            logger.exception("failed to load public sensor model path=%s", path)
            continue
        if not isinstance(artifact, dict):
            continue
        artifact = dict(artifact)
        artifact["_model_path"] = path
        artifacts.append(artifact)

    _PUBLIC_SENSOR_MODEL_CACHE.update({"paths": tuple(paths), "mtimes": tuple(mtimes), "artifacts": artifacts})
    return list(artifacts)


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

def _iso_utc(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))

def _validated_uuid_str(value: str | None, field_name: str) -> str | None:
    if not value:
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise HTTPException(400, f"{field_name} must be a UUID") from exc

def _coalesce_nonempty(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, (list, dict, tuple, set)) and len(v) == 0:
            continue
        return v
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"true", "1", "yes", "y", "on"}:
            return True
        if token in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _normalize_profile_map(value: Any) -> dict[str, Any]:
    profile_data = value if isinstance(value, dict) else {}
    nested_profile = profile_data.get("profile") if isinstance(profile_data.get("profile"), dict) else None
    if nested_profile:
        profile_data = {**profile_data, **nested_profile}
    return profile_data


def _doctor_profile_summary_fields(value: Any) -> dict[str, str | None]:
    profile_data = _normalize_profile_map(value)
    return {
        "doctor_title": _coalesce_nonempty(profile_data.get("doctor_title"), profile_data.get("title")),
        "doctor_specialty": _coalesce_nonempty(profile_data.get("doctor_specialty"), profile_data.get("specialty")),
        "doctor_avatar_url": _coalesce_nonempty(
            profile_data.get("doctor_avatar_url"),
            profile_data.get("avatar_url"),
            profile_data.get("photo_url"),
            profile_data.get("image_url"),
        ),
    }


def _estimate_pending_request_wait_minutes(queue_position: int | None) -> int | None:
    if queue_position is None or queue_position < 1:
        return None
    return max(PATIENT_REQUEST_MIN_WAIT_MINUTES, queue_position * PATIENT_REQUEST_WAIT_MINUTES_PER_POSITION)


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        candidates = value.split(",") if "," in value else [value]
    elif isinstance(value, list):
        candidates = value
    else:
        return []

    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if isinstance(candidate, dict):
            raw_value = candidate.get("label") or candidate.get("name") or candidate.get("value")
            if not isinstance(raw_value, str):
                continue
            item = raw_value.strip()
        elif isinstance(candidate, str):
            item = candidate.strip()
        else:
            continue
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _normalize_symptom_list(value: Any) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = [part.strip() for part in value.split(",")]
    else:
        raw_items = []

    for raw in raw_items:
        if isinstance(raw, dict):
            label = str(raw.get("label") or raw.get("value") or "").strip()
            severity = str(raw.get("severity") or "").strip()
        elif isinstance(raw, str):
            label = raw.strip()
            severity = ""
        else:
            continue
        if not label or label in seen:
            continue
        seen.add(label)
        item = {"label": label}
        if severity:
            item["severity"] = severity
        items.append(item)
    return items


def _compute_age_from_dob(value: Any) -> int | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dob = datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
    except ValueError:
        try:
            dob = datetime.strptime(value.strip(), "%Y-%m-%d").date()
        except ValueError:
            return None
    today = _utcnow().date()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age if age >= 0 else None


def _encode_offset_cursor(offset: int) -> str:
    return _b64url(_json.dumps({"offset": max(0, offset)}, separators=(",", ":")).encode("utf-8"))


def _decode_offset_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        payload = _json.loads(_b64url_decode(cursor).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(400, "Invalid cursor") from exc
    offset = payload.get("offset")
    if not isinstance(offset, int) or offset < 0:
        raise HTTPException(400, "Invalid cursor")
    return offset


def _paginate_offset_items(items: list[dict[str, Any]], *, cursor: str | None, limit: int) -> tuple[list[dict[str, Any]], str | None]:
    offset = _decode_offset_cursor(cursor)
    page = items[offset: offset + limit]
    next_cursor = _encode_offset_cursor(offset + limit) if offset + limit < len(items) else None
    return page, next_cursor

def _as_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None

def _extract_from_data(data: dict[str, Any], keys: list[str]):
    if not isinstance(data, dict):
        return None
    data = _normalized_sensor_payload(data)
    for k in keys:
        if k in data:
            n = _as_number(data.get(k))
            if n is not None:
                return n
    return None


def _first_numeric(data: dict[str, Any], keys: list[str]) -> float | None:
    if not isinstance(data, dict):
        return None
    for key in keys:
        if key not in data:
            continue
        value = _as_number(data.get(key))
        if value is not None:
            return value
    return None


def _set_number_aliases(target: dict[str, Any], keys: list[str], value: float | None) -> None:
    if value is None:
        return
    for key in keys:
        target.setdefault(key, value)


def _scaled_numeric(data: dict[str, Any], key: str, divisor: float) -> float | None:
    if not isinstance(data, dict) or key not in data:
        return None
    value = _as_number(data.get(key))
    if value is None or divisor == 0:
        return None
    return value / divisor


def _normalized_sensor_payload(data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    normalized = dict(data)

    heart_rate = _first_numeric(normalized, ["heart_rate_bpm", "heart_rate", "hr", "pulse", "bpm", "hr_bpm"])
    _set_number_aliases(normalized, ["heart_rate_bpm", "heart_rate", "hr", "bpm"], heart_rate)

    spo2 = _first_numeric(normalized, ["spo2_percent", "spo2", "oxygen_saturation", "oxygen", "o2"])
    if spo2 is None:
        spo2 = _scaled_numeric(normalized, "spo2_x10", 10.0)
    _set_number_aliases(normalized, ["spo2_percent", "spo2", "oxygen_saturation", "oxygen", "o2"], spo2)

    skin_temperature = _first_numeric(normalized, ["skin_temperature_c", "skin_temperature", "skin_temp", "temperature", "temp", "celsius"])
    if skin_temperature is None:
        skin_temperature = _scaled_numeric(normalized, "mlx_To_mC", 1000.0)
    _set_number_aliases(
        normalized,
        ["skin_temperature_c", "skin_temperature", "skin_temp", "temperature", "temp", "celsius"],
        skin_temperature,
    )
    _set_number_aliases(normalized, ["object_temperature_c", "mlx_object_temperature_c"], skin_temperature)

    ambient_temperature = _first_numeric(
        normalized,
        ["ambient_temperature_c", "ambient_temperature", "ambient_temp_c", "room_temperature_c"],
    )
    if ambient_temperature is None:
        ambient_temperature = _scaled_numeric(normalized, "sht_Tc_x100", 100.0)
    if ambient_temperature is None:
        ambient_temperature = _scaled_numeric(normalized, "mlx_Ta_mC", 1000.0)
    if ambient_temperature is None:
        ambient_temperature = _scaled_numeric(normalized, "ens_Tc_x100", 100.0)
    _set_number_aliases(
        normalized,
        ["ambient_temperature_c", "ambient_temperature", "ambient_temp_c", "room_temperature_c"],
        ambient_temperature,
    )
    _set_number_aliases(normalized, ["sht_temperature_c"], _scaled_numeric(normalized, "sht_Tc_x100", 100.0))
    _set_number_aliases(normalized, ["mlx_ambient_temperature_c"], _scaled_numeric(normalized, "mlx_Ta_mC", 1000.0))
    _set_number_aliases(normalized, ["ens_temperature_c"], _scaled_numeric(normalized, "ens_Tc_x100", 100.0))

    humidity = _first_numeric(normalized, ["humidity_percent", "humidity", "relative_humidity", "rh"])
    if humidity is None:
        humidity = _scaled_numeric(normalized, "sht_RH_x100", 100.0)
    _set_number_aliases(normalized, ["humidity_percent", "humidity", "relative_humidity", "rh"], humidity)

    eco2 = _first_numeric(normalized, ["eco2_ppm", "co2_ppm", "co2", "ppm"])
    if eco2 is None:
        eco2 = _first_numeric(normalized, ["ens_eCO2_ppm"])
    _set_number_aliases(normalized, ["eco2_ppm", "co2_ppm", "co2", "ppm"], eco2)

    air_quality_index = _first_numeric(normalized, ["air_quality_index", "iaq_index", "iaq"])
    if air_quality_index is None:
        air_quality_index = _first_numeric(normalized, ["ens_iaq"])
    _set_number_aliases(normalized, ["air_quality_index", "iaq_index", "iaq"], air_quality_index)

    accel_x = _first_numeric(normalized, ["accel_x_g"])
    if accel_x is None:
        accel_x = _scaled_numeric(normalized, "accel_x_mg", 1000.0)
    accel_y = _first_numeric(normalized, ["accel_y_g"])
    if accel_y is None:
        accel_y = _scaled_numeric(normalized, "accel_y_mg", 1000.0)
    accel_z = _first_numeric(normalized, ["accel_z_g"])
    if accel_z is None:
        accel_z = _scaled_numeric(normalized, "accel_z_mg", 1000.0)
    _set_number_aliases(normalized, ["accel_x_g"], accel_x)
    _set_number_aliases(normalized, ["accel_y_g"], accel_y)
    _set_number_aliases(normalized, ["accel_z_g"], accel_z)

    motion_level = _first_numeric(normalized, ["motion_level_g", "accel_magnitude_g"])
    if motion_level is None and None not in (accel_x, accel_y, accel_z):
        motion_level = math.sqrt(float(accel_x) ** 2 + float(accel_y) ** 2 + float(accel_z) ** 2)
    _set_number_aliases(normalized, ["motion_level_g", "accel_magnitude_g"], motion_level)
    if motion_level is not None:
        normalized.setdefault("motion_delta_g", abs(float(motion_level) - 1.0))

    return normalized

def _latest_measurement(events: list[dict[str, Any]], kinds: set[str], keys: list[str]):
    for ev in events:
        if ev.get("kind") in kinds:
            n = _extract_from_data(ev.get("data") or {}, keys)
            if n is not None:
                return n
        n = _extract_from_data(ev.get("data") or {}, keys)
        if n is not None:
            return n
    return None

def _latest_bp(events: list[dict[str, Any]]):
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

def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]

def _extract_feelings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    if isinstance(value, dict):
        nested = _extract_feelings(value.get("feelings"))
        if nested:
            return nested
        feelings: list[str] = []
        for key, raw in value.items():
            if not isinstance(key, str) or not key.strip():
                continue
            include = False
            if isinstance(raw, bool):
                include = raw
            elif isinstance(raw, (int, float)):
                include = raw > 0
            elif isinstance(raw, str):
                include = bool(raw.strip())
            elif raw is not None:
                include = True
            if include:
                feelings.append(key.strip())
        return feelings
    return []

def _history_chat_result_from_meta(meta: dict[str, Any] | None, fallback_text: str | None = None) -> dict[str, Any] | None:
    if not isinstance(meta, dict):
        meta = {}

    assistant_summary = _coalesce_nonempty(
        meta.get("assistant_summary"),
        meta.get("last_assistant_summary"),
        meta.get("assistant_message"),
        meta.get("last_assistant_message"),
        fallback_text,
    )
    suggested_next_step = _coalesce_nonempty(
        meta.get("suggested_next_step"),
        meta.get("last_suggested_next_step"),
    )
    recommended_sensor = _coalesce_nonempty(
        meta.get("recommended_sensor"),
        meta.get("last_recommended_sensor"),
    )
    sensor_confidence = _as_number(meta.get("sensor_confidence"))
    if sensor_confidence is None:
        sensor_confidence = _as_number(meta.get("last_sensor_confidence"))

    recommended_measurements = _string_list(meta.get("recommended_measurements"))
    if not recommended_measurements:
        recommended_measurements = _string_list(meta.get("last_recommended_measurements"))

    if (
        assistant_summary is None
        and suggested_next_step is None
        and recommended_sensor is None
        and sensor_confidence is None
        and not recommended_measurements
    ):
        return None

    return {
        "assistant_summary": assistant_summary,
        "suggested_next_step": suggested_next_step,
        "recommended_sensor": recommended_sensor,
        "sensor_confidence": sensor_confidence,
        "recommended_measurements": recommended_measurements,
    }

TREND_SERIES_SPECS: dict[str, dict[str, Any]] = {
    "temperature": {
        "label": "Temperature",
        "unit": "C",
        "sensor_kinds": {"temp", "temperature", "body_temp", "skin_temperature", "skin_temp"},
        "data_keys": ["skin_temperature_c", "skin_temperature", "skin_temp", "temperature", "temp", "celsius", "value"],
        "specific_keys": ["skin_temperature_c", "skin_temperature", "skin_temp", "temperature", "temp", "celsius"],
        "min_allowed": 1.0,
        "max_allowed": 60.0,
    },
    "spo2": {
        "label": "SpO2",
        "unit": "%",
        "normal_range": {"min": 95.0, "max": 100.0},
        "sensor_kinds": {"spo2", "pulse_ox", "pulse_oxygen", "oxygen"},
        "data_keys": ["spo2_percent", "spo2", "oxygen_saturation", "oxygen", "o2", "value"],
        "specific_keys": ["spo2_percent", "spo2", "oxygen_saturation", "oxygen", "o2"],
        "min_allowed": 1.0,
        "max_allowed": 100.0,
    },
    "heart_rate": {
        "label": "Heart Rate",
        "unit": "bpm",
        "normal_range": {"min": 60.0, "max": 100.0},
        "sensor_kinds": {"hr", "heart_rate", "pulse"},
        "data_keys": ["heart_rate_bpm", "heart_rate", "hr", "pulse", "bpm", "value"],
        "specific_keys": ["heart_rate_bpm", "heart_rate", "hr", "pulse", "bpm"],
        "min_allowed": 1.0,
    },
    "humidity": {
        "label": "Humidity",
        "unit": "%",
        "sensor_kinds": {"humidity", "humid"},
        "data_keys": ["humidity_percent", "humidity", "relative_humidity", "rh", "value"],
        "specific_keys": ["humidity_percent", "humidity", "relative_humidity", "rh"],
        "min_allowed": 0.0,
        "max_allowed": 100.0,
    },
    "ambient_temperature": {
        "label": "Ambient Temperature",
        "unit": "C",
        "sensor_kinds": {"ambient_temperature", "ambient_temp", "room_temperature"},
        "data_keys": ["ambient_temperature_c", "ambient_temperature", "ambient_temp_c", "room_temperature_c", "value"],
        "specific_keys": ["ambient_temperature_c", "ambient_temperature", "ambient_temp_c", "room_temperature_c"],
        "min_allowed": -20.0,
        "max_allowed": 60.0,
    },
    "co2": {
        "label": "Air CO2",
        "unit": "ppm",
        "sensor_kinds": {"co2"},
        "data_keys": ["eco2_ppm", "co2_ppm", "co2", "ppm", "value"],
        "specific_keys": ["eco2_ppm", "co2_ppm", "co2", "ppm"],
        "min_allowed": 0.0,
    },
    "air_quality_index": {
        "label": "Air Quality Index",
        "unit": "index",
        "sensor_kinds": {"air_quality", "air_quality_index", "iaq"},
        "data_keys": ["air_quality_index", "iaq_index", "iaq", "value"],
        "specific_keys": ["air_quality_index", "iaq_index", "iaq"],
        "min_allowed": 0.0,
        "max_allowed": 10.0,
    },
}

def _extract_trend_value(raw_kind: str | None, data: dict[str, Any], spec: dict[str, Any]) -> float | None:
    if not isinstance(data, dict):
        return None

    if raw_kind in spec["sensor_kinds"]:
        value = _extract_from_data(data, spec["data_keys"])
    else:
        value = _extract_from_data(data, spec["specific_keys"])

    if value is None:
        return None
    min_allowed = spec.get("min_allowed")
    max_allowed = spec.get("max_allowed")
    if min_allowed is not None and value < min_allowed:
        return None
    if max_allowed is not None and value > max_allowed:
        return None
    return value

ANALYSIS_SERIES_SPECS: dict[str, dict[str, Any]] = {
    **TREND_SERIES_SPECS,
    "respiratory_rate": {
        "label": "Respiratory Rate",
        "unit": "breaths/min",
        "sensor_kinds": {"rr", "resp_rate", "respiratory_rate"},
        "data_keys": ["rr", "resp_rate", "respiratory_rate", "value"],
        "specific_keys": ["rr", "resp_rate", "respiratory_rate"],
        "min_allowed": 1.0,
        "max_allowed": 80.0,
    },
}

def _round_number(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)

def _metric_points_from_events(events: list[dict[str, Any]], spec: dict[str, Any]) -> list[tuple[datetime, float]]:
    points: list[tuple[datetime, float]] = []
    for event in events:
        ts = event.get("_ts")
        if not isinstance(ts, datetime):
            continue
        value = _extract_trend_value(event.get("kind"), event.get("data") or {}, spec)
        if value is None:
            continue
        points.append((ts, value))
    return points

def _metric_summary(points: list[tuple[datetime, float]]) -> dict[str, Any]:
    values = [value for _, value in points]
    first_ts, first_value = points[0]
    last_ts, last_value = points[-1]
    duration_seconds = max(0.0, (last_ts - first_ts).total_seconds())
    slope_per_minute = None
    if duration_seconds > 0:
        slope_per_minute = (last_value - first_value) / (duration_seconds / 60.0)

    summary = {
        "count": len(values),
        "latest": _round_number(last_value),
        "min": _round_number(min(values)),
        "max": _round_number(max(values)),
        "mean": _round_number(statistics.fmean(values)),
        "duration_seconds": _round_number(duration_seconds, 1),
    }
    if len(values) > 1:
        summary["delta"] = _round_number(last_value - first_value)
        summary["span"] = _round_number(max(values) - min(values))
    if slope_per_minute is not None:
        summary["slope_per_minute"] = _round_number(slope_per_minute)
    return summary

def _make_analysis_finding(
    code: str,
    severity: str,
    category: str,
    message: str,
    kind: str | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "category": category,
        "kind": kind,
        "message": message,
    }

def _analyze_sensor_events(
    events: list[dict[str, Any]],
    *,
    scope: str,
    session_id: str | None,
    encounter_id: str | None,
    measurement_run_id: str | None,
) -> dict[str, Any]:
    analyzed_at = _utcnow()
    ordered_events = sorted(
        events,
        key=lambda event: event.get("_ts") or datetime.min.replace(tzinfo=timezone.utc),
    )

    if not ordered_events:
        return {
            "analyzer": SENSOR_ANALYZER_VERSION,
            "scope": scope,
            "status": "insufficient_data",
            "score": 0.0,
            "confidence": 0.0,
            "summary": "No supported sensor readings are available yet.",
            "measurement_run_id": measurement_run_id,
            "session_id": session_id,
            "encounter_id": encounter_id,
            "event_count": 0,
            "analyzed_at": _iso_utc(analyzed_at),
            "findings": [],
            "latest_values": {},
            "model": {},
            "features": {
                "metric_count": 0,
                "sensor_kinds": [],
            },
        }

    findings: list[dict[str, Any]] = []
    latest_values: dict[str, Any] = {}
    metric_features: dict[str, Any] = {}
    sensor_kinds = sorted({str(event.get("kind")) for event in ordered_events if event.get("kind")})
    physiology_metrics = {"heart_rate", "spo2", "temperature", "respiratory_rate"}

    for metric_kind, spec in ANALYSIS_SERIES_SPECS.items():
        points = _metric_points_from_events(ordered_events, spec)
        if not points:
            continue

        summary = _metric_summary(points)
        metric_features[metric_kind] = summary
        latest = summary.get("latest")
        if latest is not None:
            latest_values[metric_kind] = latest

        count = int(summary["count"])
        duration_seconds = float(summary.get("duration_seconds") or 0.0)
        span = float(summary.get("span") or 0.0)

        if metric_kind == "heart_rate":
            if latest is not None and latest >= 140:
                findings.append(_make_analysis_finding("hr_high", "high", "physiology", "Heart rate is well above the configured range.", "heart_rate"))
            elif latest is not None and latest >= 110:
                findings.append(_make_analysis_finding("hr_elevated", "medium", "physiology", "Heart rate is above the configured range.", "heart_rate"))
            elif latest is not None and latest <= 40:
                findings.append(_make_analysis_finding("hr_low", "high", "physiology", "Heart rate is well below the configured range.", "heart_rate"))
            elif latest is not None and latest <= 50:
                findings.append(_make_analysis_finding("hr_depressed", "medium", "physiology", "Heart rate is below the configured range.", "heart_rate"))

            if count >= 2 and duration_seconds <= 30 and span >= 35:
                findings.append(_make_analysis_finding("hr_jump", "medium", "artifact", "Heart-rate readings changed too quickly to trust without a recheck.", "heart_rate"))

        elif metric_kind == "spo2":
            if latest is not None and latest < 90:
                findings.append(_make_analysis_finding("spo2_low", "high", "physiology", "SpO2 is well below the configured range.", "spo2"))
            elif latest is not None and latest < 94:
                findings.append(_make_analysis_finding("spo2_borderline", "medium", "physiology", "SpO2 is below the configured range.", "spo2"))

            if count >= 2 and duration_seconds <= 20 and span >= 5:
                findings.append(_make_analysis_finding("spo2_jump", "medium", "artifact", "SpO2 changed sharply within a short interval; signal quality may be unstable.", "spo2"))

        elif metric_kind == "temperature":
            if latest is not None and latest >= 39.5:
                findings.append(_make_analysis_finding("temp_high", "medium", "physiology", "Temperature is above the expected range.", "temperature"))
            elif latest is not None and latest <= 30.0:
                findings.append(_make_analysis_finding("temp_low", "medium", "physiology", "Temperature is below the expected range.", "temperature"))

            if count >= 2 and duration_seconds <= 60 and span >= 2.0:
                findings.append(_make_analysis_finding("temp_jump", "low", "artifact", "Temperature moved unusually quickly; a recheck would help confirm it.", "temperature"))

        elif metric_kind == "respiratory_rate":
            if latest is not None and latest >= 28:
                findings.append(_make_analysis_finding("rr_high", "high", "physiology", "Respiratory rate is above the configured range.", "respiratory_rate"))
            elif latest is not None and latest >= 24:
                findings.append(_make_analysis_finding("rr_elevated", "medium", "physiology", "Respiratory rate is slightly above the configured range.", "respiratory_rate"))
            elif latest is not None and latest <= 8:
                findings.append(_make_analysis_finding("rr_low", "high", "physiology", "Respiratory rate is below the configured range.", "respiratory_rate"))
            elif latest is not None and latest <= 10:
                findings.append(_make_analysis_finding("rr_depressed", "medium", "physiology", "Respiratory rate is slightly below the configured range.", "respiratory_rate"))

        elif metric_kind == "co2":
            if latest is not None and latest >= 2000:
                findings.append(_make_analysis_finding("co2_high", "high", "environment", "Ambient eCO2 is very high.", "co2"))
            elif latest is not None and latest >= 1200:
                findings.append(_make_analysis_finding("co2_elevated", "medium", "environment", "Ambient eCO2 is elevated.", "co2"))

        elif metric_kind == "humidity":
            if latest is not None and (latest < 20 or latest > 80):
                findings.append(_make_analysis_finding("humidity_extreme", "medium", "environment", "Ambient humidity is outside the comfort band.", "humidity"))
            elif latest is not None and (latest < 30 or latest > 70):
                findings.append(_make_analysis_finding("humidity_offset", "low", "environment", "Ambient humidity is drifting outside the comfort band.", "humidity"))

        elif metric_kind == "ambient_temperature":
            if latest is not None and (latest < 12 or latest > 32):
                findings.append(_make_analysis_finding("ambient_temp_extreme", "medium", "environment", "Ambient temperature is outside the comfort band.", "ambient_temperature"))
            elif latest is not None and (latest < 16 or latest > 28):
                findings.append(_make_analysis_finding("ambient_temp_offset", "low", "environment", "Ambient temperature is drifting outside the comfort band.", "ambient_temperature"))

        elif metric_kind == "air_quality_index":
            if latest is not None and latest >= 5:
                findings.append(_make_analysis_finding("air_quality_poor", "high", "environment", "Air quality index is poor.", "air_quality_index"))
            elif latest is not None and latest >= 4:
                findings.append(_make_analysis_finding("air_quality_fair", "medium", "environment", "Air quality index is elevated.", "air_quality_index"))

    physiology_findings = [finding for finding in findings if finding["category"] == "physiology"]
    artifact_findings = [finding for finding in findings if finding["category"] == "artifact"]
    environment_findings = [finding for finding in findings if finding["category"] == "environment"]
    physiology_metrics_present = [kind for kind in metric_features if kind in physiology_metrics]

    if not metric_features:
        status = "insufficient_data"
    elif physiology_findings:
        status = "unusual"
    elif artifact_findings:
        status = "artifact"
    elif environment_findings:
        status = "unusual"
    else:
        status = "normal"

    score_weights = {
        ("high", "physiology"): 0.65,
        ("medium", "physiology"): 0.35,
        ("low", "physiology"): 0.18,
        ("high", "artifact"): 0.55,
        ("medium", "artifact"): 0.35,
        ("low", "artifact"): 0.15,
        ("high", "environment"): 0.30,
        ("medium", "environment"): 0.18,
        ("low", "environment"): 0.10,
    }
    score = min(1.0, sum(score_weights.get((finding["severity"], finding["category"]), 0.0) for finding in findings))
    if status == "normal":
        score = min(score or 0.1, 0.2)
    if status == "insufficient_data":
        score = 0.0

    total_points = sum(int(summary["count"]) for summary in metric_features.values())
    confidence = 0.15 + (0.08 * len(metric_features)) + (0.04 * min(total_points, 10))
    if scope == "measurement_run":
        confidence += 0.10
    if len(metric_features) == 1:
        confidence -= 0.08
    if total_points <= 1:
        confidence = min(confidence, 0.35)
    confidence = max(0.05, min(confidence, 0.95))
    if status == "insufficient_data":
        confidence = 0.0

    severity_rank = {"high": 3, "medium": 2, "low": 1}
    ordered_findings = sorted(findings, key=lambda finding: severity_rank.get(finding["severity"], 0), reverse=True)
    top_messages = [finding["message"] for finding in ordered_findings[:2]]

    if status == "artifact":
        summary = "Signal quality looks inconsistent. Recheck sensor placement and collect another run."
        if top_messages:
            summary = f"{summary} {top_messages[0]}"
    elif status == "unusual":
        if top_messages:
            summary = " ".join(top_messages)
        else:
            summary = "Current readings look unusual relative to the configured ranges."
        if not physiology_findings and physiology_metrics_present:
            summary = f"Physiological readings look stable, but context sensors are unusual. {summary}"
    elif status == "normal":
        if physiology_metrics_present:
            summary = "Current readings are within configured ranges."
        else:
            summary = "Current readings are available, but only context sensors are present."
        if environment_findings:
            summary = f"{summary} {environment_findings[0]['message']}"
        if total_points <= 1:
            summary = f"{summary} Confidence is limited because only one data point is available."
    else:
        summary = "Not enough supported sensor readings are available yet."

    first_ts = ordered_events[0].get("_ts")
    last_ts = ordered_events[-1].get("_ts")

    return {
        "analyzer": SENSOR_ANALYZER_VERSION,
        "scope": scope,
        "status": status,
        "score": _round_number(score),
        "confidence": _round_number(confidence),
        "summary": summary,
        "measurement_run_id": measurement_run_id,
        "session_id": session_id,
        "encounter_id": encounter_id,
        "event_count": len(ordered_events),
        "analyzed_at": _iso_utc(analyzed_at),
        "findings": ordered_findings,
        "latest_values": latest_values,
        "model": {},
        "features": {
            "metric_count": len(metric_features),
            "sensor_kinds": sensor_kinds,
            "metrics": metric_features,
            "window_started_at": _iso_utc(first_ts),
            "window_ended_at": _iso_utc(last_ts),
        },
    }

def _load_sensor_events_for_analysis(
    cur,
    *,
    device_id: str,
    session_id: str | None,
    encounter_id: str | None,
    measurement_run_id: str | None,
    anchor_ts: datetime,
) -> tuple[str, list[dict[str, Any]]]:
    if measurement_run_id:
        cur.execute(
            """
            SELECT ts, kind, seq, data
            FROM sensor_events
            WHERE measurement_run_id=%s
            ORDER BY ts ASC, id ASC
            """,
            (measurement_run_id,),
        )
        scope = "measurement_run"
    elif session_id:
        cur.execute(
            """
            SELECT ts, kind, seq, data
            FROM sensor_events
            WHERE session_id=%s
              AND ts >= %s
            ORDER BY ts ASC, id ASC
            """,
            (session_id, anchor_ts - timedelta(minutes=SENSOR_ANALYSIS_WINDOW_MINUTES)),
        )
        scope = "session_window"
    elif encounter_id:
        cur.execute(
            """
            SELECT ts, kind, seq, data
            FROM sensor_events
            WHERE encounter_id=%s
              AND ts >= %s
            ORDER BY ts ASC, id ASC
            """,
            (encounter_id, anchor_ts - timedelta(minutes=SENSOR_ANALYSIS_WINDOW_MINUTES)),
        )
        scope = "encounter_window"
    else:
        cur.execute(
            """
            SELECT ts, kind, seq, data
            FROM sensor_events
            WHERE device_id=%s
              AND ts >= %s
            ORDER BY ts ASC, id ASC
            """,
            (device_id, anchor_ts - timedelta(minutes=SENSOR_ANALYSIS_WINDOW_MINUTES)),
        )
        scope = "device_window"

    rows = cur.fetchall()
    events = [
        {
            "ts": _iso_utc(row[0]),
            "_ts": row[0],
            "kind": row[1],
            "seq": row[2],
            "data": row[3] if isinstance(row[3], dict) else {},
        }
        for row in rows
    ]
    return scope, events

def _persist_sensor_analysis(cur, *, device_id: str, analysis: dict[str, Any]) -> None:
    measurement_run_id = analysis.get("measurement_run_id")
    if not measurement_run_id:
        return
    # Don't persist transient insufficient_data — a later upload for the same run
    # will upsert a real status once recognized metric values are present.
    if analysis.get("status") == "insufficient_data":
        return

    cur.execute(
        """
        INSERT INTO sensor_analysis (
            measurement_run_id,
            session_id,
            encounter_id,
            device_id,
            analyzer,
            scope,
            status,
            score,
            confidence,
            summary,
            event_count,
            findings,
            latest_values,
            features,
            model,
            analyzed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s)
        ON CONFLICT (measurement_run_id, analyzer) DO UPDATE
        SET session_id = EXCLUDED.session_id,
            encounter_id = EXCLUDED.encounter_id,
            device_id = EXCLUDED.device_id,
            scope = EXCLUDED.scope,
            status = EXCLUDED.status,
            score = EXCLUDED.score,
            confidence = EXCLUDED.confidence,
            summary = EXCLUDED.summary,
            event_count = EXCLUDED.event_count,
            findings = EXCLUDED.findings,
            latest_values = EXCLUDED.latest_values,
            features = EXCLUDED.features,
            model = EXCLUDED.model,
            analyzed_at = EXCLUDED.analyzed_at
        """,
        (
            measurement_run_id,
            analysis.get("session_id"),
            analysis.get("encounter_id"),
            device_id,
            analysis.get("analyzer"),
            analysis.get("scope"),
            analysis.get("status"),
            analysis.get("score"),
            analysis.get("confidence"),
            analysis.get("summary"),
            analysis.get("event_count"),
            _json.dumps(analysis.get("findings") or []),
            _json.dumps(analysis.get("latest_values") or {}),
            _json.dumps(analysis.get("features") or {}),
            _json.dumps(analysis.get("model") or {}),
            analysis.get("analyzed_at"),
        ),
    )

def _analysis_confidence_band(confidence: float | None) -> str | None:
    if confidence is None:
        return None
    if confidence >= 0.85:
        return "high"
    if confidence >= 0.6:
        return "moderate"
    return "low"

def _analysis_findings_text(findings: Any) -> list[str]:
    messages: list[str] = []
    seen: set[str] = set()
    if not isinstance(findings, list):
        return messages

    for finding in findings:
        message: str | None = None
        if isinstance(finding, str):
            message = finding.strip()
        elif isinstance(finding, dict):
            candidate = _coalesce_nonempty(
                finding.get("message"),
                finding.get("note"),
                finding.get("summary"),
                finding.get("text"),
                finding.get("label"),
                finding.get("name"),
                finding.get("value"),
            )
            if candidate is not None:
                message = str(candidate).strip()

        if not message or message in seen:
            continue

        seen.add(message)
        messages.append(message)

    return messages

def _normalized_history_analysis(
    payload: dict[str, Any] | None,
    *,
    fallback_status: str = "analysis_available",
    fallback_summary: str = "Analysis is available for this run.",
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    normalized = dict(payload)

    status_value = normalized.get("status")
    status = status_value.strip().lower() if isinstance(status_value, str) else ""
    if not status:
        status = fallback_status
    normalized["status"] = status

    findings = normalized.get("findings")
    if not isinstance(findings, list):
        findings = []
    normalized["findings"] = findings

    patient_findings: list[str] = []
    raw_patient_findings = normalized.get("patient_findings")
    if isinstance(raw_patient_findings, list):
        for item in raw_patient_findings:
            if not isinstance(item, str):
                continue
            item = item.strip()
            if not item or item in patient_findings:
                continue
            patient_findings.append(item)

    if not patient_findings:
        patient_findings = _analysis_findings_text(findings)
    normalized["patient_findings"] = patient_findings

    summary_value = _coalesce_nonempty(normalized.get("patient_summary"), normalized.get("summary"))
    summary = summary_value.strip() if isinstance(summary_value, str) else ""
    if not summary:
        if status == "normal":
            summary = "Readings look normal."
        elif status == "unusual":
            summary = "Reading pattern looks unusual."
        elif status == "artifact":
            summary = "Signal quality issue, recheck."
        elif status == "insufficient_data":
            summary = "Need more readings."
        else:
            summary = fallback_summary

    normalized["summary"] = summary
    normalized["patient_summary"] = summary
    return normalized

def _sensor_analysis_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    # Extract technical fields
    model_public = row[13] if isinstance(row[13], dict) else {}
    findings = row[10] if isinstance(row[10], list) else []
    score = _round_number(row[6])
    confidence = _round_number(row[7])
    summary = row[8]

    # User-facing fields
    patient_summary = summary or None
    patient_findings = _analysis_findings_text(findings)
    # Confidence band logic
    confidence_band = _analysis_confidence_band(confidence)
    confidence_score = confidence
    # Recommendation (simple logic: if findings, recommend review; else, routine)
    recommendation = None
    if findings:
        recommendation = "Review findings with your clinician."
    elif summary:
        recommendation = "No significant findings. Continue routine monitoring."

    result = {
        "patient_summary": patient_summary,
        "patient_findings": patient_findings,
        "confidence_band": confidence_band,
        "confidence_score": confidence_score,
        "recommendation": recommendation,
        "model_public": model_public,
        # Legacy/compat fields
        "measurement_run_id": row[0],
        "session_id": row[1],
        "encounter_id": row[2],
        "analyzer": row[3],
        "scope": row[4],
        "status": row[5],
        "score": score,
        "confidence": confidence,
        "summary": summary,
        "event_count": row[9] or 0,
        "findings": findings,
        "latest_values": row[11] if isinstance(row[11], dict) else {},
        "features": row[12] if isinstance(row[12], dict) else {},
        "model": model_public,
        "analyzed_at": _iso_utc(row[14]),
    }

    normalized_result = _normalized_history_analysis(result)
    return normalized_result if normalized_result is not None else result

def _image_patient_findings(findings: list[Any]) -> list[str]:
    messages: list[str] = []
    seen: set[str] = set()
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        note = finding.get("note")
        label = finding.get("label")
        confidence = _round_number(_as_number(finding.get("confidence")))
        if isinstance(note, str) and note.strip():
            message = note.strip()
        elif isinstance(label, str) and label.strip():
            display_label = label.strip().replace("_", " ")
            if confidence is not None:
                message = f"{display_label.capitalize()} highlighted for clinician review ({confidence:.0%} confidence)."
            else:
                message = f"{display_label.capitalize()} highlighted for clinician review."
        else:
            continue
        if message in seen:
            continue
        seen.add(message)
        messages.append(message)
    return messages


def _sanitize_visual_analysis_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(payload)
    for secret_key in ("source_key", "overlay_key", "overlay_preview_base64"):
        sanitized.pop(secret_key, None)
    return sanitized


def _normalize_image_analysis_findings(findings: Any) -> list[dict[str, Any]]:
    if not isinstance(findings, list):
        return []
    return [dict(finding) for finding in findings if isinstance(finding, dict)]


def _build_image_analysis_retention(analyzed_at: datetime) -> tuple[str, datetime | None, dict[str, Any]]:
    retention_policy = IMAGE_ANALYSIS_RETENTION_POLICY
    retention_expires_at = analyzed_at + timedelta(days=IMAGE_ANALYSIS_RETENTION_DAYS) if IMAGE_ANALYSIS_RETENTION_DAYS > 0 else None
    retention_meta: dict[str, Any] = {
        "policy": retention_policy,
        "days": IMAGE_ANALYSIS_RETENTION_DAYS,
    }
    if retention_expires_at is not None:
        retention_meta["expires_at"] = _iso_utc(retention_expires_at)
    return retention_policy, retention_expires_at, retention_meta

def _build_visual_history_analysis(
    *,
    status: str,
    summary: str,
    findings: list[Any],
    source_image_id: int,
    overlay_image_id: int,
    analyzed_at: datetime,
) -> dict[str, Any]:
    normalized_findings = _normalize_image_analysis_findings(findings)
    patient_findings = _image_patient_findings(normalized_findings)
    confidence_values = [
        _as_number(finding.get("confidence"))
        for finding in normalized_findings
        if _as_number(finding.get("confidence")) is not None
    ]
    confidence = _round_number(max(confidence_values)) if confidence_values else None
    recommendation = (
        "Review highlighted areas with your clinician."
        if patient_findings
        else "No prominent visual findings were detected. Continue routine monitoring."
    )
    methods = sorted(
        {
            str(method)
            for finding in normalized_findings
            for method in [finding.get("method")]
            if method
        }
    )

    analysis = {
        "analyzer": "visual_screening",
        "scope": "image",
        "status": status,
        "summary": summary,
        "patient_summary": summary,
        "patient_findings": patient_findings,
        "confidence": confidence,
        "confidence_score": confidence,
        "confidence_band": _analysis_confidence_band(confidence),
        "recommendation": recommendation,
        "non_diagnostic": True,
        "findings": normalized_findings,
        "source_image_id": source_image_id,
        "overlay_image_id": overlay_image_id,
        "model": {
            "type": "visual_screening",
            "non_diagnostic": True,
            "methods": methods,
        },
        "analyzed_at": _iso_utc(analyzed_at),
        "_analyzed_dt": analyzed_at,
    }
    return _sanitize_visual_analysis_payload(analysis)

def _resolve_session_encounter_id(cur, *, session_id: str | None) -> str | None:
    if not session_id:
        return None
    cur.execute(
        """
        SELECT COALESCE(encounter_id, id)::text
        FROM user_sessions
        WHERE id=%s
        LIMIT 1
        """,
        (session_id,),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def _resolve_latest_user_device_session_id(
    cur,
    *,
    user_id: str | None,
    device_id: str | None,
) -> str | None:
    if not user_id:
        return None

    if device_id:
        cur.execute(
            """
            SELECT id::text
            FROM user_sessions
            WHERE user_id=%s
              AND device_id=%s
            ORDER BY COALESCE(ended_at, started_at) DESC, started_at DESC, id DESC
            LIMIT 1
            """,
            (user_id, device_id),
        )
    else:
        cur.execute(
            """
            SELECT id::text
            FROM user_sessions
            WHERE user_id=%s
            ORDER BY COALESCE(ended_at, started_at) DESC, started_at DESC, id DESC
            LIMIT 1
            """,
            (user_id,),
        )

    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _resolve_image_analysis_user_id(
    cur,
    *,
    auth_user_id: str | None,
    session_id: str | None,
    device_id: str | None,
) -> str | None:
    if auth_user_id:
        return auth_user_id

    if session_id:
        cur.execute(
            """
            SELECT user_id::text
            FROM user_sessions
            WHERE id=%s
            LIMIT 1
            """,
            (session_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]

    if device_id:
        cur.execute(
            """
            SELECT user_id::text
            FROM devices
            WHERE id=%s
            LIMIT 1
            """,
            (device_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0]

    return None

def _resolve_image_measurement_run_id(
    cur,
    *,
    measurement_run_id: str | None,
    session_id: str | None,
    anchor_ts: datetime | None,
) -> str | None:
    if measurement_run_id or not session_id or anchor_ts is None:
        return measurement_run_id

    cur.execute(
        """
        SELECT measurement_run_id::text
        FROM sensor_events
        WHERE session_id=%s
          AND measurement_run_id IS NOT NULL
          AND ts BETWEEN %s AND %s
        ORDER BY ts DESC, id DESC
        LIMIT 1
        """,
        (
            session_id,
            anchor_ts - timedelta(minutes=IMAGE_ANALYSIS_LINK_WINDOW_MINUTES),
            anchor_ts + timedelta(minutes=5),
        ),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def _persist_image_analysis(
    cur,
    *,
    source_image_id: int,
    overlay_image_id: int,
    user_id: str | None,
    session_id: str | None,
    encounter_id: str | None,
    device_id: str | None,
    measurement_run_id: str | None,
    analysis: dict[str, Any],
    analyzed_at: datetime,
    retention_policy: str,
    retention_expires_at: datetime | None,
    retention_meta: dict[str, Any],
) -> None:
    persisted_analysis = _sanitize_visual_analysis_payload(dict(analysis))
    persisted_analysis.pop("_analyzed_dt", None)
    findings_payload = _normalize_image_analysis_findings(persisted_analysis.get("findings"))

    cur.execute(
        """
        INSERT INTO image_analyses (
            source_image_id,
            overlay_image_id,
            user_id,
            session_id,
            encounter_id,
            device_id,
            measurement_run_id,
            status,
            non_diagnostic,
            summary,
            findings,
            findings_json,
            analysis,
            analyzed_at,
            retention_policy,
            retention_expires_at,
            retention_meta
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s::jsonb)
        ON CONFLICT (source_image_id) DO UPDATE
        SET overlay_image_id = EXCLUDED.overlay_image_id,
            user_id = COALESCE(EXCLUDED.user_id, image_analyses.user_id),
            session_id = COALESCE(EXCLUDED.session_id, image_analyses.session_id),
            encounter_id = COALESCE(EXCLUDED.encounter_id, image_analyses.encounter_id),
            device_id = COALESCE(EXCLUDED.device_id, image_analyses.device_id),
            measurement_run_id = COALESCE(EXCLUDED.measurement_run_id, image_analyses.measurement_run_id),
            status = EXCLUDED.status,
            non_diagnostic = EXCLUDED.non_diagnostic,
            summary = EXCLUDED.summary,
            findings = EXCLUDED.findings,
            findings_json = EXCLUDED.findings_json,
            analysis = EXCLUDED.analysis,
            analyzed_at = EXCLUDED.analyzed_at,
            retention_policy = EXCLUDED.retention_policy,
            retention_expires_at = COALESCE(EXCLUDED.retention_expires_at, image_analyses.retention_expires_at),
            retention_meta = EXCLUDED.retention_meta
        """,
        (
            source_image_id,
            overlay_image_id,
            user_id,
            session_id,
            encounter_id,
            device_id,
            measurement_run_id,
            analysis.get("status") or "analysis_available",
            bool(analysis.get("non_diagnostic", True)),
            str(analysis.get("summary") or ""),
            _json.dumps(findings_payload),
            _json.dumps(findings_payload),
            _json.dumps(persisted_analysis),
            analyzed_at,
            retention_policy,
            retention_expires_at,
            _json.dumps(retention_meta or {}),
        ),
    )

def _image_analysis_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    analysis = row[5] if isinstance(row[5], dict) else {}
    payload = _sanitize_visual_analysis_payload(analysis)
    if row[1] and "measurement_run_id" not in payload:
        payload["measurement_run_id"] = row[1]
    if row[2] and "session_id" not in payload:
        payload["session_id"] = row[2]
    if row[3] and "encounter_id" not in payload:
        payload["encounter_id"] = row[3]
    payload.setdefault("analyzer", "visual_screening")
    payload["non_diagnostic"] = bool(row[4])
    payload["analyzed_at"] = payload.get("analyzed_at") or _iso_utc(row[6])
    payload["_analyzed_dt"] = row[6]
    normalized_payload = _normalized_history_analysis(
        payload,
        fallback_status="analysis_available",
        fallback_summary="Visual analysis is available for this run.",
    )
    return normalized_payload if normalized_payload is not None else payload


def _image_content_url(image_id: int | None, *, target_user_id: str | None) -> str | None:
    if image_id is None or not target_user_id:
        return None
    query = urlencode({"user_id": target_user_id})
    return f"/api/images/{image_id}/content?{query}"


def _image_analysis_history_item_from_row(
    row: tuple[Any, ...],
    *,
    include_previews: bool = False,
    target_user_id: str | None = None,
) -> dict[str, Any]:
    findings_json = _normalize_image_analysis_findings(row[9])
    analysis_payload = _sanitize_visual_analysis_payload(row[15]) if isinstance(row[15], dict) else {}

    summary_value = _coalesce_nonempty(
        row[7],
        analysis_payload.get("patient_summary"),
        analysis_payload.get("summary"),
    )
    summary = str(summary_value).strip() if summary_value is not None else ""
    if not summary:
        summary = "Visual analysis is available for this image."

    if not findings_json:
        findings_json = _normalize_image_analysis_findings(analysis_payload.get("findings"))

    status_value = row[6] if isinstance(row[6], str) else ""
    status = status_value.strip().lower() or "analysis_available"
    retention_meta = row[14] if isinstance(row[14], dict) else {}
    source_image_id = int(row[1])
    overlay_image_id = int(row[2]) if row[2] is not None else None
    source_preview_base64 = None
    overlay_preview_base64 = None
    if include_previews:
        source_preview_base64 = _build_image_preview_base64(bucket=row[16], key=row[17])
        if overlay_image_id is not None:
            overlay_preview_base64 = _build_image_preview_base64(bucket=row[19], key=row[20])

    return {
        "analysis_id": int(row[0]),
        "source_image_id": source_image_id,
        "overlay_image_id": overlay_image_id,
        "source_image_content_url": _image_content_url(source_image_id, target_user_id=target_user_id),
        "overlay_image_content_url": _image_content_url(overlay_image_id, target_user_id=target_user_id),
        "source_preview_base64": source_preview_base64,
        "overlay_preview_base64": overlay_preview_base64,
        "session_id": row[3],
        "encounter_id": row[4],
        "measurement_run_id": row[5],
        "status": status,
        "summary": summary,
        "findings_json": findings_json,
        "findings": findings_json,
        "non_diagnostic": bool(row[8]),
        "created_at": _iso_utc(row[10]),
        "analyzed_at": _iso_utc(row[11]),
        "retention_policy": row[12],
        "retention_expires_at": _iso_utc(row[13]),
        "retention_meta": retention_meta,
    }

def _resolve_analysis_subject(
    cur,
    *,
    session_id: str | None,
    encounter_id: str | None,
    device_id: str,
) -> tuple[str | None, str]:
    if session_id:
        cur.execute(
            """
            SELECT user_id::text
            FROM user_sessions
            WHERE id=%s
            LIMIT 1
            """,
            (session_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0], "user"

    if encounter_id:
        cur.execute(
            """
            SELECT user_id::text
            FROM user_sessions
            WHERE encounter_id=%s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (encounter_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            return row[0], "user"

    return None, "device"

def _historical_metric_values(
    cur,
    *,
    user_id: str | None,
    device_id: str,
    measurement_run_id: str | None,
) -> tuple[dict[str, list[float]], int]:
    where: list[str] = ["sa.status != 'artifact'"]
    params: list[Any] = []

    if user_id:
        where.append(
            """
            EXISTS (
                SELECT 1
                FROM user_sessions us
                WHERE us.user_id = %s
                  AND (
                      us.id = sa.session_id
                      OR (sa.encounter_id IS NOT NULL AND us.encounter_id = sa.encounter_id)
                  )
            )
            """
        )
        params.append(user_id)
    else:
        where.append("sa.device_id = %s")
        params.append(device_id)

    if measurement_run_id:
        where.append("sa.measurement_run_id <> %s")
        params.append(measurement_run_id)

    params.append(SENSOR_MODEL_HISTORY_LIMIT)

    cur.execute(
        f"""
        SELECT sa.latest_values
        FROM sensor_analysis sa
        WHERE {' AND '.join(where)}
        ORDER BY sa.analyzed_at DESC
        LIMIT %s
        """,
        params,
    )
    rows = cur.fetchall()

    history: dict[str, list[float]] = {}
    for (latest_values,) in rows:
        if not isinstance(latest_values, dict):
            continue
        for metric, raw_value in latest_values.items():
            value = _as_number(raw_value)
            if value is None:
                continue
            history.setdefault(metric, []).append(value)
    return history, len(rows)

def _robust_baseline_stats(values: list[float]) -> dict[str, float] | None:
    if len(values) < SENSOR_MODEL_MIN_SAMPLES:
        return None

    median = statistics.median(values)
    abs_deviations = [abs(value - median) for value in values]
    mad = statistics.median(abs_deviations)
    mean = statistics.fmean(values)
    variance = 0.0
    if len(values) > 1:
        variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    stddev = math.sqrt(max(variance, 0.0))

    return {
        "median": median,
        "mad": mad,
        "mean": mean,
        "stddev": stddev,
        "samples": float(len(values)),
    }

def _metric_label(metric: str) -> str:
    spec = ANALYSIS_SERIES_SPECS.get(metric) or TREND_SERIES_SPECS.get(metric) or {}
    return str(spec.get("label") or metric.replace("_", " ").title())

def _apply_baseline_model(
    cur,
    *,
    analysis: dict[str, Any],
    device_id: str,
    session_id: str | None,
    encounter_id: str | None,
    measurement_run_id: str | None,
) -> dict[str, Any]:
    latest_values = {
        metric: float(value)
        for metric, value in (analysis.get("latest_values") or {}).items()
        if isinstance(value, (int, float))
    }

    subject_user_id, subject_type = _resolve_analysis_subject(
        cur,
        session_id=session_id,
        encounter_id=encounter_id,
        device_id=device_id,
    )
    history, history_rows = _historical_metric_values(
        cur,
        user_id=subject_user_id,
        device_id=device_id,
        measurement_run_id=measurement_run_id,
    )

    model: dict[str, Any] = {
        "name": SENSOR_MODEL_VERSION,
        "type": "adaptive_baseline",
        "status": "warming_up",
        "ready": False,
        "subject_type": subject_type,
        "history_runs": history_rows,
        "sample_count": 0,
        "score": 0.0,
        "contributors": [],
    }
    model_bucket = analysis.get("model")
    if not isinstance(model_bucket, dict):
        model_bucket = {}
    model_bucket["baseline"] = model
    analysis["model"] = model_bucket

    if not latest_values:
        return analysis

    contributors: list[dict[str, Any]] = []
    sample_counts: list[int] = []
    for metric, current_value in latest_values.items():
        baseline = _robust_baseline_stats(history.get(metric, []))
        if baseline is None:
            continue

        mad = baseline["mad"]
        stddev = baseline["stddev"]
        if mad > 0:
            z_score = 0.6745 * abs(current_value - baseline["median"]) / mad
        elif stddev > 0:
            z_score = abs(current_value - baseline["mean"]) / stddev
        else:
            z_score = 0.0

        contributors.append(
            {
                "metric": metric,
                "label": _metric_label(metric),
                "value": _round_number(current_value),
                "baseline_median": _round_number(baseline["median"]),
                "baseline_mad": _round_number(baseline["mad"]),
                "baseline_stddev": _round_number(baseline["stddev"]),
                "samples": int(baseline["samples"]),
                "z_score": _round_number(z_score),
            }
        )
        sample_counts.append(int(baseline["samples"]))

    if not contributors:
        return analysis

    contributors.sort(key=lambda item: item.get("z_score") or 0.0, reverse=True)
    max_z = float(contributors[0].get("z_score") or 0.0)
    top_n = contributors[:2]
    avg_top_z = sum(float(item.get("z_score") or 0.0) for item in top_n) / len(top_n)
    model_score = min(1.0, max_z / 6.0)

    model.update(
        {
            "status": "ready",
            "ready": True,
            "sample_count": min(sample_counts),
            "score": _round_number(model_score),
            "contributors": contributors[:3],
        }
    )

    if analysis.get("status") == "artifact" or max_z < SENSOR_MODEL_ANOMALY_Z:
        return analysis

    severity = "high" if max_z >= SENSOR_MODEL_ANOMALY_Z + 1.5 else "medium"
    lead = contributors[0]
    finding = _make_analysis_finding(
        "baseline_shift",
        severity,
        "model",
        f"{lead['label']} is shifted away from this subject's recent baseline.",
        lead["metric"],
    )

    findings = analysis.get("findings") or []
    findings.append(finding)
    analysis["findings"] = findings
    analysis["score"] = _round_number(max(float(analysis.get("score") or 0.0), model_score))
    analysis["confidence"] = _round_number(max(float(analysis.get("confidence") or 0.0), 0.55 if min(sample_counts) >= 10 else 0.45))

    baseline_summary = f"{lead['label']} is shifted away from this subject's recent baseline."
    if analysis.get("status") == "normal":
        analysis["status"] = "unusual"
        analysis["summary"] = f"{baseline_summary} {analysis.get('summary') or ''}".strip()
    else:
        analysis["summary"] = f"{analysis.get('summary') or ''} {baseline_summary}".strip()

    model["score"] = _round_number(max(model_score, avg_top_z / 6.0))
    return analysis

def _apply_public_dataset_model(analysis: dict[str, Any]) -> dict[str, Any]:
    metric_summaries = (analysis.get("features") or {}).get("metrics")
    if not isinstance(metric_summaries, dict):
        return analysis

    model_bucket = analysis.get("model")
    if not isinstance(model_bucket, dict):
        model_bucket = {}

    artifacts = _get_public_sensor_models()
    public_results: list[dict[str, Any]] = []
    for artifact in artifacts:
        result = score_public_model(artifact, metric_summaries)
        if isinstance(result, dict):
            result = dict(result)
            result["model_path"] = artifact.get("_model_path")
        public_results.append(result)

    ready_results = [result for result in public_results if result.get("ready")]
    ready_results.sort(
        key=lambda result: (
            -int(result.get("metric_count") or 0),
            -int(result.get("inference_priority") or 0),
            float(result.get("training_quality") or 1e9),
            -int(result.get("train_windows") or 0),
            float(result.get("score") or 0.0),
        ),
    )

    best_public_result = ready_results[0] if ready_results else (public_results[0] if public_results else {"ready": False, "status": "missing_artifact"})
    model_bucket["public"] = best_public_result
    model_bucket["public_models"] = public_results
    analysis["model"] = model_bucket

    if not ready_results:
        return analysis

    findings = analysis.get("findings") or []
    public_summaries: list[str] = []
    selected_by_group: dict[str, dict[str, Any]] = {}
    for result in ready_results:
        group = str(result.get("model_group") or "public")
        if group not in selected_by_group:
            selected_by_group[group] = result

    for group_result in selected_by_group.values():
        threshold = float(group_result.get("threshold") or 0.0)
        distance = float(group_result.get("distance") or 0.0)
        if threshold <= 0 or distance < threshold:
            continue

        contributors = group_result.get("contributors") or []
        lead_metric = contributors[0]["metric"] if contributors else None
        lead_label = _metric_label(lead_metric) if isinstance(lead_metric, str) else "Current readings"
        dataset_name = str(group_result.get("dataset_name") or "public")
        severity = "high" if distance >= threshold * 1.25 else "medium"
        finding = _make_analysis_finding(
            "public_model_shift",
            severity,
            "model",
            f"{lead_label} differs from the {dataset_name} public training distribution.",
            lead_metric if isinstance(lead_metric, str) else None,
        )
        findings.append(finding)
        public_summaries.append(finding["message"])
        model_score = float(group_result.get("score") or 0.0)
        analysis["score"] = _round_number(max(float(analysis.get("score") or 0.0), model_score))
        analysis["confidence"] = _round_number(max(float(analysis.get("confidence") or 0.0), 0.4))

    analysis["findings"] = findings
    if not public_summaries:
        return analysis

    public_summary = " ".join(public_summaries[:2])
    if analysis.get("status") == "normal":
        analysis["status"] = "unusual"
        analysis["summary"] = f"{public_summary} {analysis.get('summary') or ''}".strip()
    elif analysis.get("status") != "artifact":
        analysis["summary"] = f"{analysis.get('summary') or ''} {public_summary}".strip()

    return analysis


def _apply_personalization_model(
    cur,
    *,
    analysis: dict[str, Any],
    device_id: str,
    session_id: str | None,
    encounter_id: str | None,
    measurement_run_id: str | None,
) -> dict[str, Any]:
    model_bucket = analysis.get("model")
    if not isinstance(model_bucket, dict):
        model_bucket = {}

    personalization_model: dict[str, Any] = {
        "name": "personalization-calibration-v1",
        "type": "calibration",
        "enabled": SENSOR_PERSONALIZATION_ENABLED,
        "ready": False,
        "status": "disabled" if not SENSOR_PERSONALIZATION_ENABLED else "warming_up",
        "history_runs": 0,
        "sample_count": 0,
        "max_z_score": None,
        "score_reduction": 0.0,
        "confidence_boost": 0.0,
        "contributors": [],
    }
    model_bucket["personalization"] = personalization_model
    analysis["model"] = model_bucket

    if not SENSOR_PERSONALIZATION_ENABLED:
        return analysis

    latest_values = {
        metric: float(value)
        for metric, value in (analysis.get("latest_values") or {}).items()
        if isinstance(value, (int, float))
    }
    if not latest_values:
        return analysis

    subject_user_id, _subject_type = _resolve_analysis_subject(
        cur,
        session_id=session_id,
        encounter_id=encounter_id,
        device_id=device_id,
    )
    history, history_rows = _historical_metric_values(
        cur,
        user_id=subject_user_id,
        device_id=device_id,
        measurement_run_id=measurement_run_id,
    )
    personalization_model["history_runs"] = int(history_rows)

    contributors: list[dict[str, Any]] = []
    sample_counts: list[int] = []
    for metric, current_value in latest_values.items():
        series = history.get(metric, [])
        if len(series) < SENSOR_PERSONALIZATION_MIN_SAMPLES:
            continue

        baseline = _robust_baseline_stats(series)
        if baseline is None:
            continue

        mad = float(baseline.get("mad") or 0.0)
        stddev = float(baseline.get("stddev") or 0.0)
        median = float(baseline.get("median") or 0.0)
        mean = float(baseline.get("mean") or 0.0)
        if mad > 0:
            z_score = 0.6745 * abs(current_value - median) / mad
        elif stddev > 0:
            z_score = abs(current_value - mean) / stddev
        else:
            z_score = 0.0

        samples = int(baseline.get("samples") or 0)
        contributors.append(
            {
                "metric": metric,
                "label": _metric_label(metric),
                "value": _round_number(current_value),
                "baseline_median": _round_number(median),
                "samples": samples,
                "z_score": _round_number(z_score),
            }
        )
        sample_counts.append(samples)

    if not contributors:
        return analysis

    contributors.sort(key=lambda item: item.get("z_score") or 0.0, reverse=True)
    max_z = float(contributors[0].get("z_score") or 0.0)
    sample_floor = min(sample_counts)
    history_factor = min(1.0, sample_floor / 30.0)
    stability = max(0.0, min(1.0, 1.0 - (max_z / max(1.0, SENSOR_PERSONALIZATION_ALERT_Z))))

    score_reduction = SENSOR_PERSONALIZATION_MAX_SCORE_REDUCTION * history_factor * stability
    boosted_confidence = SENSOR_PERSONALIZATION_MAX_CONFIDENCE_BOOST * history_factor

    personalization_model.update(
        {
            "ready": True,
            "status": "ready",
            "sample_count": sample_floor,
            "max_z_score": _round_number(max_z),
            "score_reduction": _round_number(score_reduction, 4),
            "confidence_boost": _round_number(boosted_confidence, 4),
            "contributors": contributors[:3],
        }
    )

    current_score = float(analysis.get("score") or 0.0)
    if score_reduction > 0:
        analysis["score"] = _round_number(max(0.0, current_score - score_reduction))

    current_confidence = float(analysis.get("confidence") or 0.0)
    if boosted_confidence > 0:
        analysis["confidence"] = _round_number(min(0.98, current_confidence + boosted_confidence))

    if analysis.get("status") == "normal" and max_z >= SENSOR_PERSONALIZATION_ALERT_Z:
        lead = contributors[0]
        findings = analysis.get("findings") or []
        findings.append(
            _make_analysis_finding(
                "personalized_shift",
                "high" if max_z >= (SENSOR_PERSONALIZATION_ALERT_Z + 1.0) else "medium",
                "model",
                f"{lead['label']} is outside this subject's usual pattern.",
                str(lead.get("metric") or "") or None,
            )
        )
        analysis["findings"] = findings
        analysis["status"] = "unusual"
        analysis["summary"] = (
            f"{lead['label']} is outside this subject's usual pattern. "
            f"{analysis.get('summary') or ''}"
        ).strip()

    return analysis

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
            data = _normalized_sensor_payload(data)
            for k in keys:
                if k in data:
                    n = _as_number(data.get(k))
                    if n is not None:
                        return n
            return None

        def _latest_measurement(events: list[dict], kinds: set[str], keys: list[str]):
            specific_keys = [key for key in keys if key != "value"] or keys
            for ev in events:
                if ev.get("kind") in kinds:
                    n = _extract_from_data(ev.get("data") or {}, keys)
                    if n is not None:
                        return n
                n = _extract_from_data(ev.get("data") or {}, specific_keys)
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

        def _expand_context_maps(value: Any) -> list[dict[str, Any]]:
            maps: list[dict[str, Any]] = []
            if not isinstance(value, dict):
                return maps
            maps.append(value)
            for key in ("environment", "vitals", "summary", "sensor_summary"):
                nested = value.get(key)
                if isinstance(nested, dict):
                    maps.append(nested)
            return maps

        def _extract_environment_context(*sources: Any) -> dict[str, Any]:
            candidate_maps: list[dict[str, Any]] = []
            for source in sources:
                candidate_maps.extend(_expand_context_maps(source))

            environment = {
                "ambient_temperature_c": _coalesce(
                    *[
                        _extract_from_data(
                            data,
                            [
                                "ambient_temperature_c",
                                "ambient_temperature",
                                "ambient_temp_c",
                                "room_temperature_c",
                            ],
                        )
                        for data in candidate_maps
                    ]
                ),
                "humidity_percent": _coalesce(
                    *[
                        _extract_from_data(
                            data,
                            [
                                "humidity_percent",
                                "humidity",
                                "relative_humidity",
                                "relative_humidity_percent",
                                "rh",
                            ],
                        )
                        for data in candidate_maps
                    ]
                ),
                "co2_ppm": _coalesce(
                    *[
                        _extract_from_data(
                            data,
                            [
                                "co2_ppm",
                                "eco2_ppm",
                                "co2",
                                "ppm",
                                "ens_eco2_ppm",
                                "ens_eCO2_ppm",
                            ],
                        )
                        for data in candidate_maps
                    ]
                ),
            }
            return {key: value for key, value in environment.items() if value is not None}

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
                       se.measurement_run_id::text,
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
                    "measurement_run_id": r[2],
                    "device_id": r[3],
                    "ts": _iso(r[4]),
                    "_ts": r[4],
                    "kind": r[5],
                    "seq": r[6],
                    "data": r[7] or {},
                }
                for r in cur.fetchall()
            ]

        def _collect_values(events: list[dict], kinds: set[str], keys: list[str]):
            specific_keys = [key for key in keys if key != "value"] or keys
            vals = []
            for ev in events:
                if ev.get("kind") in kinds:
                    n = _extract_from_data(ev.get("data") or {}, keys)
                    if n is not None:
                        vals.append(n)
                    continue
                n = _extract_from_data(ev.get("data") or {}, specific_keys)
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

        def _summarize_sensor_events(events: list[dict]) -> tuple[dict[str, Any], dict[str, Any]]:
            sensor_vitals: dict[str, Any] = {}
            sensor_summary: dict[str, Any] = {}
            if not events:
                return sensor_vitals, sensor_summary

            ordered_events = sorted(
                events,
                key=lambda ev: ev.get("_ts") or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )

            hr = _latest_measurement(
                ordered_events,
                {"hr", "heart_rate", "pulse"},
                ["hr", "heart_rate", "pulse", "bpm", "value"],
            )
            spo2 = _latest_measurement(
                ordered_events,
                {"spo2", "pulse_ox", "pulse_oxygen", "oxygen"},
                ["spo2", "oxygen_saturation", "oxygen", "o2", "value"],
            )
            temp = _latest_measurement(
                ordered_events,
                {"temp", "temperature", "body_temp"},
                ["temp", "temperature", "celsius", "value"],
            )
            rr = _latest_measurement(
                ordered_events,
                {"rr", "resp_rate", "respiratory_rate"},
                ["rr", "resp_rate", "respiratory_rate", "value"],
            )
            ambient_temp = _latest_measurement(
                ordered_events,
                {"ambient_temperature", "ambient_temp", "room_temperature"},
                ["ambient_temperature_c", "ambient_temperature", "ambient_temp_c", "room_temperature_c"],
            )
            humidity = _latest_measurement(
                ordered_events,
                {"humidity", "humid"},
                ["humidity_percent", "humidity", "relative_humidity", "rh"],
            )
            co2 = _latest_measurement(
                ordered_events,
                {"co2"},
                ["eco2_ppm", "co2_ppm", "co2", "ppm"],
            )
            bp = _latest_bp(ordered_events)
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
            if ambient_temp is not None:
                sensor_summary["ambient_temperature_c"] = {"latest": ambient_temp}
            if humidity is not None:
                sensor_summary["humidity_percent"] = {"latest": humidity}
            if co2 is not None:
                sensor_summary["co2_ppm"] = {"latest": co2}

            hr_vals = _collect_values(ordered_events, {"hr", "heart_rate", "pulse"}, ["hr", "heart_rate", "pulse", "bpm", "value"])
            spo2_vals = _collect_values(ordered_events, {"spo2", "pulse_ox", "pulse_oxygen", "oxygen"}, ["spo2", "oxygen_saturation", "oxygen", "o2", "value"])
            temp_vals = _collect_values(ordered_events, {"temp", "temperature", "body_temp"}, ["temp", "temperature", "celsius", "value"])
            rr_vals = _collect_values(ordered_events, {"rr", "resp_rate", "respiratory_rate"}, ["rr", "resp_rate", "respiratory_rate", "value"])
            ambient_temp_vals = _collect_values(
                ordered_events,
                {"ambient_temperature", "ambient_temp", "room_temperature"},
                ["ambient_temperature_c", "ambient_temperature", "ambient_temp_c", "room_temperature_c"],
            )
            humidity_vals = _collect_values(
                ordered_events,
                {"humidity", "humid"},
                ["humidity_percent", "humidity", "relative_humidity", "rh"],
            )
            co2_vals = _collect_values(
                ordered_events,
                {"co2"},
                ["eco2_ppm", "co2_ppm", "co2", "ppm"],
            )

            hr_range = _range(hr_vals, min_allowed=1)
            spo2_range = _range(spo2_vals, min_allowed=1, max_allowed=100)
            temp_range = _range(temp_vals, min_allowed=1)
            rr_range = _range(rr_vals, min_allowed=1)
            ambient_temp_range = _range(ambient_temp_vals)
            humidity_range = _range(humidity_vals, min_allowed=0, max_allowed=100)
            co2_range = _range(co2_vals, min_allowed=1)

            if hr_range:
                sensor_summary["hr"] = hr_range
            if spo2_range:
                sensor_summary["spo2"] = spo2_range
            if temp_range:
                sensor_summary["temp"] = temp_range
            if rr_range:
                sensor_summary["rr"] = rr_range
            if ambient_temp_range:
                sensor_summary["ambient_temperature_c"] = {
                    **sensor_summary.get("ambient_temperature_c", {}),
                    **ambient_temp_range,
                }
            if humidity_range:
                sensor_summary["humidity_percent"] = {
                    **sensor_summary.get("humidity_percent", {}),
                    **humidity_range,
                }
            if co2_range:
                sensor_summary["co2_ppm"] = {
                    **sensor_summary.get("co2_ppm", {}),
                    **co2_range,
                }

            return sensor_vitals, sensor_summary

        def _group_measurement_runs(events: list[dict]) -> list[dict[str, Any]]:
            grouped: dict[str, dict[str, Any]] = {}
            for ev in events:
                run_id = ev.get("measurement_run_id")
                ts_dt = ev.get("_ts")
                if not run_id or ts_dt is None:
                    continue
                group = grouped.setdefault(
                    run_id,
                    {
                        "measurement_run_id": run_id,
                        "session_id": ev.get("session_id"),
                        "_started_ts": ts_dt,
                        "_ended_ts": ts_dt,
                        "event_count": 0,
                        "kinds": set(),
                        "_events": [],
                    },
                )
                if not group.get("session_id") and ev.get("session_id"):
                    group["session_id"] = ev.get("session_id")
                if ts_dt < group["_started_ts"]:
                    group["_started_ts"] = ts_dt
                if ts_dt > group["_ended_ts"]:
                    group["_ended_ts"] = ts_dt
                if ev.get("kind"):
                    group["kinds"].add(ev["kind"])
                group["event_count"] += 1
                group["_events"].append(ev)

            runs: list[dict[str, Any]] = []
            for group in grouped.values():
                run_vitals, run_summary = _summarize_sensor_events(group["_events"])
                runs.append(
                    {
                        "measurement_run_id": group["measurement_run_id"],
                        "session_id": group.get("session_id"),
                        "started_at": _iso(group["_started_ts"]),
                        "ended_at": _iso(group["_ended_ts"]),
                        "event_count": group["event_count"],
                        "kinds": sorted(group["kinds"]),
                        "vitals": run_vitals,
                        "summary": run_summary,
                        "_ended_ts": group["_ended_ts"],
                    }
                )

            runs.sort(key=lambda run: run["_ended_ts"], reverse=True)
            trimmed_runs = runs[:limit_sessions]
            for run in trimmed_runs:
                run.pop("_ended_ts", None)
            return trimmed_runs

        measurement_runs: list[dict[str, Any]] = []
        sensor_vitals: dict[str, Any] = {}
        sensor_summary: dict[str, Any] = {}
        sensor_environment: dict[str, Any] = {}
        if sensor_events:
            measurement_runs = _group_measurement_runs(sensor_events)
            measurement_events = [ev for ev in sensor_events if ev.get("measurement_run_id")]
            analysis_events = measurement_events or sensor_events
            sensor_vitals, sensor_summary = _summarize_sensor_events(analysis_events)
            _, environment_summary = _summarize_sensor_events(sensor_events)
            for key in ("ambient_temperature_c", "humidity_percent", "co2_ppm"):
                if key in environment_summary:
                    sensor_summary[key] = environment_summary[key]
            sensor_environment = {
                key: value.get("latest")
                for key, value in environment_summary.items()
                if key in {"ambient_temperature_c", "humidity_percent", "co2_ppm"}
                and isinstance(value, dict)
                and value.get("latest") is not None
            }

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
            sensor_vitals,
            profile_data.get("vitals") if isinstance(profile_data, dict) else None,
            memory.get("vitals") if isinstance(memory, dict) else None,
        )
        exam = _coalesce(
            profile_data.get("exam") if isinstance(profile_data, dict) else None,
            memory.get("exam") if isinstance(memory, dict) else None,
        )
        environment = _extract_environment_context(
            sensor_environment,
            profile_data,
            memory if isinstance(memory, dict) else None,
        )

    payload = {
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
    if environment:
        payload["environment"] = environment
    if measurement_runs:
        payload["measurement_runs"] = measurement_runs
    return payload

USER_ROLE_PATIENT = "patient"
USER_ROLE_DOCTOR = "doctor"
CONSULT_STATUS_NEW = "new"
CONSULT_STATUS_IN_REVIEW = "in_review"
CONSULT_STATUS_AWAITING_PATIENT = "awaiting_patient"
CONSULT_STATUS_FOLLOW_UP_DUE = "follow_up_due"
CONSULT_STATUS_CLOSED = "closed"
CONSULT_STATUS_VALUES = frozenset(
    {
        CONSULT_STATUS_NEW,
        CONSULT_STATUS_IN_REVIEW,
        CONSULT_STATUS_AWAITING_PATIENT,
        CONSULT_STATUS_FOLLOW_UP_DUE,
        CONSULT_STATUS_CLOSED,
    }
)
CONSULT_PRIORITY_ROUTINE = "routine"
CONSULT_PRIORITY_URGENT = "urgent"
CONSULT_PRIORITY_CRITICAL = "critical"
CONSULT_PRIORITY_VALUES = frozenset(
    {
        CONSULT_PRIORITY_ROUTINE,
        CONSULT_PRIORITY_URGENT,
        CONSULT_PRIORITY_CRITICAL,
    }
)
CONSULT_ESCALATION_NONE = "none"
CONSULT_ESCALATION_WATCH = "watch"
CONSULT_ESCALATION_ESCALATED = "escalated"
CONSULT_ESCALATION_TRANSFERRED = "transferred"
CONSULT_ESCALATION_VALUES = frozenset(
    {
        CONSULT_ESCALATION_NONE,
        CONSULT_ESCALATION_WATCH,
        CONSULT_ESCALATION_ESCALATED,
        CONSULT_ESCALATION_TRANSFERRED,
    }
)
CONSULT_MESSAGE_TYPE_VALUES = frozenset({"message", "instruction", "system"})
CONSULT_SENDER_TYPE_VALUES = frozenset({USER_ROLE_PATIENT, USER_ROLE_DOCTOR, "system"})
PATIENT_DOCTOR_REQUEST_PENDING = "pending"
PATIENT_DOCTOR_REQUEST_CLAIMED = "claimed"
PATIENT_DOCTOR_REQUEST_CANCELLED = "cancelled"
PATIENT_DOCTOR_REQUEST_STATUS_VALUES = frozenset(
    {
        PATIENT_DOCTOR_REQUEST_PENDING,
        PATIENT_DOCTOR_REQUEST_CLAIMED,
        PATIENT_DOCTOR_REQUEST_CANCELLED,
    }
)
PRIORITY_RANK = {
    CONSULT_PRIORITY_ROUTINE: 0,
    CONSULT_PRIORITY_URGENT: 1,
    CONSULT_PRIORITY_CRITICAL: 2,
}


def _normalized_claim_token(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _normalized_user_role(value: Any) -> str:
    if isinstance(value, str) and value.strip().lower() == USER_ROLE_DOCTOR:
        return USER_ROLE_DOCTOR
    return USER_ROLE_PATIENT


def _normalized_consult_status(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in CONSULT_STATUS_VALUES else CONSULT_STATUS_NEW


def _normalized_consult_priority(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in CONSULT_PRIORITY_VALUES else CONSULT_PRIORITY_ROUTINE


def _normalized_escalation_status(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in CONSULT_ESCALATION_VALUES else CONSULT_ESCALATION_NONE


def _normalized_message_type(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in CONSULT_MESSAGE_TYPE_VALUES else "message"


def _normalized_sender_type(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in CONSULT_SENDER_TYPE_VALUES else "system"


def _normalized_patient_doctor_request_status(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in PATIENT_DOCTOR_REQUEST_STATUS_VALUES else PATIENT_DOCTOR_REQUEST_PENDING


def _normalized_if_match_etag(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip()
    if not token:
        return None
    if token.startswith("W/"):
        token = token[2:].strip()
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        token = token[1:-1]
    return token or None


def _claim_is_truthy(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"true", "1", "yes", "y", "on"}:
            return True
        if token in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _claim_tokens(value: Any) -> list[str]:
    tokens: list[str] = []

    def _visit(raw: Any) -> None:
        if raw is None:
            return
        if isinstance(raw, str):
            parts = raw.split(",") if "," in raw else [raw]
            for part in parts:
                token = _normalized_claim_token(part)
                if token:
                    tokens.append(token)
            return
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                _visit(item)
            return
        if isinstance(raw, dict):
            for key, item in raw.items():
                if _claim_is_truthy(item):
                    _visit(key)

    _visit(value)
    return list(dict.fromkeys(tokens))


def _portal_for_role(role: str) -> str:
    return USER_ROLE_DOCTOR if _normalized_user_role(role) == USER_ROLE_DOCTOR else USER_ROLE_PATIENT


def _default_redirect_for_role(role: str) -> str:
    if _portal_for_role(role) == USER_ROLE_DOCTOR:
        return AUTH_SUCCESS_REDIRECT_DOCTOR
    return AUTH_SUCCESS_REDIRECT_PATIENT


def _redirect_path_from_url(url: str) -> str:
    parsed = urlsplit(url or "")
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    if parsed.fragment:
        path = f"{path}#{parsed.fragment}"
    return path


def _resolve_user_role_from_claims(info: dict[str, Any] | None) -> tuple[str | None, list[str]]:
    if not isinstance(info, dict):
        return None, []

    labels: list[str] = []
    claims_present = False

    for key in AUTH_ROLE_CLAIM_KEYS:
        if key not in info:
            continue
        raw_value = info.get(key)
        tokens = _claim_tokens(raw_value)
        if tokens:
            claims_present = True
            labels.extend(tokens)
        elif raw_value is not None:
            claims_present = True

    labels = list(dict.fromkeys(labels))

    for bool_key in ("is_doctor", "isDoctor"):
        if bool_key not in info:
            continue
        claims_present = True
        truthy = _claim_is_truthy(info.get(bool_key))
        if truthy is True:
            return USER_ROLE_DOCTOR, labels
        if truthy is False:
            return USER_ROLE_PATIENT, labels

    if any(label in AUTH_DOCTOR_GROUPS or label == USER_ROLE_DOCTOR for label in labels):
        return USER_ROLE_DOCTOR, labels
    if claims_present:
        return USER_ROLE_PATIENT, labels
    return None, []


def _require_user_auth_context(user_id: str) -> dict[str, Any]:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT email, name, role
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(401, "Unknown user")

    role = _normalized_user_role(row[2])
    redirect_url = _default_redirect_for_role(role)
    return {
        "user_id": user_id,
        "email": row[0],
        "name": row[1],
        "role": role,
        "portal": _portal_for_role(role),
        "is_doctor": role == USER_ROLE_DOCTOR,
        "default_redirect_url": redirect_url,
        "default_redirect_path": _redirect_path_from_url(redirect_url),
    }


def _build_auth_response_payload(
    user_id: str,
    *,
    access_token: str | None = None,
    refresh_token: str | None = None,
) -> dict[str, Any]:
    payload = _require_user_auth_context(user_id)
    if access_token is not None:
        payload["access_token"] = access_token
    if refresh_token is not None:
        payload["refresh_token"] = refresh_token
    return payload


def make_access_token(user_id: str, role: str | None = None) -> str:
    now = _utcnow()
    resolved_role = _normalized_user_role(role) if role is not None else _require_user_auth_context(user_id)["role"]
    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": user_id,
        "role": resolved_role,
        "portal": _portal_for_role(resolved_role),
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
    encounter_id: str | None = None

class StartSessionResp(BaseModel):
    session_id: str
    device_id: str
    encounter_id: str

class EndSessionReq(BaseModel):
    session_id: str
    ended_at: datetime | None = None

class SensorReadingReq(BaseModel):
    ts: datetime | None = None
    seq: int | None = None
    data: dict[str, Any] = Field(default_factory=dict)

class SensorEventReq(BaseModel):
    device_external_id: str
    session_id: str | None = None
    measurement_run_id: str | None = None
    encounter_id: str | None = None
    ts: datetime | None = None
    kind: str
    seq: int | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    readings: list[SensorReadingReq] = Field(default_factory=list)

class OpticalCaptureRunCreateReq(BaseModel):
    model_config = ConfigDict(extra="allow")

    device_external_id: str
    session_id: str
    encounter_id: str | None = None
    measurement_run_id: str | None = None
    mode: str = OPTICAL_MEASUREMENT_MODE
    started_at: datetime | None = None
    sample_rate_hz: float | None = Field(default=None, gt=0, le=10000)
    requested_measurements: list[str] = Field(default_factory=list)


class OpticalCaptureChunkReq(BaseModel):
    model_config = ConfigDict(extra="allow")

    chunk_index: int = Field(ge=0)
    chunk_started_at: datetime | None = None
    chunk_ended_at: datetime | None = None
    packet_loss_count_delta: int = Field(
        default=0,
        ge=0,
        description="Packet-loss count observed within this chunk. Omitting the field and sending 0 are treated the same.",
    )
    checksum_failure_count_delta: int = Field(
        default=0,
        ge=0,
        description="Checksum-failure count observed within this chunk. Omitting the field and sending 0 are treated the same.",
    )
    malformed_packet_count_delta: int = Field(
        default=0,
        ge=0,
        description="Malformed-packet count observed within this chunk. Omitting the field and sending 0 are treated the same.",
    )
    ppg_packets: list[dict[str, Any]] = Field(default_factory=list)
    ts1_frames: list[dict[str, Any]] = Field(default_factory=list)


class OpticalCaptureStopReq(BaseModel):
    stopped_at: datetime | None = None


class OpticalFinalOpticalResult(BaseModel):
    heart_rate_bpm: float | None = None
    spo2_percent: float | None = None
    confidence: float = 0.0
    quality: str = OPTICAL_FINAL_OPTICAL_QUALITY_INSUFFICIENT
    sample_count_used: int = 0
    algorithm_version: str = OPTICAL_FINAL_OPTICAL_ALGORITHM_VERSION
    window_seconds: float = 0.0
    derived_from: str = "raw_ppg"
    rejection_reason: str | None = None


class MeasurementRunAnalysisResp(BaseModel):
    measurement_run_id: str
    status: str = OPTICAL_ANALYSIS_STATUS_PROCESSING
    final_optical: OpticalFinalOpticalResult | None = None
    analyzed_at: datetime | None = None


class OpticalCaptureRunItem(BaseModel):
    measurement_run_id: str
    patient_user_id: str
    encounter_id: str | None = None
    session_id: str | None = None
    device_external_id: str
    mode: str = OPTICAL_MEASUREMENT_MODE
    status: str = OPTICAL_RUN_STATUS_ACTIVE
    started_at: datetime
    stopped_at: datetime | None = None
    sample_rate_hz: float | None = None
    requested_measurements: list[str] = Field(default_factory=list)
    raw_waveform_available: bool = False
    total_chunks: int = 0
    total_packets: int = 0
    total_samples: int = 0
    ts1_frame_count: int = 0
    packet_loss_count: int = 0
    checksum_failure_count: int = 0
    malformed_packet_count: int = 0
    latest_chunk_at: datetime | None = None
    waveform_api_path: str | None = None
    waveform_stream_path: str | None = None


class OpticalCaptureRunResp(BaseModel):
    run: OpticalCaptureRunItem
    analysis: MeasurementRunAnalysisResp | None = None
    created: bool = False
    already_exists: bool = False
    already_stopped: bool = False


class OpticalCaptureChunkResp(BaseModel):
    measurement_run_id: str
    chunk_index: int
    stored: bool = True
    duplicate: bool = False
    run: OpticalCaptureRunItem


class OpticalWaveformGap(BaseModel):
    kind: str = Field(default="gap", description="Stable marker type for viewer gap metadata.")
    reason: str = Field(description="Why the gap marker was emitted, such as 'time_gap' or 'packet_loss'.")
    start_ts: str | None = Field(default=None, description="ISO 8601 timestamp at the start edge of the gap.")
    end_ts: str | None = Field(default=None, description="ISO 8601 timestamp at the end edge of the gap.")
    gap_ms: float | None = Field(default=None, description="Observed elapsed milliseconds across the gap when known.")
    expected_interval_ms: float | None = Field(
        default=None,
        description="Expected sample/frame interval in milliseconds when the gap was detected from time spacing.",
    )
    chunk_index: int | None = Field(default=None, description="Chunk index that produced the gap marker when applicable.")
    packet_loss_count_delta: int | None = Field(
        default=None,
        description="Packet-loss count attached to this gap marker when the gap was sourced from chunk loss metadata.",
    )


class OpticalWaveformSeries(BaseModel):
    series: str
    kind: str
    label: str | None = None
    unit: str | None = None
    points: list[dict[str, Any]] = Field(default_factory=list)
    gaps: list[OpticalWaveformGap] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


class OpticalWaveformResp(BaseModel):
    measurement_run_id: str
    run: OpticalCaptureRunItem
    filters: dict[str, Any] = Field(default_factory=dict)
    available_series: list[str] = Field(default_factory=list)
    series: list[OpticalWaveformSeries] = Field(default_factory=list)

class ChatReq(BaseModel):
    user_id: str
    message: str
    session_id: str | None = None
    encounter_id: str | None = None

class MobileExchangeReq(BaseModel):
    code: str
    code_verifier: str
    redirect_uri: str | None = None

class AuthSessionResp(BaseModel):
    user_id: str
    role: str
    portal: str
    is_doctor: bool
    default_redirect_url: str
    default_redirect_path: str
    email: str | None = None
    name: str | None = None
    access_token: str | None = None
    refresh_token: str | None = None

class AuthRefreshReq(BaseModel):
    refresh_token: str | None = None

class DoctorPatientAssignReq(BaseModel):
    patient_user_id: str | None = None
    patient_email: str | None = None

class DoctorPatientListItem(BaseModel):
    user_id: str
    email: str | None = None
    name: str | None = None
    role: str
    assigned_at: datetime
    profile: dict[str, Any] = Field(default_factory=dict)
    profile_updated_at: datetime | None = None

class DoctorPatientListResp(BaseModel):
    items: list[DoctorPatientListItem] = Field(default_factory=list)
    next_cursor: str | None = None

class DoctorPatientNoteReq(BaseModel):
    note_text: str
    encounter_id: str | None = None

class DoctorPatientNoteItem(BaseModel):
    note_id: str
    doctor_user_id: str
    patient_user_id: str
    encounter_id: str | None = None
    note_text: str
    created_at: datetime
    updated_at: datetime

class DoctorPatientNoteListResp(BaseModel):
    items: list[DoctorPatientNoteItem] = Field(default_factory=list)

class DoctorEncounterTagReq(BaseModel):
    encounter_id: str
    tag: str

class DoctorEncounterTagItem(BaseModel):
    tag_id: str
    doctor_user_id: str
    patient_user_id: str
    encounter_id: str
    tag: str
    created_at: datetime

class DoctorEncounterTagListResp(BaseModel):
    items: list[DoctorEncounterTagItem] = Field(default_factory=list)

class DoctorDashboardMeta(BaseModel):
    report_limit: int = 0
    reports_returned: int = 0
    reports_truncated: bool = False
    history_limit: int = 0
    measurements_returned: int = 0
    measurements_truncated: bool = False
    trends_series_count: int = 0
    image_limit: int = 0
    image_history_returned: int = 0
    image_history_truncated: bool = False
    note_limit: int = 0
    notes_returned: int = 0
    notes_truncated: bool = False
    tag_limit: int = 0
    encounter_tags_returned: int = 0
    encounter_tags_truncated: bool = False

class DoctorEncounterSummary(BaseModel):
    encounter_id: str
    started_at: str | None = None
    ended_at: str | None = None
    updated_at: datetime | None = None
    revision: int = 0
    etag: str | None = None
    measurement_count: int = 0
    image_count: int = 0
    note_count: int = 0
    tag_count: int = 0
    measurement_ids: list[str] = Field(default_factory=list)
    latest_measurement_id: str | None = None
    latest_measurement_label: str | None = None
    latest_measurement_mode: str | None = None
    latest_source: str | None = None
    device_name: str | None = None
    location_label: str | None = None
    chat_summary: str | None = None
    reason_for_consult: str | None = None
    patient_reported_symptoms: list[dict[str, str]] = Field(default_factory=list)
    clinician_assessment: str | None = None
    disposition: str | None = None
    follow_up_plan: str | None = None
    tags: list[str] = Field(default_factory=list)
    measurement_modes: list[str] = Field(default_factory=list)
    linked_measurement_run_ids: list[str] = Field(default_factory=list)
    waveform_measurement_run_ids: list[str] = Field(default_factory=list)
    has_waveform_measurements: bool = False
    linked_image_ids: list[int] = Field(default_factory=list)
    linked_report_ids: list[str] = Field(default_factory=list)
    linked_message_ids: list[str] = Field(default_factory=list)

class DoctorCareTeamItem(BaseModel):
    doctor_user_id: str
    name: str | None = None
    email: str | None = None
    assigned_at: datetime | None = None

class DoctorChartSummary(BaseModel):
    patient_id: str
    external_patient_id: str | None = None
    full_name: str | None = None
    dob: str | None = None
    age: int | None = None
    sex: str | None = None
    primary_condition: str | None = None
    secondary_conditions: list[str] = Field(default_factory=list)
    allergies: list[str] = Field(default_factory=list)
    current_medications: list[str] = Field(default_factory=list)
    pregnancy_status: str | None = None
    gestational_age_weeks: int | None = None
    risk_flags: list[str] = Field(default_factory=list)
    care_team: list[DoctorCareTeamItem] = Field(default_factory=list)
    preferred_language: str | None = None
    problem_list: list[str] = Field(default_factory=list)
    vaccination_status: str | None = None
    recent_hospitalizations: list[str] = Field(default_factory=list)
    family_history: list[str] = Field(default_factory=list)
    smoking_status: str | None = None
    substance_use: str | None = None
    baseline_notes: str | None = None
    last_patient_activity_at: datetime | None = None
    last_clinician_activity_at: datetime | None = None
    next_follow_up_due_at: datetime | None = None
    chart_banner: dict[str, Any] = Field(default_factory=dict)

class DoctorConsultSummary(BaseModel):
    consult_case_id: str | None = None
    consult_status: str = CONSULT_STATUS_NEW
    consult_status_reason: str | None = None
    priority: str = CONSULT_PRIORITY_ROUTINE
    escalation_status: str = CONSULT_ESCALATION_NONE
    assigned_clinician_id: str | None = None
    assigned_clinician_name: str | None = None
    assigned_clinician_email: str | None = None
    assigned_clinician_title: str | None = None
    assigned_clinician_specialty: str | None = None
    assigned_clinician_avatar_url: str | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    closed_reason: str | None = None
    handoff_requested: bool = False
    handoff_target_clinician_id: str | None = None
    handoff_target_clinician_name: str | None = None
    reopened_at: datetime | None = None
    last_clinician_action_at: datetime | None = None
    last_clinician_action_type: str | None = None
    next_action_due_at: datetime | None = None
    next_follow_up_due_at: datetime | None = None
    last_action_summary: str | None = None
    updated_at: datetime | None = None
    revision: int = 0
    etag: str | None = None

class DoctorTriageSummary(BaseModel):
    attention_score: float = 0.0
    attention_reasons: list[str] = Field(default_factory=list)
    review_recommended_count: int = 0
    abnormal_measurement_count: int = 0
    unread_patient_message_count: int = 0
    overdue_follow_up: bool = False
    last_critical_event_at: datetime | None = None

class DoctorFreshnessSummary(BaseModel):
    updated_at: datetime | None = None
    revision: int = 0
    etag: str | None = None

class DoctorPatientDashboardResp(BaseModel):
    patient: DoctorPatientListItem
    profile: dict[str, Any] = Field(default_factory=dict)
    profile_updated_at: datetime | None = None
    chart_summary: DoctorChartSummary
    consult: DoctorConsultSummary = Field(default_factory=DoctorConsultSummary)
    triage: DoctorTriageSummary = Field(default_factory=DoctorTriageSummary)
    freshness: DoctorFreshnessSummary = Field(default_factory=DoctorFreshnessSummary)
    meta: DoctorDashboardMeta = Field(default_factory=DoctorDashboardMeta)
    reports: list["ReportListItem"] = Field(default_factory=list)
    measurements: "MeasurementHistoryResp" = Field(default_factory=lambda: {"sessions": []})
    trends: "MeasurementTrendsResp" = Field(default_factory=lambda: {"series": []})
    image_history: "ImageAnalysisHistoryResp" = Field(default_factory=lambda: {"items": []})
    encounters: list[DoctorEncounterSummary] = Field(default_factory=list)
    notes: list[DoctorPatientNoteItem] = Field(default_factory=list)
    encounter_tags: list[DoctorEncounterTagItem] = Field(default_factory=list)

class UserProfileUpdateReq(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    sex: str | None = None
    age: int | None = Field(default=None, ge=0, le=130)
    weight_kg: float | None = Field(default=None, gt=0, le=500)
    height_cm: float | None = Field(default=None, gt=0, le=300)
    ambient_temperature_c: float | None = None
    humidity_percent: float | None = Field(default=None, ge=0, le=100)
    co2_ppm: float | None = Field(default=None, ge=0)
    medical_history: str | None = None
    medical_history_file_key: str | None = None

class UserProfileResp(BaseModel):
    user_id: str
    profile: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime | None = None

class ReportGenerateReq(BaseModel):
    limit_sessions: int = Field(default=5, ge=1, le=50)
    limit_events: int = Field(default=200, ge=0, le=2000)
    encounter_id: str | None = None

class ReportRunReq(ReportGenerateReq):
    model: str | None = None

class ReportListItem(BaseModel):
    report_id: str
    created_at: datetime
    model: str
    status: str
    payload_hash: str | None = None
    encounter_id: str | None = None
    consult_case_id: str | None = None

class ReportDetail(ReportListItem):
    report_md: str

class DoctorConsultUpdateReq(BaseModel):
    consult_case_id: str | None = None
    revision: int | None = None
    consult_status: str | None = None
    consult_status_reason: str | None = None
    priority: str | None = None
    escalation_status: str | None = None
    closed_reason: str | None = None
    handoff_requested: bool | None = None
    handoff_target_clinician_id: str | None = None
    next_action_due_at: datetime | None = None
    next_follow_up_due_at: datetime | None = None
    last_action_summary: str | None = None

class DoctorEncounterClinicalUpdateReq(BaseModel):
    revision: int | None = None
    reason_for_consult: str | None = None
    patient_reported_symptoms: list[dict[str, str]] | list[str] | None = None
    clinician_assessment: str | None = None
    disposition: str | None = None
    follow_up_plan: str | None = None

class ConsultCaseMessageReq(BaseModel):
    consult_case_id: str | None = None
    encounter_id: str | None = None
    body: str
    message_type: str = "message"
    requires_acknowledgement: bool = False
    attachments: list[dict[str, Any]] = Field(default_factory=list)

class ConsultCaseMessageItem(BaseModel):
    message_id: str
    consult_case_id: str
    doctor_user_id: str
    patient_user_id: str
    encounter_id: str | None = None
    sender_type: str
    sender_user_id: str | None = None
    sender_name: str | None = None
    message_type: str
    body: str
    requires_acknowledgement: bool = False
    read_at: datetime | None = None
    attachments: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime

class ConsultCaseMessageListResp(BaseModel):
    items: list[ConsultCaseMessageItem] = Field(default_factory=list)
    next_cursor: str | None = None


class ConsultCaseMessageSummaryResp(BaseModel):
    consult_case_id: str
    unread_patient_message_count: int = 0
    requires_acknowledgement_count: int = 0
    last_message_at: datetime | None = None
    last_message_sender_type: str | None = None
    last_message_preview: str | None = None

class DoctorTimelineEvent(BaseModel):
    event_id: str
    event_type: str
    timestamp: datetime
    actor_type: str
    actor_name: str | None = None
    summary: str
    encounter_id: str | None = None
    consult_case_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

class DoctorTimelineResp(BaseModel):
    items: list[DoctorTimelineEvent] = Field(default_factory=list)
    next_cursor: str | None = None

class DoctorQueueItem(BaseModel):
    consult_case_id: str
    patient_id: str
    full_name: str | None = None
    email: str | None = None
    external_patient_id: str | None = None
    consult_status: str
    consult_status_reason: str | None = None
    priority: str
    attention_score: float = 0.0
    attention_reasons: list[str] = Field(default_factory=list)
    review_recommended_count: int = 0
    abnormal_measurement_count: int = 0
    unread_patient_message_count: int = 0
    requires_acknowledgement_count: int = 0
    overdue_follow_up: bool = False
    risk_flags: list[str] = Field(default_factory=list)
    patient_age: int | None = None
    patient_sex: str | None = None
    primary_condition: str | None = None
    last_message_at: datetime | None = None
    last_message_sender_type: str | None = None
    last_message_preview: str | None = None
    last_patient_activity_at: datetime | None = None
    last_clinician_activity_at: datetime | None = None
    last_clinician_action_at: datetime | None = None
    last_clinician_action_type: str | None = None
    last_critical_event_at: datetime | None = None
    case_summary: str | None = None
    next_action_due_at: datetime | None = None
    next_follow_up_due_at: datetime | None = None
    assigned_clinician_id: str | None = None
    assigned_clinician_name: str | None = None
    assigned_clinician_email: str | None = None
    assigned_clinician_title: str | None = None
    assigned_clinician_specialty: str | None = None
    assigned_clinician_avatar_url: str | None = None
    closed_reason: str | None = None
    reopened_at: datetime | None = None
    updated_at: datetime | None = None
    revision: int = 0
    etag: str | None = None

class DoctorQueueResp(BaseModel):
    items: list[DoctorQueueItem] = Field(default_factory=list)
    next_cursor: str | None = None


class DoctorQueueSummaryResp(BaseModel):
    total_open: int = 0
    urgent: int = 0
    critical: int = 0
    unread_messages: int = 0
    overdue_follow_up: int = 0
    awaiting_patient: int = 0
    in_review: int = 0


class DoctorQueueChangesResp(BaseModel):
    updated_consults: list[str] = Field(default_factory=list)
    updated_items: list[DoctorQueueItem] = Field(default_factory=list)
    removed_consults: list[str] = Field(default_factory=list)
    summary: DoctorQueueSummaryResp = Field(default_factory=DoctorQueueSummaryResp)
    new_requests: int = 0
    new_unread_messages: int = 0
    server_time: datetime


class PatientConsultItem(BaseModel):
    consult_case_id: str
    doctor_user_id: str
    doctor_name: str | None = None
    doctor_email: str | None = None
    doctor_title: str | None = None
    doctor_specialty: str | None = None
    doctor_avatar_url: str | None = None
    consult_status: str = CONSULT_STATUS_NEW
    priority: str = CONSULT_PRIORITY_ROUTINE
    escalation_status: str = CONSULT_ESCALATION_NONE
    unread_doctor_message_count: int = 0
    requires_acknowledgement_count: int = 0
    last_message_preview: str | None = None
    last_message_at: datetime | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    next_action_due_at: datetime | None = None
    next_follow_up_due_at: datetime | None = None
    last_action_summary: str | None = None
    updated_at: datetime | None = None
    revision: int = 0
    freshness: DoctorFreshnessSummary = Field(default_factory=DoctorFreshnessSummary)


class PatientDoctorRequestReq(BaseModel):
    request_reason: str | None = None


class PatientDoctorRequestItem(BaseModel):
    request_id: str
    patient_user_id: str
    status: str = PATIENT_DOCTOR_REQUEST_PENDING
    request_reason: str | None = None
    request_priority: str = CONSULT_PRIORITY_ROUTINE
    request_source: str = "patient_portal"
    queue_position: int | None = None
    estimated_wait_minutes: int | None = None
    last_patient_message_at: datetime | None = None
    claimed_by_doctor_user_id: str | None = None
    claimed_by_doctor_name: str | None = None
    consult_case_id: str | None = None
    created_at: datetime
    updated_at: datetime
    claimed_at: datetime | None = None
    cancelled_at: datetime | None = None


class PatientConsultListResp(BaseModel):
    items: list[PatientConsultItem] = Field(default_factory=list)
    pending_request: PatientDoctorRequestItem | None = None


class PatientConsultChangeItem(BaseModel):
    consult_case_id: str
    updated_at: datetime | None = None
    revision: int = 0
    unread_doctor_message_count: int = 0
    requires_acknowledgement_count: int = 0
    last_message_preview: str | None = None
    last_message_at: datetime | None = None


class PatientConsultChangesResp(BaseModel):
    updated_consults: list[PatientConsultChangeItem] = Field(default_factory=list)
    updated_items: list[PatientConsultItem] = Field(default_factory=list)
    removed_consults: list[str] = Field(default_factory=list)
    pending_request_updated: bool = False
    pending_request: PatientDoctorRequestItem | None = None
    unread_doctor_messages: int = 0
    server_time: datetime


class ConsultAttachmentUploadResp(BaseModel):
    attachment_id: str
    consult_case_id: str
    kind: str
    filename: str
    content_type: str
    size_bytes: int
    download_url: str
    preview_available: bool = False
    preview_kind: str | None = None
    preview_content_type: str | None = None
    preview_url: str | None = None
    thumbnail_url: str | None = None
    created_at: datetime


class DoctorNotificationSummaryResp(BaseModel):
    unread_messages: int = 0
    unread_patient_message_count: int = 0
    requires_acknowledgement_count: int = 0
    pending_requests: int = 0
    pending_request_count: int = 0
    updated_consults: int = 0
    active_consult_count: int = 0
    overdue_follow_up_count: int = 0
    urgent_count: int = 0
    critical_count: int = 0
    last_event_at: datetime | None = None
    updated_at: datetime | None = None


class PatientNotificationSummaryResp(BaseModel):
    unread_messages: int = 0
    unread_doctor_message_count: int = 0
    requires_acknowledgement_count: int = 0
    pending_requests: int = 0
    pending_request_count: int = 0
    updated_consults: int = 0
    active_consult_count: int = 0
    last_event_at: datetime | None = None
    updated_at: datetime | None = None


class PatientMessageReadAllResp(BaseModel):
    consult_case_id: str
    read_count: int = 0
    updated_at: datetime


class DoctorPatientRequestItem(BaseModel):
    request_id: str
    patient_user_id: str
    patient_name: str | None = None
    patient_email: str | None = None
    patient_profile: dict[str, Any] = Field(default_factory=dict)
    status: str = PATIENT_DOCTOR_REQUEST_PENDING
    request_reason: str | None = None
    request_priority: str = CONSULT_PRIORITY_ROUTINE
    request_source: str = "patient_portal"
    queue_position: int | None = None
    estimated_wait_minutes: int | None = None
    last_patient_message_at: datetime | None = None
    claimed_by_doctor_user_id: str | None = None
    claimed_by_doctor_name: str | None = None
    consult_case_id: str | None = None
    created_at: datetime
    updated_at: datetime
    claimed_at: datetime | None = None
    cancelled_at: datetime | None = None


class DoctorPatientRequestListResp(BaseModel):
    items: list[DoctorPatientRequestItem] = Field(default_factory=list)
    next_cursor: str | None = None


class DoctorPatientRequestClaimResp(BaseModel):
    request: DoctorPatientRequestItem
    patient: DoctorPatientListItem
    consult: DoctorConsultSummary = Field(default_factory=DoctorConsultSummary)


class MeasurementHistoryMessage(BaseModel):
    role: str
    text: str
    created_at: str

class MeasurementHistoryChatResult(BaseModel):
    assistant_summary: str | None = None
    suggested_next_step: str | None = None
    recommended_sensor: str | None = None
    sensor_confidence: float | None = None
    recommended_measurements: list[str] = Field(default_factory=list)

class MeasurementHistorySummary(BaseModel):
    skin_temperature_c: float | None = None
    ambient_temperature_c: float | None = None
    spo2_percent: float | None = None
    heart_rate_bpm: float | None = None
    humidity_percent: float | None = None
    co2_ppm: float | None = None
    air_quality_index: float | None = None

class MeasurementHistorySession(BaseModel):
    encounter_id: str
    user_id: str
    chat_session_id: str | None = None
    measurement_session_id: str | None = None
    measurement_run_id: str | None = None
    measurement_run_ids: list[str] = Field(default_factory=list)
    measurement_id: str | None = None
    measurement_mode: str | None = None
    measurement_label: str | None = None
    source: str | None = None
    is_user_selected: bool | None = None
    started_at: str | None = None
    ended_at: str | None = None
    device_name: str | None = None
    capture_status: str | None = None
    raw_waveform_available: bool | None = None
    sample_rate_hz: float | None = None
    packet_loss_count: int | None = None
    ts1_frame_count: int | None = None
    total_chunks: int | None = None
    total_packets: int | None = None
    total_samples: int | None = None
    checksum_failure_count: int | None = None
    malformed_packet_count: int | None = None
    requested_measurements: list[str] = Field(default_factory=list)
    waveform_api_path: str | None = None
    waveform_stream_path: str | None = None
    feelings: list[str] = Field(default_factory=list)
    notes: str | None = None
    chat_result: MeasurementHistoryChatResult | None = None
    messages: list[MeasurementHistoryMessage] = Field(default_factory=list)
    summary: MeasurementHistorySummary = Field(default_factory=MeasurementHistorySummary)
    analysis: dict[str, Any] | None = None
    analysis_result: dict[str, Any] | None = None
    sensor_analysis: dict[str, Any] | None = None
    visual_analysis: dict[str, Any] | None = None
    findings: list[str] = Field(default_factory=list)
    location_label: str | None = None
    capture_location: str | None = None

class MeasurementHistoryResp(BaseModel):
    sessions: list[MeasurementHistorySession] = Field(default_factory=list)

class MeasurementTrendNormalRange(BaseModel):
    min: float
    max: float

class MeasurementTrendPoint(BaseModel):
    ts: str
    value: float
    encounter_id: str | None = None

class MeasurementTrendSeries(BaseModel):
    kind: str
    label: str | None = None
    unit: str | None = None
    normal_range: MeasurementTrendNormalRange | None = None
    points: list[MeasurementTrendPoint] = Field(default_factory=list)

class MeasurementTrendsResp(BaseModel):
    series: list[MeasurementTrendSeries] = Field(default_factory=list)

class SensorAnalysisFinding(BaseModel):
    code: str
    severity: str
    category: str
    message: str
    kind: str | None = None

class SensorAnalysisResult(BaseModel):
    # User-facing fields
    patient_summary: str | None = None
    patient_findings: list[str] = Field(default_factory=list)
    confidence_band: str | None = None  # low|moderate|high
    confidence_score: float | None = None
    recommendation: str | None = None

    # Technical/raw fields (for debugging, not shown to user)
    model_public: dict[str, Any] = Field(default_factory=dict)

    # Legacy/compat fields (for API compatibility)
    measurement_run_id: str | None = None
    session_id: str | None = None
    encounter_id: str | None = None
    analyzer: str | None = None
    scope: str | None = None
    status: str | None = None
    score: float | None = None
    confidence: float | None = None
    summary: str | None = None
    event_count: int = 0
    findings: list[SensorAnalysisFinding] = Field(default_factory=list)
    latest_values: dict[str, Any] = Field(default_factory=dict)
    features: dict[str, Any] = Field(default_factory=dict)
    model: dict[str, Any] = Field(default_factory=dict)
    analyzed_at: str | None = None

class MeasurementAnalysisResp(BaseModel):
    analyses: list[SensorAnalysisResult] = Field(default_factory=list)

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


def upsert_user_from_oidc(
    issuer: str,
    sub: str,
    email: str | None,
    name: str | None,
    *,
    role: str | None = None,
    auth_groups: list[str] | None = None,
) -> str:
    normalized_role = _normalized_user_role(role) if role is not None else None
    normalized_auth_groups = list(dict.fromkeys(auth_groups or []))
    sync_auth_claims = normalized_role is not None or bool(normalized_auth_groups)
    auth_groups_json = _json.dumps(normalized_auth_groups)

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
                    name  = COALESCE(%s, name),
                    role = COALESCE(%s, role),
                    auth_groups = CASE
                        WHEN %s THEN %s::jsonb
                        ELSE auth_groups
                    END,
                    last_login_at = now()
                WHERE id = %s
                RETURNING id::text
                """,
                (email, name, normalized_role, sync_auth_claims, auth_groups_json, user_id),
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
                        email       = COALESCE(%s, email),
                        role        = COALESCE(%s, role),
                        auth_groups = CASE
                            WHEN %s THEN %s::jsonb
                            ELSE auth_groups
                        END,
                        last_login_at = now()
                    WHERE id = %s
                    RETURNING id::text
                    """,
                    (issuer, sub, name, email, normalized_role, sync_auth_claims, auth_groups_json, user_id),
                )
                return cur.fetchone()[0]

        # 3) Otherwise insert fresh.
        try:
            cur.execute(
                """
                INSERT INTO users (auth_issuer, auth_sub, email, name, role, auth_groups, last_login_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, now())
                RETURNING id::text
                """,
                (issuer, sub, email, name, normalized_role or USER_ROLE_PATIENT, auth_groups_json),
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
                        name        = COALESCE(%s, name),
                        role        = COALESCE(%s, role),
                        auth_groups = CASE
                            WHEN %s THEN %s::jsonb
                            ELSE auth_groups
                        END,
                        last_login_at = now()
                    WHERE id = %s
                    RETURNING id::text
                    """,
                    (issuer, sub, name, normalized_role, sync_auth_claims, auth_groups_json, user_id),
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
                "scope": AUTH_OIDC_SCOPES,
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
                "scope": AUTH_OIDC_SCOPES,
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


@app.post("/auth/mobile/exchange", response_model=AuthSessionResp, response_model_exclude_none=True)
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
    role, auth_groups = _resolve_user_role_from_claims(info)

    issuer = oidc["issuer"]
    user_id = upsert_user_from_oidc(
        issuer=issuer,
        sub=sub,
        email=email,
        name=name,
        role=role,
        auth_groups=auth_groups,
    )

    # your own app tokens
    refresh_jwt = make_refresh_token(user_id)
    access_jwt = make_access_token(user_id, role=role)

    # Return JSON to the app (store refresh securely in flutter)
    return _build_auth_response_payload(
        user_id,
        access_token=access_jwt,
        refresh_token=refresh_jwt,
    )


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
    role, auth_groups = _resolve_user_role_from_claims(info)

    issuer = oidc["issuer"]
    user_id = upsert_user_from_oidc(
        issuer=issuer,
        sub=sub,
        email=email,
        name=name,
        role=role,
        auth_groups=auth_groups,
    )

    # set refresh cookie for api.tracksante.com
    refresh_jwt = make_refresh_token(user_id)

    # decide where to send the browser next
    if client_kind == "mobile":
        # redirect to the App Link page so Android opens the app
        # (pass code/state through so the app can finish login if you want)
        next_url = f"https://tracksante.com/mobile/callback?{urlencode({'code': code, 'state': state})}"
    else:
        # optional "next" support
        resolved_role = _normalized_user_role(role) if role is not None else _require_user_auth_context(user_id)["role"]
        next_url = request.cookies.get("oidc_next") or _default_redirect_for_role(resolved_role)

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

@app.post("/auth/refresh", response_model=AuthSessionResp, response_model_exclude_none=True)
def auth_refresh(request: Request, req: AuthRefreshReq = Body(default_factory=AuthRefreshReq)):
    token = (req.refresh_token or None) or request.cookies.get(REFRESH_COOKIE)
    if not token:
        raise HTTPException(401, "Missing refresh token")

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
            leeway=JWT_LEEWAY_SECONDS,
        )
    except Exception:
        raise HTTPException(401, "Invalid refresh token")

    if payload.get("typ") != "refresh":
        raise HTTPException(401, "Wrong token type")

    user_id = payload["sub"]
    access = make_access_token(user_id)
    return _build_auth_response_payload(user_id, access_token=access, refresh_token=req.refresh_token or None)


bearer = HTTPBearer(auto_error=False)

def optional_access_user_id(creds: HTTPAuthorizationCredentials = Depends(bearer)) -> str | None:
    if not creds:
        return None

    try:
        payload = jwt.decode(
            creds.credentials,
            JWT_SECRET,
            algorithms=["HS256"],
            audience=JWT_AUDIENCE,
            issuer=JWT_ISSUER,
            leeway=JWT_LEEWAY_SECONDS,
        )
    except Exception as exc:
        logger.info("optional access token rejected: %s", exc)
        raise HTTPException(401, "Invalid token")

    if payload.get("typ") != "access":
        raise HTTPException(401, "Wrong token type")

    return payload["sub"]

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
            leeway=JWT_LEEWAY_SECONDS,
        )
    except Exception as exc:
        logger.info("access token rejected: %s", exc)
        raise HTTPException(401, "Invalid token")

    if payload.get("typ") != "access":
        raise HTTPException(401, "Wrong token type")

    return payload["sub"]


def _fetch_user_role(cur, user_id: str) -> str:
    cur.execute("SELECT role FROM users WHERE id = %s LIMIT 1;", (user_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "User not found")
    return _normalized_user_role(row[0])


def _require_doctor_assignment(cur, doctor_user_id: str, patient_user_id: str) -> None:
    cur.execute(
        """
        SELECT 1
        FROM doctor_patient_assignments
        WHERE doctor_user_id = %s
          AND patient_user_id = %s
        LIMIT 1
        """,
        (doctor_user_id, patient_user_id),
    )
    if not cur.fetchone():
        raise HTTPException(403, "Doctor is not assigned to this patient")


def _authorize_user_read_access(auth_user_id: str, target_user_id: str) -> None:
    if target_user_id == auth_user_id:
        return

    with db() as conn, conn.cursor() as cur:
        if _fetch_user_role(cur, auth_user_id) != USER_ROLE_DOCTOR:
            raise HTTPException(403, "Cannot access another user's records")
        _require_doctor_assignment(cur, auth_user_id, target_user_id)


def require_doctor_user_id(user_id: str = Depends(require_access_user_id)) -> str:
    with db() as conn, conn.cursor() as cur:
        role = _fetch_user_role(cur, user_id)
    if role != USER_ROLE_DOCTOR:
        raise HTTPException(403, "Doctor access required")
    return user_id


def _fetch_profile_payload(user_id: str) -> dict[str, Any]:
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


def _doctor_patient_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "user_id": row[0],
        "email": row[1],
        "name": row[2],
        "role": _normalized_user_role(row[3]),
        "assigned_at": row[4],
        "profile": row[5] or {},
        "profile_updated_at": row[6],
    }


def _patient_doctor_request_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    queue_position = int(row[14] or 0) if len(row) > 14 and row[14] is not None else None
    return {
        "request_id": row[0],
        "patient_user_id": row[1],
        "status": _normalized_patient_doctor_request_status(row[2]),
        "request_reason": row[3],
        "request_priority": _normalized_consult_priority(row[4]) if len(row) > 4 else CONSULT_PRIORITY_ROUTINE,
        "request_source": row[5] if len(row) > 5 and isinstance(row[5], str) and row[5].strip() else "patient_portal",
        "last_patient_message_at": row[6] if len(row) > 6 else None,
        "claimed_by_doctor_user_id": row[7] if len(row) > 7 else None,
        "claimed_by_doctor_name": row[8] if len(row) > 8 else None,
        "consult_case_id": row[9] if len(row) > 9 else None,
        "created_at": row[10] if len(row) > 10 else row[7],
        "updated_at": row[11] if len(row) > 11 else row[8],
        "claimed_at": row[12] if len(row) > 12 else row[9],
        "cancelled_at": row[13] if len(row) > 13 else row[10],
        "queue_position": queue_position,
        "estimated_wait_minutes": _estimate_pending_request_wait_minutes(queue_position),
    }


def _doctor_patient_request_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    queue_position = int(row[17] or 0) if len(row) > 17 and row[17] is not None else None
    return {
        "request_id": row[0],
        "patient_user_id": row[1],
        "patient_name": row[2],
        "patient_email": row[3],
        "patient_profile": row[4] if isinstance(row[4], dict) else {},
        "status": _normalized_patient_doctor_request_status(row[5]),
        "request_reason": row[6],
        "request_priority": _normalized_consult_priority(row[7]) if len(row) > 7 else CONSULT_PRIORITY_ROUTINE,
        "request_source": row[8] if len(row) > 8 and isinstance(row[8], str) and row[8].strip() else "patient_portal",
        "last_patient_message_at": row[9] if len(row) > 9 else None,
        "claimed_by_doctor_user_id": row[10] if len(row) > 10 else None,
        "claimed_by_doctor_name": row[11] if len(row) > 11 else None,
        "consult_case_id": row[12] if len(row) > 12 else None,
        "created_at": row[13] if len(row) > 13 else row[10],
        "updated_at": row[14] if len(row) > 14 else row[11],
        "claimed_at": row[15] if len(row) > 15 else row[12],
        "cancelled_at": row[16] if len(row) > 16 else row[13],
        "queue_position": queue_position,
        "estimated_wait_minutes": _estimate_pending_request_wait_minutes(queue_position),
    }


def _pending_request_queue_position_expr(alias: str = "pdr") -> str:
    return f"""
    CASE
      WHEN {alias}.status = 'pending' THEN (
        SELECT count(*)::int
        FROM patient_doctor_requests q
        WHERE q.status = 'pending'
          AND (
            q.created_at < {alias}.created_at
            OR (q.created_at = {alias}.created_at AND q.id <= {alias}.id)
          )
      )
      ELSE NULL
    END
    """


def _list_doctor_patients(doctor_user_id: str) -> list[dict[str, Any]]:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.id::text,
                   u.email,
                   u.name,
                   u.role,
                   dpa.created_at,
                   up.profile,
                   up.updated_at
            FROM doctor_patient_assignments dpa
            JOIN users u ON u.id = dpa.patient_user_id
            LEFT JOIN user_profile up ON up.user_id = u.id
            WHERE dpa.doctor_user_id = %s
            ORDER BY dpa.created_at DESC, u.created_at DESC
            """,
            (doctor_user_id,),
        )
        rows = cur.fetchall()

    return [_doctor_patient_item_from_row(row) for row in rows]


def _list_patient_assigned_doctor_ids(cur, patient_user_id: str) -> list[str]:
    cur.execute(
        """
        SELECT doctor_user_id::text
        FROM doctor_patient_assignments
        WHERE patient_user_id = %s
        ORDER BY created_at DESC
        """,
        (patient_user_id,),
    )
    return [row[0] for row in cur.fetchall() if isinstance(row[0], str) and row[0]]


def _fetch_open_patient_doctor_request(cur, patient_user_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT pdr.id::text,
               pdr.patient_user_id::text,
               pdr.status,
               pdr.request_reason,
               pdr.request_priority,
               'patient_portal'::text AS request_source,
               COALESCE((
                   SELECT max(m.created_at)
                   FROM consult_case_messages m
                   WHERE m.patient_user_id = pdr.patient_user_id
                     AND m.sender_type = 'patient'
               ), pdr.updated_at, pdr.created_at) AS last_patient_message_at,
               pdr.claimed_by_doctor_user_id::text,
               u.name,
               pdr.consult_case_id::text,
               pdr.created_at,
               pdr.updated_at,
               pdr.claimed_at,
               pdr.cancelled_at,
               """ + _pending_request_queue_position_expr("pdr") + """ AS queue_position
        FROM patient_doctor_requests pdr
        LEFT JOIN users u ON u.id = pdr.claimed_by_doctor_user_id
        WHERE pdr.patient_user_id = %s
          AND pdr.status = %s
        ORDER BY pdr.created_at DESC, pdr.id DESC
        LIMIT 1
        """,
        (patient_user_id, PATIENT_DOCTOR_REQUEST_PENDING),
    )
    row = cur.fetchone()
    return _patient_doctor_request_item_from_row(row) if row else None


def _fetch_latest_patient_doctor_request(cur, patient_user_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT pdr.id::text,
               pdr.patient_user_id::text,
               pdr.status,
               pdr.request_reason,
               pdr.request_priority,
               'patient_portal'::text AS request_source,
               COALESCE((
                   SELECT max(m.created_at)
                   FROM consult_case_messages m
                   WHERE m.patient_user_id = pdr.patient_user_id
                     AND m.sender_type = 'patient'
               ), pdr.updated_at, pdr.created_at) AS last_patient_message_at,
               pdr.claimed_by_doctor_user_id::text,
               u.name,
               pdr.consult_case_id::text,
               pdr.created_at,
               pdr.updated_at,
               pdr.claimed_at,
               pdr.cancelled_at,
               """ + _pending_request_queue_position_expr("pdr") + """ AS queue_position
        FROM patient_doctor_requests pdr
        LEFT JOIN users u ON u.id = pdr.claimed_by_doctor_user_id
        WHERE pdr.patient_user_id = %s
        ORDER BY pdr.updated_at DESC, pdr.created_at DESC, pdr.id DESC
        LIMIT 1
        """,
        (patient_user_id,),
    )
    row = cur.fetchone()
    return _patient_doctor_request_item_from_row(row) if row else None


def _require_assigned_patient_user_id(doctor_user_id: str, patient_user_id: str) -> str:
    try:
        resolved_patient_user_id = str(uuid.UUID(patient_user_id))
    except ValueError as exc:
        raise HTTPException(400, "patient_user_id must be a valid UUID") from exc

    with db() as conn, conn.cursor() as cur:
        if _fetch_user_role(cur, resolved_patient_user_id) != USER_ROLE_PATIENT:
            raise HTTPException(400, "Selected user is not a patient")
        _require_doctor_assignment(cur, doctor_user_id, resolved_patient_user_id)

    return resolved_patient_user_id


def _get_doctor_patient_item(doctor_user_id: str, patient_user_id: str) -> dict[str, Any]:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.id::text,
                   u.email,
                   u.name,
                   u.role,
                   dpa.created_at,
                   up.profile,
                   up.updated_at
            FROM doctor_patient_assignments dpa
            JOIN users u ON u.id = dpa.patient_user_id
            LEFT JOIN user_profile up ON up.user_id = u.id
            WHERE dpa.doctor_user_id = %s
              AND dpa.patient_user_id = %s
            LIMIT 1
            """,
            (doctor_user_id, patient_user_id),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(404, "Doctor-patient assignment not found")
    return _doctor_patient_item_from_row(row)


def _report_list_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "report_id": row[0],
        "created_at": row[1],
        "model": row[2],
        "status": row[3],
        "payload_hash": row[4],
        "encounter_id": row[5] if len(row) > 5 else None,
        "consult_case_id": row[6] if len(row) > 6 else None,
    }


def _list_reports_for_user(user_id: str, limit: int) -> list[dict[str, Any]]:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, created_at, model, status, payload_hash, encounter_id::text, consult_case_id::text
            FROM reports
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (user_id, limit),
        )
        rows = cur.fetchall()
    return [_report_list_item_from_row(row) for row in rows]


def _doctor_patient_note_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "note_id": row[0],
        "doctor_user_id": row[1],
        "patient_user_id": row[2],
        "encounter_id": row[3],
        "note_text": row[4],
        "created_at": row[5],
        "updated_at": row[6],
    }


def _list_doctor_patient_notes(
    doctor_user_id: str,
    patient_user_id: str,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text,
                   doctor_user_id::text,
                   patient_user_id::text,
                   encounter_id::text,
                   note_text,
                   created_at,
                   updated_at
            FROM doctor_patient_notes
            WHERE doctor_user_id = %s
              AND patient_user_id = %s
            ORDER BY updated_at DESC, created_at DESC, id DESC
            LIMIT %s
            """,
            (doctor_user_id, patient_user_id, limit),
        )
        rows = cur.fetchall()

    return [_doctor_patient_note_item_from_row(row) for row in rows]


def _doctor_encounter_tag_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "tag_id": row[0],
        "doctor_user_id": row[1],
        "patient_user_id": row[2],
        "encounter_id": row[3],
        "tag": row[4],
        "created_at": row[5],
    }


def _list_doctor_encounter_tags(
    doctor_user_id: str,
    patient_user_id: str,
    *,
    encounter_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    where = ["doctor_user_id = %s", "patient_user_id = %s"]
    params: list[Any] = [doctor_user_id, patient_user_id]

    if encounter_id is not None:
        where.append("encounter_id = %s::uuid")
        params.append(encounter_id)

    params.append(limit)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT id::text,
                   doctor_user_id::text,
                   patient_user_id::text,
                   encounter_id::text,
                   tag,
                   created_at
            FROM doctor_encounter_tags
            WHERE {' AND '.join(where)}
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

    return [_doctor_encounter_tag_item_from_row(row) for row in rows]


def _consult_case_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "consult_case_id": row[0],
        "doctor_user_id": row[1],
        "patient_user_id": row[2],
        "consult_status": _normalized_consult_status(row[3]),
        "priority": _normalized_consult_priority(row[4]),
        "escalation_status": _normalized_escalation_status(row[5]),
        "opened_at": row[6],
        "closed_at": row[7],
        "consult_status_reason": row[8],
        "next_action_due_at": row[9],
        "next_follow_up_due_at": row[10],
        "last_action_summary": row[11],
        "attention_score": float(row[12] or 0.0),
        "attention_reasons": _normalize_string_list(row[13]),
        "last_patient_activity_at": row[14],
        "last_clinician_activity_at": row[15],
        "last_critical_event_at": row[16],
        "updated_at": row[17],
        "revision": int(row[18] or 0),
        "closed_reason": row[19] if len(row) > 19 else None,
        "handoff_requested": bool(row[20]) if len(row) > 20 else False,
        "handoff_target_clinician_id": row[21] if len(row) > 21 else None,
        "reopened_at": row[22] if len(row) > 22 else None,
        "last_clinician_action_at": row[23] if len(row) > 23 else None,
        "last_clinician_action_type": row[24] if len(row) > 24 else None,
        "assigned_clinician_name": row[25] if len(row) > 25 else None,
        "assigned_clinician_email": row[26] if len(row) > 26 else None,
        "handoff_target_clinician_name": row[27] if len(row) > 27 else None,
        "assigned_clinician_profile": row[28] if len(row) > 28 and isinstance(row[28], dict) else {},
    }


def _select_consult_case_row(
    cur,
    *,
    doctor_user_id: str | None = None,
    patient_user_id: str | None = None,
    consult_case_id: str | None = None,
    open_only: bool = False,
) -> dict[str, Any] | None:
    where: list[str] = []
    params: list[Any] = []
    if consult_case_id is not None:
        where.append("cc.id = %s")
        params.append(consult_case_id)
    if doctor_user_id is not None:
        where.append("cc.doctor_user_id = %s")
        params.append(doctor_user_id)
    if patient_user_id is not None:
        where.append("cc.patient_user_id = %s")
        params.append(patient_user_id)
    if open_only:
        where.append("cc.closed_at IS NULL")

    if not where:
        raise ValueError("consult case selector requires at least one filter")

    cur.execute(
        f"""
        SELECT cc.id::text,
               cc.doctor_user_id::text,
               cc.patient_user_id::text,
               cc.consult_status,
               cc.priority,
               cc.escalation_status,
               cc.opened_at,
               cc.closed_at,
               cc.consult_status_reason,
               cc.next_action_due_at,
               cc.next_follow_up_due_at,
               cc.last_action_summary,
               cc.attention_score,
               cc.attention_reasons,
               cc.last_patient_activity_at,
               cc.last_clinician_activity_at,
               cc.last_critical_event_at,
               cc.updated_at,
               cc.revision,
               cc.closed_reason,
               cc.handoff_requested,
               cc.handoff_target_clinician_id::text,
               cc.reopened_at,
               cc.last_clinician_action_at,
               cc.last_clinician_action_type,
               u.name,
               u.email,
               tu.name,
               dup.profile
        FROM consult_cases cc
        JOIN users u ON u.id = cc.doctor_user_id
        LEFT JOIN users tu ON tu.id = cc.handoff_target_clinician_id
        LEFT JOIN user_profile dup ON dup.user_id = cc.doctor_user_id
        WHERE {' AND '.join(where)}
        ORDER BY cc.closed_at NULLS FIRST, cc.updated_at DESC, cc.opened_at DESC
        LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    return _consult_case_from_row(row) if row else None


def _insert_consult_case_event(
    cur,
    *,
    consult_case_id: str,
    doctor_user_id: str,
    patient_user_id: str,
    event_type: str,
    summary: str,
    encounter_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO consult_case_events (
            consult_case_id,
            doctor_user_id,
            patient_user_id,
            event_type,
            summary,
            encounter_id,
            metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            consult_case_id,
            doctor_user_id,
            patient_user_id,
            event_type,
            summary,
            encounter_id,
            psyjson.Json(metadata or {}),
        ),
    )


def _ensure_consult_case(
    cur,
    *,
    doctor_user_id: str,
    patient_user_id: str,
    consult_case_id: str | None = None,
    create_if_missing: bool = True,
) -> dict[str, Any]:
    resolved_case_id = _validated_uuid_str(consult_case_id, "consult_case_id") if consult_case_id else None
    case = _select_consult_case_row(
        cur,
        doctor_user_id=doctor_user_id,
        patient_user_id=patient_user_id,
        consult_case_id=resolved_case_id,
        open_only=resolved_case_id is None,
    )
    if case is not None:
        return case
    if resolved_case_id is not None:
        raise HTTPException(404, "Consult case not found")
    if not create_if_missing:
        raise HTTPException(404, "Consult case not found")

    cur.execute("SAVEPOINT consult_case_create")
    try:
        cur.execute(
            """
            INSERT INTO consult_cases (
                doctor_user_id,
                patient_user_id,
                consult_status,
                priority,
                escalation_status,
                opened_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, now(), now())
            RETURNING id::text
            """,
            (
                doctor_user_id,
                patient_user_id,
                CONSULT_STATUS_NEW,
                CONSULT_PRIORITY_ROUTINE,
                CONSULT_ESCALATION_NONE,
            ),
        )
        new_case_id = cur.fetchone()[0]
        _insert_consult_case_event(
            cur,
            consult_case_id=new_case_id,
            doctor_user_id=doctor_user_id,
            patient_user_id=patient_user_id,
            event_type="consult_created",
            summary="Case opened.",
            metadata={"previous_status": None, "current_status": CONSULT_STATUS_NEW},
        )
        cur.execute("RELEASE SAVEPOINT consult_case_create")
    except UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT consult_case_create")
        cur.execute("RELEASE SAVEPOINT consult_case_create")

    case = _select_consult_case_row(
        cur,
        doctor_user_id=doctor_user_id,
        patient_user_id=patient_user_id,
        open_only=True,
    )
    if case is None:
        raise HTTPException(500, "Unable to resolve consult case")
    return case


def _close_consult_case_for_doctor_removal(
    cur,
    *,
    doctor_user_id: str,
    patient_user_id: str,
) -> dict[str, Any] | None:
    closed_reason = "Doctor removed patient assignment."
    cur.execute(
        """
        UPDATE consult_cases
        SET consult_status = %s,
            consult_status_reason = %s,
            closed_reason = %s,
            closed_at = COALESCE(closed_at, now()),
            next_action_due_at = NULL,
            next_follow_up_due_at = NULL,
            handoff_requested = FALSE,
            handoff_target_clinician_id = NULL,
            last_action_summary = %s,
            last_clinician_action_at = now(),
            last_clinician_action_type = 'consult_closed',
            updated_at = now(),
            revision = revision + 1
        WHERE doctor_user_id = %s
          AND patient_user_id = %s
          AND closed_at IS NULL
        RETURNING id::text,
                  doctor_user_id::text,
                  patient_user_id::text,
                  consult_status,
                  priority,
                  escalation_status,
                  opened_at,
                  closed_at,
                  consult_status_reason,
                  next_action_due_at,
                  next_follow_up_due_at,
                  last_action_summary,
                  attention_score,
                  attention_reasons,
                  last_patient_activity_at,
                  last_clinician_activity_at,
                  last_critical_event_at,
                  updated_at,
                  revision,
                  closed_reason,
                  handoff_requested,
                  handoff_target_clinician_id::text,
                  reopened_at,
                  last_clinician_action_at,
                  last_clinician_action_type,
                  (SELECT name FROM users WHERE id = consult_cases.doctor_user_id),
                  (SELECT email FROM users WHERE id = consult_cases.doctor_user_id),
                  (SELECT name FROM users WHERE id = consult_cases.handoff_target_clinician_id),
                  (SELECT profile FROM user_profile WHERE user_id = consult_cases.doctor_user_id)
        """,
        (
            CONSULT_STATUS_CLOSED,
            closed_reason,
            closed_reason,
            closed_reason,
            doctor_user_id,
            patient_user_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        return None

    consult_case = _consult_case_from_row(row)
    _insert_consult_case_event(
        cur,
        consult_case_id=consult_case["consult_case_id"],
        doctor_user_id=doctor_user_id,
        patient_user_id=patient_user_id,
        event_type="case_closed",
        summary="Case closed after doctor removed patient assignment.",
        metadata={
            "current_status": CONSULT_STATUS_CLOSED,
            "closed_reason": closed_reason,
            "close_source": "doctor_patient_removal",
        },
    )
    return consult_case


def _resolve_patient_consult_case(
    cur,
    *,
    patient_user_id: str,
    consult_case_id: str | None = None,
    create_if_missing: bool = False,
) -> dict[str, Any]:
    resolved_case_id = _validated_uuid_str(consult_case_id, "consult_case_id") if consult_case_id else None
    if resolved_case_id:
        case = _select_consult_case_row(cur, patient_user_id=patient_user_id, consult_case_id=resolved_case_id)
        if case is None:
            raise HTTPException(404, "Consult case not found")
        return case

    rows = _list_patient_consults(
        cur,
        patient_user_id=patient_user_id,
        include_closed=False,
        ensure_assignment_cases=create_if_missing,
    )
    if len(rows) == 1:
        return rows[0]
    if len(rows) > 1:
        raise HTTPException(400, "Multiple active consult cases; consult_case_id is required")
    if not create_if_missing:
        raise HTTPException(404, "Consult case not found")

    doctor_rows = _list_patient_assigned_doctor_ids(cur, patient_user_id)
    if not doctor_rows:
        raise HTTPException(404, "No doctor assigned")
    if len(doctor_rows) != 1:
        raise HTTPException(400, "Multiple doctor assignments; consult_case_id is required")
    return _ensure_consult_case(
        cur,
        doctor_user_id=doctor_rows[0],
        patient_user_id=patient_user_id,
        create_if_missing=True,
    )


def _resolve_consult_case_participant(
    cur,
    *,
    consult_case_id: str,
    auth_user_id: str,
    allow_assigned_doctor_read: bool = True,
) -> tuple[dict[str, Any], str]:
    resolved_case_id = _validated_uuid_str(consult_case_id, "consult_case_id")
    case = _select_consult_case_row(cur, consult_case_id=resolved_case_id)
    if case is None:
        raise HTTPException(404, "Consult case not found")

    if auth_user_id == case["patient_user_id"]:
        return case, USER_ROLE_PATIENT
    if auth_user_id == case["doctor_user_id"]:
        return case, USER_ROLE_DOCTOR

    role = _fetch_user_role(cur, auth_user_id)
    if allow_assigned_doctor_read and role == USER_ROLE_DOCTOR:
        _require_doctor_assignment(cur, auth_user_id, case["patient_user_id"])
        return case, USER_ROLE_DOCTOR

    raise HTTPException(403, "Not authorized for this consult case")


def _max_dt(*values: Any) -> datetime | None:
    normalized: list[datetime] = []
    for value in values:
        if isinstance(value, datetime):
            normalized.append(value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc))
    return max(normalized) if normalized else None


def _compute_consult_etag(consult_case_id: str | None, revision: int, updated_at: datetime | None) -> str | None:
    if not consult_case_id:
        return None
    payload = f"{consult_case_id}:{revision}:{_iso_utc(updated_at) or ''}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def _compute_encounter_etag(encounter_id: str | None, revision: int, updated_at: datetime | None) -> str | None:
    if not encounter_id:
        return None
    payload = f"{encounter_id}:{revision}:{_iso_utc(updated_at) or ''}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def _revision_conflict_response(message: str, *, current_revision: int, current_etag: str | None = None) -> JSONResponse:
    content: dict[str, Any] = {
        "error": "revision_conflict",
        "message": message,
        "current_revision": current_revision,
    }
    if current_etag:
        content["current_etag"] = current_etag
    return JSONResponse(status_code=409, content=content)


def _consult_attachment_preview_url(attachment_id: str) -> str:
    return f"/api/consult-attachments/{attachment_id}/preview"


def _attachment_preview_kind(*, kind: str | None, content_type: str | None, filename: str | None) -> str | None:
    normalized_kind = str(kind or "").strip().lower()
    normalized_type = str(content_type or "").strip().lower()
    suffix = os.path.splitext(str(filename or "").strip().lower())[-1]
    if normalized_kind == "image" or normalized_type.startswith("image/"):
        return "image"
    if normalized_type.startswith("text/") or suffix in {".txt", ".md", ".json", ".csv", ".log"}:
        return "text"
    return None


def _consult_attachment_preview_meta(
    *,
    attachment_id: str,
    kind: str | None,
    content_type: str | None,
    filename: str | None,
) -> dict[str, Any]:
    preview_kind = _attachment_preview_kind(kind=kind, content_type=content_type, filename=filename)
    preview_url = _consult_attachment_preview_url(attachment_id) if preview_kind else None
    preview_content_type = "image/jpeg" if preview_kind == "image" else "text/plain; charset=utf-8" if preview_kind == "text" else None
    return {
        "preview_available": bool(preview_kind),
        "preview_kind": preview_kind,
        "preview_content_type": preview_content_type,
        "preview_url": preview_url,
        "thumbnail_url": preview_url if preview_kind == "image" else None,
    }


def _build_image_preview_bytes(image_bytes: bytes, max_side: int = CONSULT_ATTACHMENT_PREVIEW_MAX_SIDE) -> bytes:
    with Image.open(io.BytesIO(image_bytes)) as raw:
        image = ImageOps.exif_transpose(raw).convert("RGB")
        width, height = image.size
        if max(width, height) > max_side:
            ratio = max_side / float(max(width, height))
            resized = (max(1, int(round(width * ratio))), max(1, int(round(height * ratio))))
            resampling = getattr(Image, "Resampling", Image)
            image = image.resize(resized, resample=resampling.LANCZOS)
        output = io.BytesIO()
        image.save(output, format="JPEG", quality=82, optimize=True)
        return output.getvalue()


def _build_attachment_text_preview_bytes(content_bytes: bytes, max_chars: int = CONSULT_ATTACHMENT_PREVIEW_TEXT_MAX_CHARS) -> bytes:
    decoded = content_bytes.decode("utf-8", errors="replace").strip()
    if len(decoded) > max_chars:
        decoded = decoded[: max_chars - 1].rstrip() + "…"
    return decoded.encode("utf-8")


def _summarize_message_preview(body: Any, attachments: Any = None) -> str | None:
    if isinstance(body, str) and body.strip():
        return body.strip()[:160]
    attachment_count = len(attachments) if isinstance(attachments, list) else 0
    if attachment_count:
        return f"Sent {attachment_count} attachment(s)."
    return None


def _consult_runtime_due_fields(
    consult_case: dict[str, Any],
    *,
    triage: dict[str, Any] | None = None,
    message_summary: dict[str, Any] | None = None,
) -> dict[str, datetime | None]:
    if consult_case.get("closed_at") is not None:
        return {
            "next_action_due_at": consult_case.get("next_action_due_at"),
            "next_follow_up_due_at": consult_case.get("next_follow_up_due_at"),
        }

    triage_data = triage if isinstance(triage, dict) else {}
    message_data = message_summary if isinstance(message_summary, dict) else {}
    last_message_at = message_data.get("last_message_at")
    last_patient_activity_at = triage_data.get("last_patient_activity_at") or consult_case.get("last_patient_activity_at")
    last_clinician_activity_at = triage_data.get("last_clinician_activity_at") or consult_case.get("last_clinician_activity_at")
    updated_at = consult_case.get("updated_at")
    opened_at = consult_case.get("opened_at")
    base_dt = _max_dt(last_message_at, last_patient_activity_at, last_clinician_activity_at, updated_at, opened_at) or _utcnow()

    next_action_due_at = consult_case.get("next_action_due_at")
    next_follow_up_due_at = consult_case.get("next_follow_up_due_at")
    status = _normalized_consult_status(consult_case.get("consult_status"))
    priority = _normalized_consult_priority(consult_case.get("priority"))

    if next_action_due_at is None:
        unread_patient_messages = int(
            message_data.get("unread_patient_message_count")
            or triage_data.get("unread_patient_message_count")
            or 0
        )
        if unread_patient_messages:
            next_action_due_at = last_message_at or last_patient_activity_at or base_dt
        elif priority == CONSULT_PRIORITY_CRITICAL:
            next_action_due_at = base_dt
        elif priority == CONSULT_PRIORITY_URGENT:
            next_action_due_at = base_dt + timedelta(hours=12)
        elif status == CONSULT_STATUS_NEW:
            next_action_due_at = (opened_at or base_dt) + timedelta(hours=24)
        elif status == CONSULT_STATUS_IN_REVIEW:
            next_action_due_at = base_dt + timedelta(hours=24)

    if next_follow_up_due_at is None:
        requires_acknowledgement_count = int(message_data.get("requires_acknowledgement_count") or 0)
        if status == CONSULT_STATUS_FOLLOW_UP_DUE:
            next_follow_up_due_at = base_dt + timedelta(days=2)
        elif requires_acknowledgement_count:
            next_follow_up_due_at = (last_message_at or base_dt) + timedelta(days=1)

    return {
        "next_action_due_at": next_action_due_at,
        "next_follow_up_due_at": next_follow_up_due_at,
    }


def _consult_clinician_fields(consult_case: dict[str, Any]) -> dict[str, Any]:
    profile_fields = _doctor_profile_summary_fields(consult_case.get("assigned_clinician_profile"))
    return {
        "assigned_clinician_id": consult_case.get("doctor_user_id"),
        "assigned_clinician_name": consult_case.get("assigned_clinician_name"),
        "assigned_clinician_email": consult_case.get("assigned_clinician_email"),
        "assigned_clinician_title": profile_fields.get("doctor_title"),
        "assigned_clinician_specialty": profile_fields.get("doctor_specialty"),
        "assigned_clinician_avatar_url": profile_fields.get("doctor_avatar_url"),
    }


def _list_care_team(cur, patient_user_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT dpa.doctor_user_id::text,
               u.name,
               u.email,
               dpa.created_at
        FROM doctor_patient_assignments dpa
        JOIN users u ON u.id = dpa.doctor_user_id
        WHERE dpa.patient_user_id = %s
        ORDER BY dpa.created_at DESC, u.created_at DESC
        """,
        (patient_user_id,),
    )
    return [
        {
            "doctor_user_id": row[0],
            "name": row[1],
            "email": row[2],
            "assigned_at": row[3],
        }
        for row in cur.fetchall()
    ]


def _patient_report_count_expr() -> str:
    return """
    (
        COALESCE(ia.user_id, us.user_id, d.user_id) = %s::uuid
        OR (
            COALESCE(ia.user_id, us.user_id, d.user_id) IS NULL
            AND ia.encounter_id IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM user_sessions ux
                WHERE ux.user_id = %s::uuid
                  AND ux.encounter_id = ia.encounter_id
            )
        )
    )
    """


def _compute_consult_metrics(cur, *, doctor_user_id: str, patient_user_id: str, consult_case: dict[str, Any]) -> dict[str, Any]:
    consult_case_id = consult_case["consult_case_id"]

    cur.execute(
        """
        SELECT GREATEST(
            COALESCE((SELECT max(COALESCE(ended_at, started_at)) FROM user_sessions WHERE user_id = %s), '-infinity'::timestamptz),
            COALESCE((SELECT max(created_at) FROM reports WHERE user_id = %s), '-infinity'::timestamptz),
            COALESCE((
                SELECT max(ia.analyzed_at)
                FROM image_analyses ia
                JOIN images src ON src.id = ia.source_image_id
                LEFT JOIN user_sessions us ON us.id = COALESCE(ia.session_id, src.session_id)
                LEFT JOIN devices d ON d.id = COALESCE(ia.device_id, src.device_id)
                WHERE """ + _patient_report_count_expr() + """
            ), '-infinity'::timestamptz),
            COALESCE((SELECT max(created_at) FROM consult_case_messages WHERE consult_case_id = %s AND sender_type = 'patient'), '-infinity'::timestamptz)
        ),
        GREATEST(
            COALESCE((SELECT max(updated_at) FROM doctor_patient_notes WHERE doctor_user_id = %s AND patient_user_id = %s), '-infinity'::timestamptz),
            COALESCE((SELECT max(created_at) FROM doctor_encounter_tags WHERE doctor_user_id = %s AND patient_user_id = %s), '-infinity'::timestamptz),
            COALESCE((SELECT max(created_at) FROM consult_case_messages WHERE consult_case_id = %s AND sender_type = 'doctor'), '-infinity'::timestamptz),
            COALESCE((SELECT max(created_at) FROM consult_case_events WHERE consult_case_id = %s), '-infinity'::timestamptz),
            COALESCE((SELECT max(updated_at) FROM encounter_clinical_summaries WHERE doctor_user_id = %s AND patient_user_id = %s), '-infinity'::timestamptz)
        ),
        COALESCE((SELECT count(*) FROM consult_case_messages WHERE consult_case_id = %s AND sender_type = 'patient' AND read_at IS NULL), 0),
        COALESCE((
            SELECT count(*)
            FROM image_analyses ia
            JOIN images src ON src.id = ia.source_image_id
            LEFT JOIN user_sessions us ON us.id = COALESCE(ia.session_id, src.session_id)
            LEFT JOIN devices d ON d.id = COALESCE(ia.device_id, src.device_id)
            WHERE """ + _patient_report_count_expr() + """
              AND lower(coalesce(ia.status, '')) = 'review_recommended'
        ), 0),
        COALESCE((
            SELECT count(*)
            FROM sensor_analysis sa
            LEFT JOIN user_sessions us ON us.id = sa.session_id
            WHERE (
                us.user_id = %s::uuid
                OR (
                    sa.encounter_id IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM user_sessions ux
                        WHERE ux.user_id = %s::uuid
                          AND ux.encounter_id = sa.encounter_id
                    )
                )
            )
              AND lower(coalesce(sa.status, '')) = 'unusual'
        ), 0),
        COALESCE((SELECT max(last_critical_event_at) FROM consult_cases WHERE id = %s), NULL)
        """,
        (
            patient_user_id,
            patient_user_id,
            patient_user_id,
            patient_user_id,
            consult_case_id,
            doctor_user_id,
            patient_user_id,
            doctor_user_id,
            patient_user_id,
            consult_case_id,
            consult_case_id,
            doctor_user_id,
            patient_user_id,
            consult_case_id,
            patient_user_id,
            patient_user_id,
            patient_user_id,
            patient_user_id,
            consult_case_id,
        ),
    )
    row = cur.fetchone()
    last_patient_activity_at = row[0]
    last_clinician_activity_at = row[1]
    unread_patient_message_count = int(row[2] or 0)
    review_recommended_count = int(row[3] or 0)
    abnormal_measurement_count = int(row[4] or 0)
    last_critical_event_at = row[5]

    overdue_follow_up = bool(
        consult_case.get("next_follow_up_due_at")
        and consult_case.get("closed_at") is None
        and isinstance(consult_case["next_follow_up_due_at"], datetime)
        and consult_case["next_follow_up_due_at"] < _utcnow()
    )

    attention_score = 0.0
    attention_reasons: list[str] = []
    priority = _normalized_consult_priority(consult_case.get("priority"))
    if priority == CONSULT_PRIORITY_CRITICAL:
        attention_score += 80
        attention_reasons.append("Critical priority")
    elif priority == CONSULT_PRIORITY_URGENT:
        attention_score += 40
        attention_reasons.append("Urgent priority")

    escalation_status = _normalized_escalation_status(consult_case.get("escalation_status"))
    if escalation_status == CONSULT_ESCALATION_ESCALATED:
        attention_score += 30
        attention_reasons.append("Escalated case")
        last_critical_event_at = _max_dt(last_critical_event_at, consult_case.get("updated_at"))

    if overdue_follow_up:
        attention_score += 30
        attention_reasons.append("Follow-up overdue")

    if unread_patient_message_count:
        attention_score += min(unread_patient_message_count, 5) * 5
        attention_reasons.append(f"{unread_patient_message_count} unread patient message(s)")

    if review_recommended_count:
        attention_score += min(review_recommended_count, 3) * 10
        attention_reasons.append(f"{review_recommended_count} image review recommendation(s)")

    if abnormal_measurement_count:
        attention_score += min(abnormal_measurement_count, 4) * 5
        attention_reasons.append(f"{abnormal_measurement_count} unusual measurement analysis result(s)")

    cur.execute(
        """
        UPDATE consult_cases
        SET attention_score = %s,
            attention_reasons = %s,
            last_patient_activity_at = %s,
            last_clinician_activity_at = %s,
            last_critical_event_at = %s
        WHERE id = %s
        """,
        (
            attention_score,
            psyjson.Json(attention_reasons),
            last_patient_activity_at,
            last_clinician_activity_at,
            last_critical_event_at,
            consult_case_id,
        ),
    )
    consult_case["attention_score"] = attention_score
    consult_case["attention_reasons"] = attention_reasons
    consult_case["last_patient_activity_at"] = last_patient_activity_at
    consult_case["last_clinician_activity_at"] = last_clinician_activity_at
    consult_case["last_critical_event_at"] = last_critical_event_at
    return {
        "attention_score": attention_score,
        "attention_reasons": attention_reasons,
        "review_recommended_count": review_recommended_count,
        "abnormal_measurement_count": abnormal_measurement_count,
        "unread_patient_message_count": unread_patient_message_count,
        "overdue_follow_up": overdue_follow_up,
        "last_critical_event_at": last_critical_event_at,
        "last_patient_activity_at": last_patient_activity_at,
        "last_clinician_activity_at": last_clinician_activity_at,
    }


def _build_chart_summary(
    cur,
    *,
    patient_user_id: str,
    consult_case: dict[str, Any],
    triage: dict[str, Any],
) -> dict[str, Any]:
    due_fields = _consult_runtime_due_fields(consult_case, triage=triage)
    cur.execute(
        """
        SELECT u.id::text,
               u.email,
               u.name,
               up.profile,
               um.memory
        FROM users u
        LEFT JOIN user_profile up ON up.user_id = u.id
        LEFT JOIN user_memory um ON um.user_id = u.id
        WHERE u.id = %s
        LIMIT 1
        """,
        (patient_user_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Patient not found")

    profile_data = _normalize_profile_map(row[3])
    memory_data = row[4] if isinstance(row[4], dict) else {}
    first_name = _coalesce_nonempty(profile_data.get("first_name"), memory_data.get("first_name"))
    last_name = _coalesce_nonempty(profile_data.get("last_name"), memory_data.get("last_name"))
    full_name = " ".join([part for part in [first_name, last_name] if isinstance(part, str) and part.strip()]).strip() or row[2]
    dob = _coalesce_nonempty(profile_data.get("dob"), memory_data.get("dob"))
    age = _coalesce_nonempty(profile_data.get("age"), memory_data.get("age"), _compute_age_from_dob(dob))
    gestational_age = _coalesce_nonempty(profile_data.get("gestational_age_weeks"), memory_data.get("gestational_age_weeks"))
    try:
        gestational_age_value = int(float(gestational_age)) if gestational_age is not None else None
    except (TypeError, ValueError):
        gestational_age_value = None
    try:
        age_value = int(float(age)) if age is not None else None
    except (TypeError, ValueError):
        age_value = None
    allergies = _normalize_string_list(_coalesce_nonempty(profile_data.get("allergies"), memory_data.get("allergies")))
    risk_flags = _normalize_string_list(_coalesce_nonempty(profile_data.get("risk_flags"), memory_data.get("risk_flags")))
    pregnancy_status = _coalesce_nonempty(profile_data.get("pregnancy_status"), memory_data.get("pregnancy_status"))
    critical_alerts = list(allergies[:])
    for flag in risk_flags:
        token = flag.strip().lower()
        if token in {"critical", "high_risk", "fall_risk", "requires_isolation"} and flag not in critical_alerts:
            critical_alerts.append(flag)

    return {
        "patient_id": row[0],
        "external_patient_id": _coalesce_nonempty(profile_data.get("external_patient_id"), memory_data.get("external_patient_id")),
        "full_name": full_name,
        "dob": dob if isinstance(dob, str) else None,
        "age": age_value,
        "sex": _coalesce_nonempty(profile_data.get("sex"), memory_data.get("sex")),
        "primary_condition": _coalesce_nonempty(profile_data.get("primary_condition"), memory_data.get("primary_condition")),
        "secondary_conditions": _normalize_string_list(_coalesce_nonempty(profile_data.get("secondary_conditions"), memory_data.get("secondary_conditions"))),
        "allergies": allergies,
        "current_medications": _normalize_string_list(_coalesce_nonempty(profile_data.get("medications"), memory_data.get("medications"))),
        "pregnancy_status": pregnancy_status,
        "gestational_age_weeks": gestational_age_value,
        "risk_flags": risk_flags,
        "care_team": _list_care_team(cur, patient_user_id),
        "preferred_language": _coalesce_nonempty(profile_data.get("preferred_language"), memory_data.get("preferred_language")),
        "problem_list": _normalize_string_list(_coalesce_nonempty(profile_data.get("problem_list"), memory_data.get("problem_list"))),
        "vaccination_status": _coalesce_nonempty(profile_data.get("vaccination_status"), memory_data.get("vaccination_status")),
        "recent_hospitalizations": _normalize_string_list(_coalesce_nonempty(profile_data.get("recent_hospitalizations"), memory_data.get("recent_hospitalizations"))),
        "family_history": _normalize_string_list(_coalesce_nonempty(profile_data.get("family_history"), memory_data.get("family_history"))),
        "smoking_status": _coalesce_nonempty(profile_data.get("smoking_status"), memory_data.get("smoking_status")),
        "substance_use": _coalesce_nonempty(profile_data.get("substance_use"), memory_data.get("substance_use")),
        "baseline_notes": _coalesce_nonempty(
            profile_data.get("baseline_notes"),
            memory_data.get("baseline_notes"),
            profile_data.get("medical_history"),
            memory_data.get("medical_history"),
        ),
        "last_patient_activity_at": triage.get("last_patient_activity_at"),
        "last_clinician_activity_at": triage.get("last_clinician_activity_at"),
        "next_follow_up_due_at": due_fields.get("next_follow_up_due_at"),
        "chart_banner": {
            "critical_alerts": critical_alerts,
            "requires_isolation": bool(_as_bool(_coalesce_nonempty(profile_data.get("requires_isolation"), memory_data.get("requires_isolation")))),
            "fall_risk": bool(_as_bool(_coalesce_nonempty(profile_data.get("fall_risk"), memory_data.get("fall_risk")))),
            "pregnancy_context": (
                f"{pregnancy_status} ({gestational_age_value} weeks)"
                if pregnancy_status and gestational_age_value is not None
                else pregnancy_status
            ),
        },
    }


def _fetch_encounter_clinical_summaries(cur, *, patient_user_id: str) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT encounter_id::text,
               reason_for_consult,
               patient_reported_symptoms,
               clinician_assessment,
               disposition,
               follow_up_plan,
               updated_at,
               revision
        FROM encounter_clinical_summaries
        WHERE patient_user_id = %s
        """,
        (patient_user_id,),
    )
    result: dict[str, dict[str, Any]] = {}
    for row in cur.fetchall():
        result[row[0]] = {
            "reason_for_consult": row[1],
            "patient_reported_symptoms": _normalize_symptom_list(row[2]),
            "clinician_assessment": row[3],
            "disposition": row[4],
            "follow_up_plan": row[5],
            "updated_at": row[6],
            "revision": int(row[7] or 0),
            "etag": _compute_encounter_etag(row[0], int(row[7] or 0), row[6]),
        }
    return result


def _consult_message_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    attachments = row[12] if isinstance(row[12], list) else []
    normalized_attachments: list[dict[str, Any]] = []
    for attachment in attachments:
        if not isinstance(attachment, dict):
            continue
        attachment_id = str(attachment.get("attachment_id") or attachment.get("id") or "").strip()
        if attachment_id:
            normalized_attachments.append(
                {
                    **attachment,
                    **_consult_attachment_preview_meta(
                        attachment_id=attachment_id,
                        kind=attachment.get("kind"),
                        content_type=attachment.get("content_type"),
                        filename=attachment.get("filename"),
                    ),
                }
            )
        else:
            normalized_attachments.append(dict(attachment))
    return {
        "message_id": row[0],
        "consult_case_id": row[1],
        "doctor_user_id": row[2],
        "patient_user_id": row[3],
        "encounter_id": row[4],
        "sender_type": _normalized_sender_type(row[5]),
        "sender_user_id": row[6],
        "sender_name": row[7],
        "message_type": _normalized_message_type(row[8]),
        "body": row[9],
        "requires_acknowledgement": bool(row[10]),
        "read_at": row[11],
        "attachments": normalized_attachments,
        "created_at": row[13],
    }


def _consult_attachment_download_url(attachment_id: str) -> str:
    return f"/api/consult-attachments/{attachment_id}/content"


def _consult_attachment_item_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    attachment_id = row[0]
    kind = row[2] if isinstance(row[2], str) and row[2].strip() else "file"
    filename = row[3]
    content_type = row[4]
    preview_meta = _consult_attachment_preview_meta(
        attachment_id=attachment_id,
        kind=kind,
        content_type=content_type,
        filename=filename,
    )
    return {
        "attachment_id": attachment_id,
        "consult_case_id": row[1],
        "kind": kind,
        "filename": filename,
        "content_type": content_type,
        "size_bytes": int(row[5] or 0),
        "download_url": _consult_attachment_download_url(attachment_id),
        **preview_meta,
        "created_at": row[6],
    }


def _normalize_consult_message_attachments(
    cur,
    *,
    consult_case: dict[str, Any],
    attachments: list[dict[str, Any]],
    sender_user_id: str | None,
) -> list[dict[str, Any]]:
    if not attachments:
        return []

    normalized_items: list[dict[str, Any]] = []
    for raw_attachment in attachments:
        if not isinstance(raw_attachment, dict):
            raise HTTPException(400, "attachments must be an array of objects")
        attachment_id = _validated_uuid_str(
            str(raw_attachment.get("attachment_id") or raw_attachment.get("id") or "").strip() or None,
            "attachment_id",
        )
        if not attachment_id:
            raise HTTPException(400, "attachments must be uploaded first and include attachment_id")

        cur.execute(
            """
            SELECT id::text,
                   consult_case_id::text,
                   kind,
                   filename,
                   content_type,
                   size_bytes,
                   created_at,
                   uploaded_by_user_id::text
            FROM consult_attachments
            WHERE id = %s
              AND consult_case_id = %s
              AND patient_user_id = %s
            LIMIT 1
            """,
            (
                attachment_id,
                consult_case["consult_case_id"],
                consult_case["patient_user_id"],
            ),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(400, "Attachment not found for this consult")
        if sender_user_id and row[7] != sender_user_id:
            raise HTTPException(403, "Attachments can only be sent by the user who uploaded them")
        attachment_item = _consult_attachment_item_from_row(row)
        attachment_item["created_at"] = _iso_utc(attachment_item.get("created_at"))
        normalized_items.append(attachment_item)

    return normalized_items


def _fetch_consult_attachment_for_user(
    cur,
    *,
    attachment_id: str,
    auth_user_id: str,
) -> tuple[dict[str, Any], tuple[Any, ...]]:
    cur.execute(
        """
        SELECT ca.id::text,
               ca.consult_case_id::text,
               ca.kind,
               ca.filename,
               ca.content_type,
               ca.size_bytes,
               ca.created_at,
               ca.uploaded_by_user_id::text,
               ca.bucket,
               ca.object_key,
               ca.patient_user_id::text
        FROM consult_attachments ca
        WHERE ca.id = %s
        LIMIT 1
        """,
        (attachment_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Attachment not found")

    patient_user_id = row[10]
    if auth_user_id != patient_user_id:
        _authorize_user_read_access(auth_user_id, patient_user_id)

    attachment_item = _consult_attachment_item_from_row(row[:7])
    return attachment_item, row


def _list_consult_case_messages(
    cur,
    *,
    consult_case_id: str,
    patient_user_id: str,
    encounter_id: str | None = None,
) -> list[dict[str, Any]]:
    where = ["m.consult_case_id = %s", "m.patient_user_id = %s"]
    params: list[Any] = [consult_case_id, patient_user_id]
    if encounter_id is not None:
        where.append("m.encounter_id = %s")
        params.append(encounter_id)
    cur.execute(
        f"""
        SELECT m.id::text,
               m.consult_case_id::text,
               m.doctor_user_id::text,
               m.patient_user_id::text,
               m.encounter_id::text,
               m.sender_type,
               m.sender_user_id::text,
               COALESCE(u.name, u.email, CASE WHEN m.sender_type = 'patient' THEN 'Patient' WHEN m.sender_type = 'doctor' THEN 'Doctor' ELSE 'System' END),
               m.message_type,
               m.body,
               m.requires_acknowledgement,
               m.read_at,
               m.attachments,
               m.created_at
        FROM consult_case_messages m
        LEFT JOIN users u ON u.id = m.sender_user_id
        WHERE {' AND '.join(where)}
        ORDER BY m.created_at ASC, m.id ASC
        """,
        params,
    )
    return [_consult_message_item_from_row(row) for row in cur.fetchall()]


def _consult_summary_from_case(consult_case: dict[str, Any]) -> dict[str, Any]:
    revision = int(consult_case.get("revision") or 0)
    due_fields = _consult_runtime_due_fields(consult_case)
    clinician_fields = _consult_clinician_fields(consult_case)
    return {
        "consult_case_id": consult_case.get("consult_case_id"),
        "consult_status": _normalized_consult_status(consult_case.get("consult_status")),
        "consult_status_reason": consult_case.get("consult_status_reason"),
        "priority": _normalized_consult_priority(consult_case.get("priority")),
        "escalation_status": _normalized_escalation_status(consult_case.get("escalation_status")),
        **clinician_fields,
        "opened_at": consult_case.get("opened_at"),
        "closed_at": consult_case.get("closed_at"),
        "closed_reason": consult_case.get("closed_reason"),
        "handoff_requested": bool(consult_case.get("handoff_requested")),
        "handoff_target_clinician_id": consult_case.get("handoff_target_clinician_id"),
        "handoff_target_clinician_name": consult_case.get("handoff_target_clinician_name"),
        "reopened_at": consult_case.get("reopened_at"),
        "last_clinician_action_at": consult_case.get("last_clinician_action_at"),
        "last_clinician_action_type": consult_case.get("last_clinician_action_type"),
        "next_action_due_at": due_fields.get("next_action_due_at"),
        "next_follow_up_due_at": due_fields.get("next_follow_up_due_at"),
        "last_action_summary": consult_case.get("last_action_summary"),
        "updated_at": consult_case.get("updated_at"),
        "revision": revision,
        "etag": _compute_consult_etag(consult_case.get("consult_case_id"), revision, consult_case.get("updated_at")),
    }


def _patient_consult_item_from_case(consult_case: dict[str, Any]) -> dict[str, Any]:
    due_fields = _consult_runtime_due_fields(consult_case)
    return {
        "consult_case_id": consult_case.get("consult_case_id"),
        "doctor_user_id": consult_case.get("doctor_user_id"),
        "doctor_name": consult_case.get("assigned_clinician_name"),
        "doctor_email": consult_case.get("assigned_clinician_email"),
        "consult_status": _normalized_consult_status(consult_case.get("consult_status")),
        "priority": _normalized_consult_priority(consult_case.get("priority")),
        "escalation_status": _normalized_escalation_status(consult_case.get("escalation_status")),
        "opened_at": consult_case.get("opened_at"),
        "closed_at": consult_case.get("closed_at"),
        "next_action_due_at": due_fields.get("next_action_due_at"),
        "next_follow_up_due_at": due_fields.get("next_follow_up_due_at"),
        "last_action_summary": consult_case.get("last_action_summary"),
        "updated_at": consult_case.get("updated_at"),
        "revision": int(consult_case.get("revision") or 0),
        "freshness": _freshness_summary_from_case(consult_case),
    }


def _freshness_summary_from_case(consult_case: dict[str, Any]) -> dict[str, Any]:
    updated_at = _max_dt(
        consult_case.get("updated_at"),
        consult_case.get("last_patient_activity_at"),
        consult_case.get("last_clinician_activity_at"),
        consult_case.get("last_critical_event_at"),
    )
    revision = int(consult_case.get("revision") or 0)
    return {
        "updated_at": updated_at,
        "revision": revision,
        "etag": _compute_consult_etag(consult_case.get("consult_case_id"), revision, updated_at),
    }


def _build_patient_consult_item(cur, consult_case: dict[str, Any]) -> dict[str, Any]:
    message_summary = _build_consult_message_summary(
        cur,
        consult_case_id=consult_case["consult_case_id"],
        patient_user_id=consult_case["patient_user_id"],
    )
    consult_item = _patient_consult_item_from_case(
        {
            **consult_case,
            **_consult_runtime_due_fields(consult_case, message_summary=message_summary),
        }
    )
    doctor_profile = _doctor_profile_summary_fields(consult_case.get("assigned_clinician_profile"))
    consult_item.update(
        {
            "doctor_title": doctor_profile.get("doctor_title"),
            "doctor_specialty": doctor_profile.get("doctor_specialty"),
            "doctor_avatar_url": doctor_profile.get("doctor_avatar_url"),
            "unread_doctor_message_count": int(message_summary.get("unread_doctor_message_count") or 0),
            "requires_acknowledgement_count": int(message_summary.get("requires_acknowledgement_count") or 0),
            "last_message_preview": message_summary.get("last_message_preview") or consult_case.get("last_action_summary"),
            "last_message_at": message_summary.get("last_message_at"),
        }
    )
    return consult_item


def _patient_consult_change_item_from_consult_item(item: dict[str, Any]) -> dict[str, Any]:
    freshness = item.get("freshness") if isinstance(item.get("freshness"), dict) else {}
    return {
        "consult_case_id": item.get("consult_case_id"),
        "updated_at": freshness.get("updated_at") or item.get("updated_at"),
        "revision": int(freshness.get("revision") or item.get("revision") or 0),
        "unread_doctor_message_count": int(item.get("unread_doctor_message_count") or 0),
        "requires_acknowledgement_count": int(item.get("requires_acknowledgement_count") or 0),
        "last_message_preview": item.get("last_message_preview"),
        "last_message_at": item.get("last_message_at"),
    }


def _list_patient_consults(
    cur,
    *,
    patient_user_id: str,
    include_closed: bool = False,
    ensure_assignment_cases: bool = True,
) -> list[dict[str, Any]]:
    if ensure_assignment_cases:
        for doctor_user_id in _list_patient_assigned_doctor_ids(cur, patient_user_id):
            _ensure_consult_case(
                cur,
                doctor_user_id=doctor_user_id,
                patient_user_id=patient_user_id,
                create_if_missing=True,
            )

    where = ["cc.patient_user_id = %s"]
    params: list[Any] = [patient_user_id]
    if not include_closed:
        where.append("cc.closed_at IS NULL")

    cur.execute(
        f"""
        SELECT cc.id::text,
               cc.doctor_user_id::text,
               cc.patient_user_id::text,
               cc.consult_status,
               cc.priority,
               cc.escalation_status,
               cc.opened_at,
               cc.closed_at,
               cc.consult_status_reason,
               cc.next_action_due_at,
               cc.next_follow_up_due_at,
               cc.last_action_summary,
               cc.attention_score,
               cc.attention_reasons,
               cc.last_patient_activity_at,
               cc.last_clinician_activity_at,
               cc.last_critical_event_at,
               cc.updated_at,
               cc.revision,
               cc.closed_reason,
               cc.handoff_requested,
               cc.handoff_target_clinician_id::text,
               cc.reopened_at,
               cc.last_clinician_action_at,
               cc.last_clinician_action_type,
               u.name,
               u.email,
               tu.name,
               dup.profile
        FROM consult_cases cc
        JOIN users u ON u.id = cc.doctor_user_id
        LEFT JOIN users tu ON tu.id = cc.handoff_target_clinician_id
        LEFT JOIN user_profile dup ON dup.user_id = cc.doctor_user_id
        WHERE {' AND '.join(where)}
        ORDER BY cc.closed_at NULLS FIRST, cc.updated_at DESC, cc.opened_at DESC, cc.id DESC
        """,
        params,
    )
    return [_consult_case_from_row(row) for row in cur.fetchall()]


def _list_doctor_patient_requests(
    cur,
    *,
    doctor_user_id: str,
    status: str | None = None,
) -> list[dict[str, Any]]:
    where: list[str] = ["pdr.status = %s"]
    params: list[Any] = [PATIENT_DOCTOR_REQUEST_PENDING]
    where_sql = " AND ".join(where)
    queue_position_sql = _pending_request_queue_position_expr("pdr")
    normalized_status = _normalized_patient_doctor_request_status(status) if status else None
    if normalized_status:
        if normalized_status == PATIENT_DOCTOR_REQUEST_PENDING:
            where = ["pdr.status = %s"]
            params = [PATIENT_DOCTOR_REQUEST_PENDING]
        elif normalized_status == PATIENT_DOCTOR_REQUEST_CLAIMED:
            where = ["pdr.status = %s", "pdr.claimed_by_doctor_user_id = %s"]
            params = [PATIENT_DOCTOR_REQUEST_CLAIMED, doctor_user_id]
        else:
            where = ["pdr.status = %s"]
            params = [PATIENT_DOCTOR_REQUEST_CANCELLED]
        where_sql = " AND ".join(where)

    cur.execute(
        f"""
        SELECT pdr.id::text,
               pdr.patient_user_id::text,
               pu.name,
               pu.email,
               up.profile,
               pdr.status,
               pdr.request_reason,
               pdr.request_priority,
               'patient_portal'::text AS request_source,
               COALESCE((
                   SELECT max(m.created_at)
                   FROM consult_case_messages m
                   WHERE m.patient_user_id = pdr.patient_user_id
                     AND m.sender_type = 'patient'
               ), pdr.updated_at, pdr.created_at) AS last_patient_message_at,
               pdr.claimed_by_doctor_user_id::text,
               du.name,
               pdr.consult_case_id::text,
               pdr.created_at,
               pdr.updated_at,
               pdr.claimed_at,
               pdr.cancelled_at,
               {queue_position_sql} AS queue_position
        FROM patient_doctor_requests pdr
        JOIN users pu ON pu.id = pdr.patient_user_id
        LEFT JOIN user_profile up ON up.user_id = pu.id
        LEFT JOIN users du ON du.id = pdr.claimed_by_doctor_user_id
        WHERE {where_sql}
        ORDER BY CASE WHEN pdr.status = 'pending' THEN 0 ELSE 1 END, pdr.created_at DESC, pdr.id DESC
        """,
        params,
    )
    return [_doctor_patient_request_item_from_row(row) for row in cur.fetchall()]


def _build_consult_message_summary(
    cur,
    *,
    consult_case_id: str,
    patient_user_id: str,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT COALESCE(sum(CASE WHEN sender_type = 'patient' AND read_at IS NULL THEN 1 ELSE 0 END), 0),
               COALESCE(sum(CASE WHEN sender_type = 'doctor' AND read_at IS NULL THEN 1 ELSE 0 END), 0),
               COALESCE(sum(CASE WHEN requires_acknowledgement AND sender_type = 'doctor' AND read_at IS NULL THEN 1 ELSE 0 END), 0),
               (
                   SELECT created_at
                   FROM consult_case_messages
                   WHERE consult_case_id = %s
                     AND patient_user_id = %s
                   ORDER BY created_at DESC, id DESC
                   LIMIT 1
               ),
               (
                   SELECT sender_type
                   FROM consult_case_messages
                   WHERE consult_case_id = %s
                     AND patient_user_id = %s
                   ORDER BY created_at DESC, id DESC
                   LIMIT 1
               ),
               (
                   SELECT body
                   FROM consult_case_messages
                   WHERE consult_case_id = %s
                     AND patient_user_id = %s
                   ORDER BY created_at DESC, id DESC
                   LIMIT 1
               ),
               (
                   SELECT attachments
                   FROM consult_case_messages
                   WHERE consult_case_id = %s
                     AND patient_user_id = %s
                   ORDER BY created_at DESC, id DESC
                   LIMIT 1
               )
        FROM consult_case_messages
        WHERE consult_case_id = %s
          AND patient_user_id = %s
        """,
        (
            consult_case_id,
            patient_user_id,
            consult_case_id,
            patient_user_id,
            consult_case_id,
            patient_user_id,
            consult_case_id,
            patient_user_id,
            consult_case_id,
            patient_user_id,
        ),
    )
    row = cur.fetchone() or (0, 0, 0, None, None, None, [])
    return {
        "consult_case_id": consult_case_id,
        "unread_patient_message_count": int(row[0] or 0),
        "unread_doctor_message_count": int(row[1] or 0),
        "requires_acknowledgement_count": int(row[2] or 0),
        "last_message_at": row[3],
        "last_message_sender_type": _normalized_sender_type(row[4]) if row[4] is not None else None,
        "last_message_preview": _summarize_message_preview(row[5], row[6]),
    }


def _create_consult_case_message(
    cur,
    *,
    consult_case: dict[str, Any],
    sender_type: str,
    sender_user_id: str | None,
    body: str,
    message_type: str,
    requires_acknowledgement: bool,
    attachments: list[dict[str, Any]],
    encounter_id: str | None = None,
) -> dict[str, Any]:
    normalized_sender = _normalized_sender_type(sender_type)
    normalized_type = _normalized_message_type(message_type)
    normalized_body = body.strip()
    if not normalized_body:
        raise HTTPException(400, "body is required")
    normalized_attachments = _normalize_consult_message_attachments(
        cur,
        consult_case=consult_case,
        attachments=attachments,
        sender_user_id=sender_user_id,
    )
    last_action_summary = f"Latest {'instruction' if normalized_type == 'instruction' else 'message'} from {normalized_sender}."
    last_action_type = f"{normalized_sender}_message_sent"
    now = _utcnow()

    cur.execute(
        """
        INSERT INTO consult_case_messages (
            consult_case_id,
            doctor_user_id,
            patient_user_id,
            encounter_id,
            sender_type,
            sender_user_id,
            message_type,
            body,
            requires_acknowledgement,
            attachments
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id::text
        """,
        (
            consult_case["consult_case_id"],
            consult_case["doctor_user_id"],
            consult_case["patient_user_id"],
            encounter_id,
            normalized_sender,
            sender_user_id,
            normalized_type,
            normalized_body,
            requires_acknowledgement,
            psyjson.Json(normalized_attachments),
        ),
    )
    message_id = cur.fetchone()[0]
    cur.execute(
        """
        UPDATE consult_cases
        SET updated_at = now(),
            revision = revision + 1,
            last_action_summary = %s,
            last_clinician_action_at = CASE WHEN %s = 'doctor' THEN now() ELSE last_clinician_action_at END,
            last_clinician_action_type = CASE WHEN %s = 'doctor' THEN %s ELSE last_clinician_action_type END
        WHERE id = %s
        """,
        (
            last_action_summary,
            normalized_sender,
            normalized_sender,
            last_action_type,
            consult_case["consult_case_id"],
        ),
    )
    _insert_consult_case_event(
        cur,
        consult_case_id=consult_case["consult_case_id"],
        doctor_user_id=consult_case["doctor_user_id"],
        patient_user_id=consult_case["patient_user_id"],
        event_type=f"{normalized_sender}_message_sent",
        summary=f"{normalized_sender.capitalize()} sent a {normalized_type}.",
        encounter_id=encounter_id,
        metadata={
            "message_id": message_id,
            "sender_type": normalized_sender,
            "message_type": normalized_type,
            "requires_acknowledgement": requires_acknowledgement,
        },
    )
    consult_case["revision"] = int(consult_case.get("revision") or 0) + 1
    consult_case["updated_at"] = now
    consult_case["last_action_summary"] = last_action_summary
    if normalized_sender == USER_ROLE_DOCTOR:
        consult_case["last_clinician_action_at"] = now
        consult_case["last_clinician_action_type"] = last_action_type

    cur.execute(
        """
        SELECT m.id::text,
               m.consult_case_id::text,
               m.doctor_user_id::text,
               m.patient_user_id::text,
               m.encounter_id::text,
               m.sender_type,
               m.sender_user_id::text,
               COALESCE(u.name, u.email, CASE WHEN m.sender_type = 'patient' THEN 'Patient' WHEN m.sender_type = 'doctor' THEN 'Doctor' ELSE 'System' END),
               m.message_type,
               m.body,
               m.requires_acknowledgement,
               m.read_at,
               m.attachments,
               m.created_at
        FROM consult_case_messages m
        LEFT JOIN users u ON u.id = m.sender_user_id
        WHERE m.id = %s
        """,
        (message_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(500, "Message not found after insert")
    return _consult_message_item_from_row(row)


def _mark_consult_case_message_read(
    cur,
    *,
    consult_case_id: str,
    patient_user_id: str,
    message_id: str,
    reader_sender_type: str,
) -> dict[str, Any]:
    cur.execute(
        """
        UPDATE consult_case_messages
        SET read_at = COALESCE(read_at, now())
        WHERE id = %s
          AND consult_case_id = %s
          AND patient_user_id = %s
          AND sender_type <> %s
        RETURNING id::text
        """,
        (message_id, consult_case_id, patient_user_id, reader_sender_type),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Message not found")

    cur.execute(
        """
        SELECT doctor_user_id::text, encounter_id::text
        FROM consult_case_messages
        WHERE id = %s
        LIMIT 1
        """,
        (message_id,),
    )
    message_meta = cur.fetchone()
    doctor_user_id = message_meta[0] if message_meta else None
    read_encounter_id = message_meta[1] if message_meta else None
    if doctor_user_id:
        _insert_consult_case_event(
            cur,
            consult_case_id=consult_case_id,
            doctor_user_id=doctor_user_id,
            patient_user_id=patient_user_id,
            event_type="message_read",
            summary=f"{reader_sender_type.capitalize()} marked a message as read.",
            encounter_id=read_encounter_id,
            metadata={"message_id": message_id, "reader_type": reader_sender_type},
        )
    if reader_sender_type == USER_ROLE_DOCTOR:
        cur.execute(
            """
            UPDATE consult_cases
            SET updated_at = now(),
                revision = revision + 1,
                last_clinician_action_at = now(),
                last_clinician_action_type = 'message_read'
            WHERE id = %s
            """,
            (consult_case_id,),
        )

    cur.execute(
        """
        SELECT m.id::text,
               m.consult_case_id::text,
               m.doctor_user_id::text,
               m.patient_user_id::text,
               m.encounter_id::text,
               m.sender_type,
               m.sender_user_id::text,
               COALESCE(u.name, u.email, CASE WHEN m.sender_type = 'patient' THEN 'Patient' WHEN m.sender_type = 'doctor' THEN 'Doctor' ELSE 'System' END),
               m.message_type,
               m.body,
               m.requires_acknowledgement,
               m.read_at,
               m.attachments,
               m.created_at
        FROM consult_case_messages m
        LEFT JOIN users u ON u.id = m.sender_user_id
        WHERE m.id = %s
        """,
        (message_id,),
    )
    message_row = cur.fetchone()
    if not message_row:
        raise HTTPException(404, "Message not found")
    return _consult_message_item_from_row(message_row)


def _build_doctor_patient_context(
    cur,
    *,
    doctor_user_id: str,
    patient_user_id: str,
    consult_case_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    consult_case = _ensure_consult_case(
        cur,
        doctor_user_id=doctor_user_id,
        patient_user_id=patient_user_id,
        consult_case_id=consult_case_id,
        create_if_missing=True,
    )
    triage = _compute_consult_metrics(
        cur,
        doctor_user_id=doctor_user_id,
        patient_user_id=patient_user_id,
        consult_case=consult_case,
    )
    chart_summary = _build_chart_summary(
        cur,
        patient_user_id=patient_user_id,
        consult_case=consult_case,
        triage=triage,
    )
    freshness = _freshness_summary_from_case(consult_case)
    return consult_case, triage, chart_summary, freshness


def _build_doctor_queue_item(
    *,
    patient_item: dict[str, Any],
    chart_summary: dict[str, Any],
    consult_case: dict[str, Any],
    triage: dict[str, Any],
    freshness: dict[str, Any],
    message_summary: dict[str, Any],
) -> dict[str, Any]:
    due_fields = _consult_runtime_due_fields(consult_case, triage=triage, message_summary=message_summary)
    clinician_fields = _consult_clinician_fields(consult_case)
    return {
        "consult_case_id": consult_case["consult_case_id"],
        "patient_id": chart_summary["patient_id"],
        "full_name": chart_summary.get("full_name") or patient_item.get("name"),
        "email": patient_item.get("email"),
        "external_patient_id": chart_summary.get("external_patient_id"),
        "consult_status": consult_case["consult_status"],
        "consult_status_reason": consult_case.get("consult_status_reason") or consult_case.get("last_action_summary"),
        "priority": consult_case["priority"],
        "attention_score": triage["attention_score"],
        "attention_reasons": list(triage.get("attention_reasons") or []),
        "review_recommended_count": int(triage.get("review_recommended_count") or 0),
        "abnormal_measurement_count": int(triage.get("abnormal_measurement_count") or 0),
        "unread_patient_message_count": int(triage.get("unread_patient_message_count") or 0),
        "requires_acknowledgement_count": int(message_summary.get("requires_acknowledgement_count") or 0),
        "overdue_follow_up": bool(triage.get("overdue_follow_up")),
        "risk_flags": list(chart_summary.get("risk_flags") or []),
        "patient_age": chart_summary.get("age"),
        "patient_sex": chart_summary.get("sex"),
        "primary_condition": chart_summary.get("primary_condition"),
        "last_message_at": message_summary.get("last_message_at"),
        "last_message_sender_type": message_summary.get("last_message_sender_type"),
        "last_message_preview": message_summary.get("last_message_preview") or consult_case.get("last_action_summary"),
        "last_patient_activity_at": triage.get("last_patient_activity_at"),
        "last_clinician_activity_at": triage.get("last_clinician_activity_at"),
        "last_clinician_action_at": consult_case.get("last_clinician_action_at"),
        "last_clinician_action_type": consult_case.get("last_clinician_action_type"),
        "last_critical_event_at": triage.get("last_critical_event_at"),
        "case_summary": consult_case.get("last_action_summary") or chart_summary.get("baseline_notes") or chart_summary.get("primary_condition"),
        "next_action_due_at": due_fields.get("next_action_due_at"),
        "next_follow_up_due_at": due_fields.get("next_follow_up_due_at"),
        **clinician_fields,
        "closed_reason": consult_case.get("closed_reason"),
        "reopened_at": consult_case.get("reopened_at"),
        "updated_at": freshness.get("updated_at"),
        "revision": freshness.get("revision") or 0,
        "etag": _compute_consult_etag(
            consult_case.get("consult_case_id"),
            int(consult_case.get("revision") or 0),
            consult_case.get("updated_at"),
        ),
    }


def _sort_doctor_queue_items(queue_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    min_dt = datetime.min.replace(tzinfo=timezone.utc)
    queue_items.sort(
        key=lambda item: (
            float(item.get("attention_score") or 0.0),
            PRIORITY_RANK.get(str(item.get("priority") or CONSULT_PRIORITY_ROUTINE), 0),
            -(item.get("next_action_due_at").timestamp()) if isinstance(item.get("next_action_due_at"), datetime) else float("-inf"),
            (item.get("last_patient_activity_at") or min_dt).timestamp() if isinstance(item.get("last_patient_activity_at"), datetime) else float("-inf"),
            item.get("patient_id") or "",
        ),
        reverse=True,
    )
    return queue_items


def _build_doctor_queue_summary_from_items(queue_items: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "total_open": 0,
        "urgent": 0,
        "critical": 0,
        "unread_messages": 0,
        "overdue_follow_up": 0,
        "awaiting_patient": 0,
        "in_review": 0,
    }
    for item in queue_items:
        if item.get("consult_status") != CONSULT_STATUS_CLOSED:
            summary["total_open"] += 1
        if item.get("priority") == CONSULT_PRIORITY_URGENT and item.get("consult_status") != CONSULT_STATUS_CLOSED:
            summary["urgent"] += 1
        if item.get("priority") == CONSULT_PRIORITY_CRITICAL and item.get("consult_status") != CONSULT_STATUS_CLOSED:
            summary["critical"] += 1
        summary["unread_messages"] += int(item.get("unread_patient_message_count") or 0)
        if item.get("overdue_follow_up"):
            summary["overdue_follow_up"] += 1
        if item.get("consult_status") == CONSULT_STATUS_AWAITING_PATIENT:
            summary["awaiting_patient"] += 1
        if item.get("consult_status") == CONSULT_STATUS_IN_REVIEW:
            summary["in_review"] += 1
    return summary


def _list_doctor_queue_items(cur, *, doctor_user_id: str) -> list[dict[str, Any]]:
    patient_items = _list_doctor_patients(doctor_user_id)
    queue_items: list[dict[str, Any]] = []
    for patient_item in patient_items:
        patient_user_id = patient_item["user_id"]
        consult_case, triage, chart_summary, freshness = _build_doctor_patient_context(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=patient_user_id,
        )
        message_summary = _build_consult_message_summary(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=patient_user_id,
        )
        queue_items.append(
            _build_doctor_queue_item(
                patient_item=patient_item,
                chart_summary=chart_summary,
                consult_case=consult_case,
                triage=triage,
                freshness=freshness,
                message_summary=message_summary,
            )
        )
    return _sort_doctor_queue_items(queue_items)


def _filter_doctor_queue_items(
    queue_items: list[dict[str, Any]],
    *,
    consult_status: str | None = None,
    priority: str | None = None,
    risk_flag: str | None = None,
    search: str | None = None,
) -> list[dict[str, Any]]:
    raw_status = (consult_status or "").strip().lower()
    normalized_status = _normalized_consult_status(raw_status) if raw_status and raw_status != "open" else None
    normalized_priority = _normalized_consult_priority(priority) if priority else None
    risk_flag_token = (risk_flag or "").strip().lower()
    search_term = (search or "").strip().lower()

    items = list(queue_items)
    if raw_status == "open":
        items = [item for item in items if item.get("consult_status") != CONSULT_STATUS_CLOSED]
    elif normalized_status:
        items = [item for item in items if item.get("consult_status") == normalized_status]
    if normalized_priority:
        items = [item for item in items if item.get("priority") == normalized_priority]
    if risk_flag_token:
        items = [
            item
            for item in items
            if any(isinstance(flag, str) and flag.strip().lower() == risk_flag_token for flag in item.get("risk_flags") or [])
        ]
    if search_term:
        filtered_items: list[dict[str, Any]] = []
        for item in items:
            haystack = " ".join(
                part
                for part in [
                    item.get("full_name"),
                    item.get("email"),
                    item.get("external_patient_id"),
                    item.get("primary_condition"),
                    item.get("case_summary"),
                    item.get("last_message_preview"),
                ]
                if isinstance(part, str) and part.strip()
            ).lower()
            if search_term in haystack:
                filtered_items.append(item)
        items = filtered_items
    return items


def _build_doctor_queue_changes_payload(cur, *, doctor_user_id: str, since_dt: datetime) -> dict[str, Any]:
    server_time = _utcnow()
    queue_items = _list_doctor_queue_items(cur, doctor_user_id=doctor_user_id)
    updated_items: list[dict[str, Any]] = []
    updated_consults: list[str] = []
    for item in queue_items:
        item_updated_at = _max_dt(item.get("updated_at"), item.get("last_message_at"), item.get("last_clinician_action_at"))
        if item_updated_at and item_updated_at > since_dt:
            updated_items.append(item)
            if isinstance(item.get("consult_case_id"), str):
                updated_consults.append(item["consult_case_id"])

    current_consult_ids = {
        item["consult_case_id"]
        for item in queue_items
        if isinstance(item.get("consult_case_id"), str)
    }
    cur.execute(
        """
        SELECT cc.id::text
        FROM consult_cases cc
        WHERE cc.doctor_user_id = %s
          AND cc.updated_at > %s
          AND (
            cc.closed_at IS NOT NULL
            OR NOT EXISTS (
                SELECT 1
                FROM doctor_patient_assignments dpa
                WHERE dpa.doctor_user_id = cc.doctor_user_id
                  AND dpa.patient_user_id = cc.patient_user_id
            )
          )
        ORDER BY cc.updated_at DESC, cc.id DESC
        LIMIT 500
        """,
        (doctor_user_id, since_dt),
    )
    removed_consults = [
        row[0]
        for row in cur.fetchall()
        if isinstance(row[0], str) and row[0] not in current_consult_ids
    ]

    cur.execute(
        """
        SELECT count(*)
        FROM patient_doctor_requests
        WHERE status = %s
          AND updated_at > %s
        """,
        (PATIENT_DOCTOR_REQUEST_PENDING, since_dt),
    )
    new_requests = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT count(*)
        FROM consult_case_messages m
        JOIN consult_cases cc ON cc.id = m.consult_case_id
        WHERE cc.doctor_user_id = %s
          AND m.sender_type = 'patient'
          AND m.read_at IS NULL
          AND m.created_at > %s
        """,
        (doctor_user_id, since_dt),
    )
    new_unread_messages = int(cur.fetchone()[0] or 0)
    return {
        "updated_consults": updated_consults,
        "updated_items": updated_items,
        "removed_consults": removed_consults,
        "summary": _build_doctor_queue_summary_from_items(queue_items),
        "new_requests": new_requests,
        "new_unread_messages": new_unread_messages,
        "server_time": server_time,
    }


def _build_patient_consult_changes_payload(cur, *, patient_user_id: str, since_dt: datetime) -> dict[str, Any]:
    server_time = _utcnow()
    consult_cases = _list_patient_consults(
        cur,
        patient_user_id=patient_user_id,
        include_closed=False,
        ensure_assignment_cases=True,
    )
    consult_items = [_build_patient_consult_item(cur, case) for case in consult_cases]
    latest_request = _fetch_latest_patient_doctor_request(cur, patient_user_id)
    cur.execute(
        """
        SELECT DISTINCT m.consult_case_id::text
        FROM consult_case_messages m
        JOIN consult_cases cc ON cc.id = m.consult_case_id
        WHERE cc.patient_user_id = %s
          AND (
            m.created_at > %s
            OR (m.read_at IS NOT NULL AND m.read_at > %s)
          )
        ORDER BY 1
        """,
        (patient_user_id, since_dt, since_dt),
    )
    message_changed_consults = {row[0] for row in cur.fetchall() if isinstance(row[0], str)}

    updated_items: list[dict[str, Any]] = []
    updated_consults: list[dict[str, Any]] = []
    unread_doctor_messages = 0
    for item in consult_items:
        unread_doctor_messages += int(item.get("unread_doctor_message_count") or 0)
        freshness = item.get("freshness") if isinstance(item.get("freshness"), dict) else {}
        item_updated_at = _max_dt(
            freshness.get("updated_at"),
            item.get("updated_at"),
            item.get("last_message_at"),
        )
        if item.get("consult_case_id") in message_changed_consults or (item_updated_at and item_updated_at > since_dt):
            updated_items.append(item)
            updated_consults.append(_patient_consult_change_item_from_consult_item(item))

    current_consult_ids = {
        item["consult_case_id"]
        for item in consult_items
        if isinstance(item.get("consult_case_id"), str)
    }
    cur.execute(
        """
        SELECT cc.id::text
        FROM consult_cases cc
        WHERE cc.patient_user_id = %s
          AND cc.updated_at > %s
          AND cc.closed_at IS NOT NULL
        ORDER BY cc.updated_at DESC, cc.id DESC
        LIMIT 500
        """,
        (patient_user_id, since_dt),
    )
    removed_consults = [
        row[0]
        for row in cur.fetchall()
        if isinstance(row[0], str) and row[0] not in current_consult_ids
    ]

    request_changed_at = _max_dt(
        latest_request.get("updated_at") if latest_request else None,
        latest_request.get("claimed_at") if latest_request else None,
        latest_request.get("cancelled_at") if latest_request else None,
        latest_request.get("last_patient_message_at") if latest_request else None,
    )
    pending_request_updated = bool(request_changed_at and request_changed_at > since_dt)
    pending_request_payload = None
    if latest_request:
        if latest_request.get("status") == PATIENT_DOCTOR_REQUEST_PENDING or pending_request_updated:
            pending_request_payload = latest_request

    return {
        "updated_consults": updated_consults,
        "updated_items": updated_items,
        "removed_consults": removed_consults,
        "pending_request_updated": pending_request_updated,
        "pending_request": pending_request_payload,
        "unread_doctor_messages": unread_doctor_messages,
        "server_time": server_time,
    }


def _build_doctor_notification_summary(cur, *, doctor_user_id: str) -> dict[str, Any]:
    queue_items = _list_doctor_queue_items(cur, doctor_user_id=doctor_user_id)
    cur.execute(
        """
        SELECT count(*)
        FROM patient_doctor_requests
        WHERE status = %s
        """,
        (PATIENT_DOCTOR_REQUEST_PENDING,),
    )
    pending_requests = int(cur.fetchone()[0] or 0)
    updated_at = _max_dt(*[item.get("updated_at") for item in queue_items]) if queue_items else None
    unread_messages = sum(int(item.get("unread_patient_message_count") or 0) for item in queue_items)
    requires_ack = sum(int(item.get("requires_acknowledgement_count") or 0) for item in queue_items)
    active_consult_count = len(queue_items)
    overdue_follow_up_count = sum(1 for item in queue_items if item.get("overdue_follow_up"))
    urgent_count = sum(1 for item in queue_items if item.get("priority") == CONSULT_PRIORITY_URGENT and item.get("consult_status") != CONSULT_STATUS_CLOSED)
    critical_count = sum(1 for item in queue_items if item.get("priority") == CONSULT_PRIORITY_CRITICAL and item.get("consult_status") != CONSULT_STATUS_CLOSED)
    last_event_at = _max_dt(
        *[
            _max_dt(item.get("last_message_at"), item.get("updated_at"), item.get("last_clinician_action_at"))
            for item in queue_items
        ]
    )
    return {
        "unread_messages": unread_messages,
        "unread_patient_message_count": unread_messages,
        "requires_acknowledgement_count": requires_ack,
        "pending_requests": pending_requests,
        "pending_request_count": pending_requests,
        "updated_consults": active_consult_count,
        "active_consult_count": active_consult_count,
        "overdue_follow_up_count": overdue_follow_up_count,
        "urgent_count": urgent_count,
        "critical_count": critical_count,
        "last_event_at": last_event_at,
        "updated_at": updated_at,
    }


def _build_patient_notification_summary(cur, *, patient_user_id: str) -> dict[str, Any]:
    consult_items = [
        _build_patient_consult_item(cur, case)
        for case in _list_patient_consults(
            cur,
            patient_user_id=patient_user_id,
            include_closed=False,
            ensure_assignment_cases=True,
        )
    ]
    pending_request = _fetch_open_patient_doctor_request(cur, patient_user_id)
    updated_at = _max_dt(
        *[
            _max_dt(item.get("updated_at"), item.get("last_message_at"))
            for item in consult_items
        ],
        pending_request.get("updated_at") if pending_request else None,
    )
    unread_messages = sum(int(item.get("unread_doctor_message_count") or 0) for item in consult_items)
    requires_ack = sum(int(item.get("requires_acknowledgement_count") or 0) for item in consult_items)
    pending_request_count = 1 if pending_request else 0
    active_consult_count = len(consult_items)
    last_event_at = _max_dt(
        *[
            _max_dt(item.get("last_message_at"), item.get("updated_at"))
            for item in consult_items
        ],
        pending_request.get("updated_at") if pending_request else None,
    )
    return {
        "unread_messages": unread_messages,
        "unread_doctor_message_count": unread_messages,
        "requires_acknowledgement_count": requires_ack,
        "pending_requests": pending_request_count,
        "pending_request_count": pending_request_count,
        "updated_consults": active_consult_count,
        "active_consult_count": active_consult_count,
        "last_event_at": last_event_at,
        "updated_at": updated_at,
    }


def _stream_poll_seconds(value: float | None) -> float:
    if value is None:
        return STREAM_DEFAULT_POLL_SECONDS
    try:
        return min(max(float(value), 1.0), STREAM_MAX_POLL_SECONDS)
    except (TypeError, ValueError):
        return STREAM_DEFAULT_POLL_SECONDS


def _sse_frame(event: str, payload: dict[str, Any]) -> bytes:
    encoded = _json.dumps(jsonable_encoder(payload), separators=(",", ":"))
    return f"event: {event}\ndata: {encoded}\n\n".encode("utf-8")


def _timeline_event(
    *,
    event_id: str,
    event_type: str,
    timestamp: datetime,
    actor_type: str,
    actor_name: str | None,
    summary: str,
    encounter_id: str | None,
    consult_case_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "actor_type": actor_type,
        "actor_name": actor_name,
        "summary": summary,
        "encounter_id": encounter_id,
        "consult_case_id": consult_case_id,
        "metadata": metadata or {},
    }


def _build_patient_timeline_events(
    cur,
    *,
    doctor_user_id: str,
    patient_user_id: str,
    consult_case: dict[str, Any],
    encounter_id: str | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    active_case_id = consult_case["consult_case_id"]
    doctor_name = consult_case.get("assigned_clinician_name") or "Doctor"

    measurement_payload = build_measurement_history(patient_user_id, 200, include_messages=False)
    for session in measurement_payload.get("sessions") or []:
        item_encounter_id = session.get("encounter_id")
        if encounter_id and item_encounter_id != encounter_id:
            continue
        event_ts = _parse_iso_datetime(session.get("ended_at")) or _parse_iso_datetime(session.get("started_at"))
        if event_ts is None:
            continue
        measurement_id = session.get("measurement_id") or "measurement"
        events.append(
            _timeline_event(
                event_id=f"measurement:{session.get('measurement_run_id') or item_encounter_id or event_ts.timestamp()}",
                event_type="measurement_uploaded",
                timestamp=event_ts,
                actor_type=USER_ROLE_PATIENT,
                actor_name="Patient",
                summary=f"Patient submitted {measurement_id}.",
                encounter_id=item_encounter_id if isinstance(item_encounter_id, str) else None,
                consult_case_id=active_case_id,
                metadata={
                    "measurement_id": session.get("measurement_id"),
                    "measurement_run_id": session.get("measurement_run_id"),
                    "source": session.get("source"),
                },
            )
        )

    cur.execute(
        """
        SELECT sa.measurement_run_id::text,
               sa.encounter_id::text,
               sa.status,
               sa.summary,
               sa.analyzed_at
        FROM sensor_analysis sa
        LEFT JOIN user_sessions us ON us.id = sa.session_id
        WHERE (
            us.user_id = %s::uuid
            OR (
                sa.encounter_id IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM user_sessions ux
                    WHERE ux.user_id = %s::uuid
                      AND ux.encounter_id = sa.encounter_id
                )
            )
        )
          AND lower(coalesce(sa.status, '')) = 'unusual'
          AND (%s::uuid IS NULL OR sa.encounter_id = %s::uuid)
        ORDER BY sa.analyzed_at DESC
        LIMIT 200
        """,
        (patient_user_id, patient_user_id, encounter_id, encounter_id),
    )
    for measurement_run_id, flagged_encounter_id, status, summary, analyzed_at in cur.fetchall():
        if not isinstance(analyzed_at, datetime):
            continue
        events.append(
            _timeline_event(
                event_id=f"measurement-flag:{measurement_run_id or flagged_encounter_id or analyzed_at.timestamp()}",
                event_type="measurement_flagged",
                timestamp=analyzed_at,
                actor_type="system",
                actor_name="System",
                summary=(str(summary).strip() if isinstance(summary, str) and summary.strip() else "Measurement flagged for review."),
                encounter_id=flagged_encounter_id if isinstance(flagged_encounter_id, str) else None,
                consult_case_id=active_case_id,
                metadata={
                    "measurement_run_id": measurement_run_id,
                    "status": status,
                },
            )
        )

    image_rows = _fetch_image_analysis_history_rows(cur, user_id=patient_user_id, limit=200)
    for row in image_rows:
        item = _image_analysis_history_item_from_row(row, include_previews=False, target_user_id=patient_user_id)
        item_encounter_id = item.get("encounter_id")
        if encounter_id and item_encounter_id != encounter_id:
            continue
        ts = _parse_iso_datetime(item.get("analyzed_at")) or _parse_iso_datetime(item.get("created_at"))
        if ts is None:
            continue
        events.append(
            _timeline_event(
                event_id=f"image:{item['analysis_id']}",
                event_type="image_review_recommended"
                if str(item.get("status") or "").strip().lower() == "review_recommended"
                else "image_uploaded",
                timestamp=ts,
                actor_type="system",
                actor_name="System",
                summary=item.get("summary") or "Image analysis available.",
                encounter_id=item_encounter_id if isinstance(item_encounter_id, str) else None,
                consult_case_id=active_case_id,
                metadata={
                    "analysis_id": item.get("analysis_id"),
                    "source_image_id": item.get("source_image_id"),
                    "overlay_image_id": item.get("overlay_image_id"),
                    "status": item.get("status"),
                },
            )
        )

    cur.execute(
        """
        SELECT id::text, created_at, model, status, payload_hash, encounter_id::text, consult_case_id::text
        FROM reports
        WHERE user_id = %s
          AND (%s::uuid IS NULL OR consult_case_id = %s::uuid OR consult_case_id IS NULL)
          AND (%s::uuid IS NULL OR encounter_id = %s::uuid)
        ORDER BY created_at DESC
        LIMIT 200
        """,
        (patient_user_id, active_case_id, active_case_id, encounter_id, encounter_id),
    )
    for row in cur.fetchall():
        report = _report_list_item_from_row(row)
        created_at = report.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        events.append(
            _timeline_event(
                event_id=f"report:{report['report_id']}",
                event_type="report_generated",
                timestamp=created_at,
                actor_type="system",
                actor_name="System",
                summary="Report generated.",
                encounter_id=report.get("encounter_id"),
                consult_case_id=report.get("consult_case_id") or active_case_id,
                metadata={"report_id": report["report_id"], "model": report.get("model"), "status": report.get("status")},
            )
        )

    for note in _list_doctor_patient_notes(doctor_user_id, patient_user_id, limit=200):
        item_encounter_id = note.get("encounter_id")
        if encounter_id and item_encounter_id != encounter_id:
            continue
        note_ts = note.get("updated_at") if isinstance(note.get("updated_at"), datetime) else note.get("created_at")
        if not isinstance(note_ts, datetime):
            continue
        events.append(
            _timeline_event(
                event_id=f"note:{note['note_id']}",
                event_type="encounter_updated" if item_encounter_id else "consult_updated",
                timestamp=note_ts,
                actor_type=USER_ROLE_DOCTOR,
                actor_name=doctor_name,
                summary="Clinician note added.",
                encounter_id=item_encounter_id if isinstance(item_encounter_id, str) else None,
                consult_case_id=active_case_id,
                metadata={"note_id": note["note_id"], "note_text": note["note_text"]},
            )
        )

    for message in _list_consult_case_messages(cur, consult_case_id=active_case_id, patient_user_id=patient_user_id, encounter_id=encounter_id):
        created_at = message.get("created_at")
        if not isinstance(created_at, datetime):
            continue
        events.append(
            _timeline_event(
                event_id=f"thread:{message['message_id']}",
                event_type="patient_message_sent"
                if message.get("sender_type") == USER_ROLE_PATIENT
                else "doctor_message_sent"
                if message.get("sender_type") == USER_ROLE_DOCTOR
                else "message_sent",
                timestamp=created_at,
                actor_type=message.get("sender_type") or "system",
                actor_name=message.get("sender_name"),
                summary=message.get("body") or "Message sent.",
                encounter_id=message.get("encounter_id"),
                consult_case_id=message.get("consult_case_id"),
                metadata={
                    "message_id": message.get("message_id"),
                    "message_type": message.get("message_type"),
                    "requires_acknowledgement": message.get("requires_acknowledgement"),
                },
            )
        )

    cur.execute(
        """
        SELECT id::text, event_type, summary, encounter_id::text, metadata, created_at
        FROM consult_case_events
        WHERE consult_case_id = %s
          AND (%s::uuid IS NULL OR encounter_id = %s::uuid)
        ORDER BY created_at DESC
        LIMIT 200
        """,
        (active_case_id, encounter_id, encounter_id),
    )
    for row in cur.fetchall():
        created_at = row[5]
        if not isinstance(created_at, datetime):
            continue
        metadata = row[4] if isinstance(row[4], dict) else {}
        raw_event_type = str(row[1] or "").strip().lower()
        mapped_event_type = {
            "consult_created": "consult_created",
            "consult_status_changed": "consult_updated",
            "priority_changed": "consult_updated",
            "escalation_changed": "consult_updated",
            "follow_up_requested": "follow_up_due",
            "case_closed": "consult_closed",
            "case_reopened": "consult_reopened",
            "consult_updated": "consult_updated",
            "message_read": "message_read",
            "patient_message_sent": "patient_message_sent",
            "doctor_message_sent": "doctor_message_sent",
        }.get(raw_event_type, raw_event_type or "consult_updated")
        if mapped_event_type in {"patient_message_sent", "doctor_message_sent", "message_sent"}:
            continue
        actor_type = USER_ROLE_DOCTOR
        actor_name = doctor_name
        if mapped_event_type in {"message_read", "patient_message_sent"} and metadata.get("reader_type") == USER_ROLE_PATIENT:
            actor_type = USER_ROLE_PATIENT
            actor_name = "Patient"
        events.append(
            _timeline_event(
                event_id=f"caseevent:{row[0]}",
                event_type=mapped_event_type,
                timestamp=created_at,
                actor_type=actor_type,
                actor_name=actor_name,
                summary=str(row[2]),
                encounter_id=row[3],
                consult_case_id=active_case_id,
                metadata=metadata,
            )
        )

    events.sort(
        key=lambda item: (
            item.get("timestamp") or datetime.min.replace(tzinfo=timezone.utc),
            item.get("event_id") or "",
        ),
        reverse=True,
    )
    return events


def _validate_patient_encounter_id(patient_user_id: str, encounter_id: str) -> str:
    try:
        resolved_encounter_id = str(uuid.UUID(encounter_id))
    except ValueError as exc:
        raise HTTPException(400, "encounter_id must be a valid UUID") from exc

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM user_sessions
            WHERE user_id = %s
              AND encounter_id = %s
            LIMIT 1
            """,
            (patient_user_id, resolved_encounter_id),
        )
        if not cur.fetchone():
            raise HTTPException(404, "Encounter not found for patient")

    return resolved_encounter_id


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_epoch_datetime(value: Any) -> datetime | None:
    numeric = _as_number(value)
    if numeric is None or not math.isfinite(numeric):
        return None

    seconds: float | None = None
    magnitude = abs(numeric)
    if magnitude >= 1_000_000_000_00:
        seconds = float(numeric) / 1000.0
    elif magnitude >= 1_000_000_000:
        seconds = float(numeric)

    if seconds is None:
        return None

    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _parse_datetime_like(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        parsed = _parse_iso_datetime(value)
        if parsed is not None:
            return parsed
    return _parse_epoch_datetime(value)


def _normalize_requested_measurements(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        token = item.strip().lower().replace(" ", "_")
        if not token or token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _optical_waveform_api_path(measurement_run_id: str) -> str:
    return f"/api/measurements/optical/runs/{measurement_run_id}/waveform"


def _optical_waveform_stream_path(measurement_run_id: str) -> str:
    return f"/api/measurements/optical/runs/{measurement_run_id}/stream"


def _serialize_optical_capture_run_row(row: Any) -> dict[str, Any]:
    return {
        "measurement_run_id": row[0],
        "patient_user_id": row[1],
        "encounter_id": row[2],
        "session_id": row[3],
        "device_external_id": row[4],
        "mode": row[5] or OPTICAL_MEASUREMENT_MODE,
        "status": row[6] or OPTICAL_RUN_STATUS_ACTIVE,
        "started_at": row[7],
        "stopped_at": row[8],
        "sample_rate_hz": _as_number(row[9]),
        "requested_measurements": _normalize_requested_measurements(row[10]),
        "raw_waveform_available": bool(row[11]),
        "total_chunks": int(row[12] or 0),
        "total_packets": int(row[13] or 0),
        "total_samples": int(row[14] or 0),
        "ts1_frame_count": int(row[15] or 0),
        "packet_loss_count": int(row[16] or 0),
        "checksum_failure_count": int(row[17] or 0),
        "malformed_packet_count": int(row[18] or 0),
        "latest_chunk_at": row[19],
        "waveform_api_path": _optical_waveform_api_path(row[0]),
        "waveform_stream_path": _optical_waveform_stream_path(row[0]),
    }


def _fetch_optical_capture_run_row(cur, measurement_run_id: str) -> Any:
    cur.execute(
        """
        SELECT o.measurement_run_id::text,
               o.patient_user_id::text,
               COALESCE(o.encounter_id::text, us.encounter_id::text, us.id::text),
               o.session_id::text,
               o.device_external_id,
               o.mode,
               o.status,
               o.started_at,
               o.stopped_at,
               o.sample_rate_hz,
               o.requested_measurements,
               o.raw_waveform_available,
               o.total_chunks,
               o.total_packets,
               o.total_samples,
               o.ts1_frame_count,
               o.packet_loss_count,
               o.checksum_failure_count,
               o.malformed_packet_count,
               o.latest_chunk_at
        FROM optical_capture_runs o
        LEFT JOIN user_sessions us ON us.id = o.session_id
        WHERE o.measurement_run_id = %s
        LIMIT 1
        """,
        (measurement_run_id,),
    )
    return cur.fetchone()


def _list_optical_capture_chunk_rows(cur, measurement_run_id: str) -> list[Any]:
    cur.execute(
        """
        SELECT chunk_index,
               chunk_started_at,
               chunk_ended_at,
               packet_count,
               sample_count,
               first_packet_id,
               last_packet_id,
               packet_loss_count_delta,
               payload_json
        FROM optical_capture_chunks
        WHERE measurement_run_id = %s
        ORDER BY chunk_started_at ASC NULLS FIRST, chunk_index ASC
        """,
        (measurement_run_id,),
    )
    return cur.fetchall()


def _fetch_optical_capture_run_analysis(cur, measurement_run_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT status,
               analysis_payload,
               analyzed_at
        FROM optical_capture_run_analysis
        WHERE measurement_run_id = %s
        LIMIT 1
        """,
        (measurement_run_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    payload = row[1] if isinstance(row[1], dict) else {}
    return {
        "measurement_run_id": measurement_run_id,
        "status": str(row[0] or OPTICAL_ANALYSIS_STATUS_PROCESSING),
        "final_optical": payload.get("final_optical") if isinstance(payload.get("final_optical"), dict) else None,
        "analyzed_at": row[2],
    }


def _persist_optical_capture_run_analysis(cur, *, measurement_run_id: str, analysis: dict[str, Any]) -> dict[str, Any]:
    analyzed_at = analysis.get("analyzed_at") if isinstance(analysis.get("analyzed_at"), datetime) else _utcnow()
    payload = {
        "final_optical": analysis.get("final_optical") if isinstance(analysis.get("final_optical"), dict) else None,
    }
    cur.execute(
        """
        INSERT INTO optical_capture_run_analysis (
            measurement_run_id,
            status,
            analysis_payload,
            analyzed_at
        )
        VALUES (%s, %s, %s::jsonb, %s)
        ON CONFLICT (measurement_run_id) DO UPDATE
        SET status = EXCLUDED.status,
            analysis_payload = EXCLUDED.analysis_payload,
            analyzed_at = EXCLUDED.analyzed_at
        RETURNING status,
                  analysis_payload,
                  analyzed_at
        """,
        (
            measurement_run_id,
            str(analysis.get("status") or OPTICAL_ANALYSIS_STATUS_PROCESSING),
            _json.dumps(jsonable_encoder(payload, exclude_none=True)),
            analyzed_at,
        ),
    )
    row = cur.fetchone()
    stored_payload = row[1] if row and isinstance(row[1], dict) else payload
    return {
        "measurement_run_id": measurement_run_id,
        "status": row[0] if row else str(analysis.get("status") or OPTICAL_ANALYSIS_STATUS_PROCESSING),
        "final_optical": stored_payload.get("final_optical") if isinstance(stored_payload.get("final_optical"), dict) else None,
        "analyzed_at": row[2] if row else analyzed_at,
    }


def _clamp_number(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _moving_average(values: list[float], window_size: int) -> list[float]:
    if not values:
        return []
    if window_size <= 1:
        return [float(value) for value in values]

    prefix: list[float] = [0.0]
    for value in values:
        prefix.append(prefix[-1] + float(value))

    half_window = window_size // 2
    averaged: list[float] = []
    for index in range(len(values)):
        start = max(0, index - half_window)
        end = min(len(values), index + half_window + 1)
        averaged.append((prefix[end] - prefix[start]) / float(end - start))
    return averaged


def _estimate_effective_sample_rate_hz(points: list[dict[str, Any]], fallback_hz: float | None) -> float | None:
    if fallback_hz and fallback_hz > 0:
        return float(fallback_hz)
    if len(points) < 2:
        return None
    start_dt = points[0].get("_dt")
    end_dt = points[-1].get("_dt")
    if not isinstance(start_dt, datetime) or not isinstance(end_dt, datetime) or end_dt <= start_dt:
        return None
    duration_seconds = (end_dt - start_dt).total_seconds()
    if duration_seconds <= 0:
        return None
    return float((len(points) - 1) / duration_seconds)


def _detect_optical_ppg_peaks(points: list[dict[str, Any]], sample_rate_hz: float | None) -> tuple[list[int], list[float], float | None, float | None]:
    values = [float(point.get("value") or 0.0) for point in points]
    if len(values) < 3:
        return [], [], sample_rate_hz, None

    effective_sample_rate_hz = _estimate_effective_sample_rate_hz(points, sample_rate_hz)
    if effective_sample_rate_hz is None or effective_sample_rate_hz <= 0:
        return [], [], None, None

    short_window = max(3, int(effective_sample_rate_hz * 0.15))
    if short_window % 2 == 0:
        short_window += 1
    long_window = max(short_window + 2, int(effective_sample_rate_hz * 0.8))
    if long_window % 2 == 0:
        long_window += 1

    smoothed = _moving_average(values, short_window)
    baseline = _moving_average(smoothed, long_window)
    detrended = [smoothed[index] - baseline[index] for index in range(len(smoothed))]
    if len(detrended) < 3:
        return [], [], effective_sample_rate_hz, None

    amplitude_std = statistics.pstdev(detrended)
    if not math.isfinite(amplitude_std) or amplitude_std <= 1e-9:
        return [], detrended, effective_sample_rate_hz, None

    threshold = amplitude_std * 0.35
    min_peak_distance = max(1, int(effective_sample_rate_hz * 0.33))
    peaks: list[int] = []
    for index in range(1, len(detrended) - 1):
        candidate = detrended[index]
        if candidate < threshold:
            continue
        if candidate <= detrended[index - 1] or candidate < detrended[index + 1]:
            continue
        if peaks and index - peaks[-1] < min_peak_distance:
            if candidate > detrended[peaks[-1]]:
                peaks[-1] = index
            continue
        peaks.append(index)

    return peaks, detrended, effective_sample_rate_hz, amplitude_std


def _optical_peak_intervals_seconds(points: list[dict[str, Any]], peak_indices: list[int], sample_rate_hz: float | None) -> list[float]:
    intervals: list[float] = []
    for previous_index, current_index in zip(peak_indices, peak_indices[1:]):
        if current_index <= previous_index:
            continue
        previous_dt = points[previous_index].get("_dt")
        current_dt = points[current_index].get("_dt")
        if isinstance(previous_dt, datetime) and isinstance(current_dt, datetime) and current_dt > previous_dt:
            interval_seconds = (current_dt - previous_dt).total_seconds()
        elif sample_rate_hz and sample_rate_hz > 0:
            interval_seconds = float(current_index - previous_index) / float(sample_rate_hz)
        else:
            continue
        if interval_seconds > 0:
            intervals.append(interval_seconds)
    return intervals


def _derive_optical_final_result(run_payload: dict[str, Any], chunk_rows: list[Any]) -> dict[str, Any]:
    points_by_series, _extra_gaps = _extract_optical_ppg_points(
        chunk_rows,
        sample_rate_hz=run_payload.get("sample_rate_hz"),
        from_dt=None,
        to_dt=None,
    )
    ir_points = points_by_series.get("ppg_ir") or []
    red_points = points_by_series.get("ppg_red") or []

    sample_count_used = min(len(ir_points), len(red_points)) if red_points else len(ir_points)
    start_candidates = [point.get("_dt") for point in (ir_points[:1] + red_points[:1]) if isinstance(point.get("_dt"), datetime)]
    end_candidates = [point.get("_dt") for point in ((ir_points[-1:] if ir_points else []) + (red_points[-1:] if red_points else [])) if isinstance(point.get("_dt"), datetime)]
    start_dt = min(start_candidates) if start_candidates else None
    end_dt = max(end_candidates) if end_candidates else None
    window_seconds = (end_dt - start_dt).total_seconds() if isinstance(start_dt, datetime) and isinstance(end_dt, datetime) and end_dt > start_dt else 0.0
    packet_loss_ratio = float(run_payload.get("packet_loss_count") or 0) / float(max(1, int(run_payload.get("total_samples") or sample_count_used or 1)))

    final_optical: dict[str, Any] = {
        "heart_rate_bpm": None,
        "spo2_percent": None,
        "confidence": 0.0,
        "quality": OPTICAL_FINAL_OPTICAL_QUALITY_INSUFFICIENT,
        "sample_count_used": int(sample_count_used),
        "algorithm_version": OPTICAL_FINAL_OPTICAL_ALGORITHM_VERSION,
        "window_seconds": _round_number(window_seconds, 2) or 0.0,
        "derived_from": "raw_ppg",
        "rejection_reason": None,
    }

    if not ir_points:
        final_optical["rejection_reason"] = "No infrared PPG samples were uploaded for this run."
        return final_optical
    if sample_count_used < 120 or window_seconds < 4.0:
        final_optical["rejection_reason"] = "Not enough raw PPG data was uploaded to compute a final result."
        return final_optical

    peak_indices, _detrended_ir, effective_sample_rate_hz, amplitude_std = _detect_optical_ppg_peaks(ir_points, run_payload.get("sample_rate_hz"))
    if not peak_indices or len(peak_indices) < 3 or effective_sample_rate_hz is None:
        final_optical["rejection_reason"] = "Pulse peaks could not be resolved from the uploaded raw PPG signal."
        return final_optical

    intervals_seconds = _optical_peak_intervals_seconds(ir_points, peak_indices, effective_sample_rate_hz)
    if len(intervals_seconds) < 2:
        final_optical["rejection_reason"] = "Not enough stable pulse intervals were detected in the raw PPG signal."
        return final_optical

    median_interval_seconds = statistics.median(intervals_seconds)
    if not math.isfinite(median_interval_seconds) or median_interval_seconds <= 0:
        final_optical["rejection_reason"] = "Pulse intervals from the uploaded raw PPG were invalid."
        return final_optical

    heart_rate_bpm = 60.0 / median_interval_seconds
    if not math.isfinite(heart_rate_bpm) or heart_rate_bpm < 35.0 or heart_rate_bpm > 220.0:
        final_optical["rejection_reason"] = "The derived heart-rate value was outside the valid range."
        return final_optical

    interval_mean_seconds = statistics.fmean(intervals_seconds)
    interval_cv = statistics.pstdev(intervals_seconds) / interval_mean_seconds if len(intervals_seconds) > 1 and interval_mean_seconds > 0 else 1.0
    ir_values = [float(point.get("value") or 0.0) for point in ir_points]
    mean_ir_value = statistics.fmean(ir_values) if ir_values else 0.0
    signal_relative_amplitude = abs(amplitude_std) / max(abs(mean_ir_value), 1e-9) if amplitude_std is not None else 0.0
    signal_confidence = _clamp_number(signal_relative_amplitude * 12.0, 0.0, 1.0)
    rhythm_confidence = _clamp_number(1.0 - (interval_cv / 0.35), 0.0, 1.0)
    duration_confidence = _clamp_number(window_seconds / 20.0, 0.0, 1.0)
    sample_confidence = _clamp_number(sample_count_used / 800.0, 0.0, 1.0)

    spo2_percent: float | None = None
    if red_points:
        paired_count = min(len(ir_points), len(red_points))
        red_values = [float(point.get("value") or 0.0) for point in red_points[-paired_count:]]
        ir_segment_values = ir_values[-paired_count:]
        if paired_count >= 120:
            short_window = max(3, int((effective_sample_rate_hz or 25.0) * 0.15))
            if short_window % 2 == 0:
                short_window += 1
            long_window = max(short_window + 2, int((effective_sample_rate_hz or 25.0) * 0.8))
            if long_window % 2 == 0:
                long_window += 1
            smoothed_red = _moving_average(red_values, short_window)
            smoothed_ir = _moving_average(ir_segment_values, short_window)
            baseline_red = _moving_average(smoothed_red, long_window)
            baseline_ir = _moving_average(smoothed_ir, long_window)
            ac_red = statistics.pstdev([smoothed_red[index] - baseline_red[index] for index in range(len(smoothed_red))])
            ac_ir = statistics.pstdev([smoothed_ir[index] - baseline_ir[index] for index in range(len(smoothed_ir))])
            dc_red = statistics.fmean(smoothed_red)
            dc_ir = statistics.fmean(smoothed_ir)
            if all(math.isfinite(value) for value in (ac_red, ac_ir, dc_red, dc_ir)) and dc_red > 0 and dc_ir > 0 and ac_red > 0 and ac_ir > 0:
                ratio = (ac_red / dc_red) / (ac_ir / dc_ir)
                if math.isfinite(ratio) and 0.2 <= ratio <= 2.2:
                    spo2_percent = _clamp_number(110.0 - (25.0 * ratio), 70.0, 100.0)

    confidence = 0.2 * duration_confidence + 0.2 * sample_confidence + 0.35 * rhythm_confidence + 0.25 * signal_confidence
    confidence *= max(0.0, 1.0 - min(0.65, packet_loss_ratio * 6.0))
    if spo2_percent is None:
        confidence *= 0.8
    confidence = _clamp_number(confidence, 0.0, 1.0)

    quality = OPTICAL_FINAL_OPTICAL_QUALITY_POOR
    if confidence >= 0.8 and packet_loss_ratio <= 0.02 and interval_cv <= 0.18:
        quality = OPTICAL_FINAL_OPTICAL_QUALITY_GOOD
    elif confidence >= 0.55:
        quality = OPTICAL_FINAL_OPTICAL_QUALITY_FAIR
    elif confidence < 0.2:
        quality = OPTICAL_FINAL_OPTICAL_QUALITY_INSUFFICIENT

    final_optical.update(
        {
            "heart_rate_bpm": _round_number(heart_rate_bpm, 1),
            "spo2_percent": _round_number(spo2_percent, 1) if spo2_percent is not None else None,
            "confidence": _round_number(confidence, 4) or 0.0,
            "quality": quality,
            "rejection_reason": None if quality != OPTICAL_FINAL_OPTICAL_QUALITY_INSUFFICIENT else "The uploaded raw PPG signal quality was insufficient for a validated final result.",
        }
    )
    return final_optical


def _compute_optical_capture_run_analysis(cur, *, run_payload: dict[str, Any]) -> dict[str, Any]:
    chunk_rows = _list_optical_capture_chunk_rows(cur, run_payload["measurement_run_id"])
    final_optical = _derive_optical_final_result(run_payload, chunk_rows)
    return {
        "measurement_run_id": run_payload["measurement_run_id"],
        "status": OPTICAL_ANALYSIS_STATUS_COMPLETED,
        "final_optical": final_optical,
        "analyzed_at": _utcnow(),
    }


def _resolve_optical_capture_run_analysis(cur, *, run_payload: dict[str, Any]) -> dict[str, Any]:
    existing_analysis = _fetch_optical_capture_run_analysis(cur, run_payload["measurement_run_id"])
    if existing_analysis is not None:
        return existing_analysis
    if run_payload.get("status") != OPTICAL_RUN_STATUS_STOPPED:
        return {
            "measurement_run_id": run_payload["measurement_run_id"],
            "status": OPTICAL_ANALYSIS_STATUS_PROCESSING,
            "final_optical": None,
            "analyzed_at": None,
        }
    computed_analysis = _compute_optical_capture_run_analysis(cur, run_payload=run_payload)
    return _persist_optical_capture_run_analysis(
        cur,
        measurement_run_id=run_payload["measurement_run_id"],
        analysis=computed_analysis,
    )


def _validate_optical_capture_run_create_context(
    cur,
    *,
    patient_user_id: str,
    session_id: str,
    encounter_id: str | None,
    device_external_id: str,
) -> tuple[str, str | None]:
    cur.execute(
        """
        SELECT id::text
        FROM devices
        WHERE device_external_id = %s
        LIMIT 1
        """,
        (device_external_id,),
    )
    device_row = cur.fetchone()
    if not device_row:
        raise HTTPException(404, "Device not registered")

    (device_id,) = device_row

    cur.execute(
        """
        SELECT COALESCE(encounter_id, id)::text,
               device_id::text
        FROM user_sessions
        WHERE id = %s
          AND user_id = %s
        LIMIT 1
        """,
        (session_id, patient_user_id),
    )
    session_row = cur.fetchone()
    if not session_row:
        raise HTTPException(404, "Session not found")

    resolved_encounter_id, session_device_id = session_row
    if session_device_id and session_device_id != device_id:
        raise HTTPException(400, "session_id does not match device_external_id")
    if encounter_id and resolved_encounter_id and encounter_id != resolved_encounter_id:
        raise HTTPException(400, "encounter_id does not match session_id")

    return device_id, resolved_encounter_id or encounter_id


def _normalize_ts1_frame_payload(frame: Any, *, fallback_received_at: datetime | None) -> dict[str, Any]:
    raw_payload = frame if isinstance(frame, dict) else {}
    parsed_payload = raw_payload.get("parsed") if isinstance(raw_payload.get("parsed"), dict) else {}

    def _frame_value(key: str) -> Any:
        if key in raw_payload:
            return raw_payload.get(key)
        return parsed_payload.get(key)

    received_at = (
        _parse_datetime_like(_frame_value("received_at"))
        or _parse_datetime_like(_frame_value("ts"))
        or _parse_datetime_like(_frame_value("timestamp"))
        or fallback_received_at
    )
    timestamp_ms_value = _as_number(_frame_value("timestamp_ms"))
    timestamp_ms = int(timestamp_ms_value) if timestamp_ms_value is not None and math.isfinite(timestamp_ms_value) else None
    checksum_ok = _as_bool(_frame_value("checksum_ok"))
    raw_line = _coalesce_nonempty(
        raw_payload.get("raw_line"),
        raw_payload.get("raw"),
        raw_payload.get("line"),
        parsed_payload.get("raw_line"),
    )

    normalized_fields: dict[str, float] = {}
    for field_name in OPTICAL_TS1_SERIES_FIELDS:
        numeric_value = _as_number(_frame_value(field_name))
        if numeric_value is not None and math.isfinite(numeric_value):
            normalized_fields[field_name] = float(numeric_value)

    parsed_payload_json = parsed_payload if parsed_payload else {
        key: value
        for key, value in raw_payload.items()
        if key not in {"parsed", "raw_line", "raw", "line"}
    }
    parsed_payload_json = jsonable_encoder(parsed_payload_json or {}, exclude_none=True)
    if timestamp_ms is not None:
        parsed_payload_json.setdefault("timestamp_ms", timestamp_ms)
    if received_at is not None:
        parsed_payload_json.setdefault("received_at", _iso(received_at))
    if checksum_ok is not None:
        parsed_payload_json.setdefault("checksum_ok", checksum_ok)
    for field_name, numeric_value in normalized_fields.items():
        parsed_payload_json.setdefault(field_name, numeric_value)

    return {
        "received_at": received_at,
        "timestamp_ms": timestamp_ms,
        "raw_line": str(raw_line).strip() if isinstance(raw_line, str) and raw_line.strip() else None,
        "checksum_ok": checksum_ok,
        "parsed_payload_json": parsed_payload_json,
        **normalized_fields,
    }


def _extract_optical_packet_id(packet: dict[str, Any]) -> str | None:
    value = _coalesce_nonempty(
        packet.get("packet_id"),
        packet.get("packetId"),
        packet.get("id"),
        packet.get("seq"),
        packet.get("sequence"),
    )
    if value is None:
        return None
    return str(value)


def _optical_ppg_series_arrays(packet: dict[str, Any]) -> dict[str, list[Any]]:
    arrays: dict[str, list[Any]] = {}
    candidate_maps = [packet]
    for nested_key in ("channels", "values", "ppg"):
        nested = packet.get(nested_key)
        if isinstance(nested, dict):
            candidate_maps.append(nested)

    for series_name, aliases in OPTICAL_PPG_SERIES_ALIASES.items():
        for candidate in candidate_maps:
            for alias in aliases:
                values = candidate.get(alias)
                if isinstance(values, list):
                    arrays[series_name] = values
                    break
            if series_name in arrays:
                break

    return arrays


def _optical_ppg_value_from_mapping(mapping: Any, series_name: str) -> float | None:
    if not isinstance(mapping, dict):
        return None

    for alias in OPTICAL_PPG_SERIES_ALIASES.get(series_name, ()):
        numeric_value = _as_number(mapping.get(alias))
        if numeric_value is not None and math.isfinite(numeric_value):
            return float(numeric_value)

    for nested_key in ("values", "channels", "ppg"):
        nested = mapping.get(nested_key)
        if not isinstance(nested, dict):
            continue
        for alias in OPTICAL_PPG_SERIES_ALIASES.get(series_name, ()):
            numeric_value = _as_number(nested.get(alias))
            if numeric_value is not None and math.isfinite(numeric_value):
                return float(numeric_value)

    return None


def _estimate_optical_ppg_packet_sample_count(packet: Any) -> int:
    if not isinstance(packet, dict):
        return 0

    explicit_count = _as_number(
        _coalesce_nonempty(
            packet.get("sample_count"),
            packet.get("sampleCount"),
            packet.get("samples_per_packet"),
        )
    )
    if explicit_count is not None and explicit_count >= 0:
        return int(explicit_count)

    samples = packet.get("samples")
    if isinstance(samples, list):
        return len(samples)

    arrays = _optical_ppg_series_arrays(packet)
    if arrays:
        return max(len(values) for values in arrays.values())

    values = packet.get("values")
    if isinstance(values, list):
        return len(values)

    for series_name in OPTICAL_PPG_SERIES_KEYS:
        if _optical_ppg_value_from_mapping(packet, series_name) is not None:
            return 1

    return 0


def _optical_packet_base_datetime(
    packet: dict[str, Any],
    *,
    fallback_start: datetime | None,
) -> tuple[datetime | None, int | None]:
    for key in ("received_at", "packet_received_at", "started_at", "packet_started_at", "ts", "timestamp"):
        parsed = _parse_datetime_like(packet.get(key))
        if parsed is not None:
            return parsed, None

    for key in ("timestamp_ms", "packet_timestamp_ms", "base_timestamp_ms", "start_timestamp_ms", "ts_ms"):
        raw_timestamp = _as_number(packet.get(key))
        if raw_timestamp is None or not math.isfinite(raw_timestamp):
            continue
        timestamp_ms = int(raw_timestamp)
        parsed = _parse_epoch_datetime(timestamp_ms)
        if parsed is not None:
            return parsed, timestamp_ms
        return fallback_start, timestamp_ms

    return fallback_start, None


def _optical_packet_timestamp_list(packet: dict[str, Any]) -> list[Any] | None:
    for key in ("sample_timestamps_ms", "timestamps_ms", "sample_timestamps", "timestamps", "time_offsets_ms"):
        values = packet.get(key)
        if isinstance(values, list):
            return values
    return None


def _append_optical_waveform_point(
    target: list[dict[str, Any]],
    *,
    point_dt: datetime | None,
    value: float | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
    metadata: dict[str, Any],
) -> None:
    if point_dt is None or value is None:
        return
    if from_dt is not None and point_dt < from_dt:
        return
    if to_dt is not None and point_dt > to_dt:
        return

    target.append(
        {
            "_dt": point_dt,
            "ts": _iso(point_dt),
            "value": float(value),
            **metadata,
        }
    )


def _make_optical_waveform_gap(
    *,
    reason: str,
    start_ts: str | None,
    end_ts: str | None,
    gap_ms: float | None = None,
    expected_interval_ms: float | None = None,
    chunk_index: int | None = None,
    packet_loss_count_delta: int | None = None,
) -> dict[str, Any]:
    return {
        "kind": "gap",
        "reason": reason,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "gap_ms": round(float(gap_ms), 3) if gap_ms is not None else None,
        "expected_interval_ms": round(float(expected_interval_ms), 3) if expected_interval_ms is not None else None,
        "chunk_index": chunk_index,
        "packet_loss_count_delta": packet_loss_count_delta,
    }


def _extract_optical_ppg_points(
    chunk_rows: list[Any],
    *,
    sample_rate_hz: float | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    points_by_series: dict[str, list[dict[str, Any]]] = {series_name: [] for series_name in OPTICAL_PPG_SERIES_KEYS}
    extra_gaps: dict[str, list[dict[str, Any]]] = {series_name: [] for series_name in OPTICAL_PPG_SERIES_KEYS}

    for chunk_row in chunk_rows:
        chunk_index = int(chunk_row[0])
        chunk_started_at = chunk_row[1] or chunk_row[2]
        chunk_ended_at = chunk_row[2] or chunk_row[1]
        packet_loss_count_delta = int(chunk_row[7] or 0)
        payload_json = chunk_row[8] if isinstance(chunk_row[8], dict) else {}
        ppg_packets = payload_json.get("ppg_packets") if isinstance(payload_json.get("ppg_packets"), list) else []

        if packet_loss_count_delta > 0:
            gap_marker = _make_optical_waveform_gap(
                reason="packet_loss",
                start_ts=_iso(chunk_started_at),
                end_ts=_iso(chunk_ended_at),
                chunk_index=chunk_index,
                packet_loss_count_delta=packet_loss_count_delta,
            )
            for series_name in OPTICAL_PPG_SERIES_KEYS:
                extra_gaps[series_name].append(dict(gap_marker))

        chunk_sample_offset = 0
        for packet_offset, packet in enumerate(ppg_packets):
            if not isinstance(packet, dict):
                continue

            packet_sample_count = _estimate_optical_ppg_packet_sample_count(packet)
            fallback_packet_start = None
            if chunk_started_at is not None and sample_rate_hz and sample_rate_hz > 0:
                fallback_packet_start = chunk_started_at + timedelta(seconds=chunk_sample_offset / sample_rate_hz)
            else:
                fallback_packet_start = chunk_started_at

            packet_base_dt, packet_timestamp_ms = _optical_packet_base_datetime(
                packet,
                fallback_start=fallback_packet_start,
            )
            packet_id = _extract_optical_packet_id(packet)
            packet_sample_rate_hz = _as_number(
                _coalesce_nonempty(packet.get("sample_rate_hz"), packet.get("sampleRateHz"), sample_rate_hz)
            )
            sample_interval_ms = _as_number(
                _coalesce_nonempty(packet.get("sample_interval_ms"), packet.get("samplePeriodMs"))
            )
            if sample_interval_ms is None and packet_sample_rate_hz is not None and packet_sample_rate_hz > 0:
                sample_interval_ms = 1000.0 / packet_sample_rate_hz

            timestamp_list = _optical_packet_timestamp_list(packet)
            emitted_points = 0

            samples = packet.get("samples")
            if isinstance(samples, list):
                if samples and all(isinstance(sample, dict) for sample in samples):
                    for sample_index, sample in enumerate(samples):
                        sample_dt = (
                            _parse_datetime_like(sample.get("received_at"))
                            or _parse_datetime_like(sample.get("ts"))
                            or _parse_datetime_like(sample.get("timestamp"))
                        )
                        raw_sample_timestamp = _as_number(sample.get("timestamp_ms"))
                        if sample_dt is None and raw_sample_timestamp is not None:
                            sample_dt = _parse_epoch_datetime(raw_sample_timestamp) or packet_base_dt
                        if sample_dt is None and timestamp_list and sample_index < len(timestamp_list):
                            raw_list_timestamp = _as_number(timestamp_list[sample_index])
                            if raw_list_timestamp is not None:
                                sample_dt = _parse_epoch_datetime(raw_list_timestamp) or (
                                    packet_base_dt + timedelta(milliseconds=sample_index * sample_interval_ms)
                                    if packet_base_dt is not None and sample_interval_ms is not None
                                    else packet_base_dt
                                )
                        if sample_dt is None and packet_base_dt is not None and sample_interval_ms is not None:
                            sample_dt = packet_base_dt + timedelta(milliseconds=sample_index * sample_interval_ms)

                        timestamp_ms = None
                        if raw_sample_timestamp is not None and math.isfinite(raw_sample_timestamp):
                            timestamp_ms = int(raw_sample_timestamp)
                        elif packet_timestamp_ms is not None and sample_interval_ms is not None:
                            timestamp_ms = int(packet_timestamp_ms + (sample_index * sample_interval_ms))

                        for series_name in OPTICAL_PPG_SERIES_KEYS:
                            value = _optical_ppg_value_from_mapping(sample, series_name)
                            if value is None:
                                continue
                            _append_optical_waveform_point(
                                points_by_series[series_name],
                                point_dt=sample_dt,
                                value=value,
                                from_dt=from_dt,
                                to_dt=to_dt,
                                metadata={
                                    "series": series_name,
                                    "chunk_index": chunk_index,
                                    "packet_index": packet_offset,
                                    "packet_id": packet_id,
                                    "sample_index": sample_index,
                                    "timestamp_ms": timestamp_ms,
                                },
                            )
                            emitted_points += 1
                else:
                    channel_name = str(packet.get("channel") or packet.get("series") or "").strip().lower()
                    series_name = None
                    for candidate_name, aliases in OPTICAL_PPG_SERIES_ALIASES.items():
                        if channel_name in aliases or channel_name == candidate_name:
                            series_name = candidate_name
                            break
                    if series_name:
                        for sample_index, sample in enumerate(samples):
                            numeric_value = _as_number(sample)
                            if numeric_value is None or not math.isfinite(numeric_value):
                                continue
                            sample_dt = packet_base_dt
                            if sample_dt is not None and sample_interval_ms is not None:
                                sample_dt = packet_base_dt + timedelta(milliseconds=sample_index * sample_interval_ms)
                            timestamp_ms = (
                                int(packet_timestamp_ms + (sample_index * sample_interval_ms))
                                if packet_timestamp_ms is not None and sample_interval_ms is not None
                                else packet_timestamp_ms
                            )
                            _append_optical_waveform_point(
                                points_by_series[series_name],
                                point_dt=sample_dt,
                                value=float(numeric_value),
                                from_dt=from_dt,
                                to_dt=to_dt,
                                metadata={
                                    "series": series_name,
                                    "chunk_index": chunk_index,
                                    "packet_index": packet_offset,
                                    "packet_id": packet_id,
                                    "sample_index": sample_index,
                                    "timestamp_ms": timestamp_ms,
                                },
                            )
                            emitted_points += 1

            arrays = _optical_ppg_series_arrays(packet)
            if emitted_points == 0 and arrays:
                array_length = max(len(values) for values in arrays.values())
                for sample_index in range(array_length):
                    sample_dt = None
                    raw_list_timestamp_value = None
                    if timestamp_list and sample_index < len(timestamp_list):
                        raw_list_timestamp_value = _as_number(timestamp_list[sample_index])
                        if raw_list_timestamp_value is not None:
                            sample_dt = _parse_epoch_datetime(raw_list_timestamp_value) or packet_base_dt
                    if sample_dt is None and packet_base_dt is not None and sample_interval_ms is not None:
                        sample_dt = packet_base_dt + timedelta(milliseconds=sample_index * sample_interval_ms)
                    if sample_dt is None:
                        sample_dt = packet_base_dt

                    for series_name, values in arrays.items():
                        if sample_index >= len(values):
                            continue
                        numeric_value = _as_number(values[sample_index])
                        if numeric_value is None or not math.isfinite(numeric_value):
                            continue
                        timestamp_ms = None
                        if raw_list_timestamp_value is not None and math.isfinite(raw_list_timestamp_value):
                            timestamp_ms = int(raw_list_timestamp_value)
                        elif packet_timestamp_ms is not None and sample_interval_ms is not None:
                            timestamp_ms = int(packet_timestamp_ms + (sample_index * sample_interval_ms))
                        _append_optical_waveform_point(
                            points_by_series[series_name],
                            point_dt=sample_dt,
                            value=float(numeric_value),
                            from_dt=from_dt,
                            to_dt=to_dt,
                            metadata={
                                "series": series_name,
                                "chunk_index": chunk_index,
                                "packet_index": packet_offset,
                                "packet_id": packet_id,
                                "sample_index": sample_index,
                                "timestamp_ms": timestamp_ms,
                            },
                        )
                        emitted_points += 1

            if emitted_points == 0:
                for series_name in OPTICAL_PPG_SERIES_KEYS:
                    numeric_value = _optical_ppg_value_from_mapping(packet, series_name)
                    if numeric_value is None:
                        continue
                    _append_optical_waveform_point(
                        points_by_series[series_name],
                        point_dt=packet_base_dt,
                        value=numeric_value,
                        from_dt=from_dt,
                        to_dt=to_dt,
                        metadata={
                            "series": series_name,
                            "chunk_index": chunk_index,
                            "packet_index": packet_offset,
                            "packet_id": packet_id,
                            "sample_index": 0,
                            "timestamp_ms": packet_timestamp_ms,
                        },
                    )
                    emitted_points += 1

            chunk_sample_offset += max(packet_sample_count, emitted_points, 0)

    for series_name in OPTICAL_PPG_SERIES_KEYS:
        points_by_series[series_name].sort(key=lambda point: (point["_dt"], point.get("chunk_index") or 0, point.get("sample_index") or 0))

    return points_by_series, extra_gaps


def _strip_optical_waveform_points(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: value for key, value in point.items() if key != "_dt"} for point in points]


def _estimate_optical_interval_ms(points: list[dict[str, Any]], fallback_ms: float | None = None) -> float | None:
    deltas: list[float] = []
    previous_dt: datetime | None = None
    for point in points:
        point_dt = point.get("_dt")
        if not isinstance(point_dt, datetime):
            continue
        if previous_dt is not None:
            delta_ms = (point_dt - previous_dt).total_seconds() * 1000.0
            if delta_ms > 0:
                deltas.append(delta_ms)
        previous_dt = point_dt

    if deltas:
        return float(statistics.median(deltas))
    return fallback_ms


def _detect_optical_waveform_gaps(points: list[dict[str, Any]], *, expected_interval_ms: float | None) -> list[dict[str, Any]]:
    if expected_interval_ms is None or expected_interval_ms <= 0:
        return []

    gaps: list[dict[str, Any]] = []
    previous_point: dict[str, Any] | None = None
    for point in points:
        point_dt = point.get("_dt")
        if not isinstance(point_dt, datetime):
            continue
        if previous_point is not None:
            previous_dt = previous_point.get("_dt")
            if isinstance(previous_dt, datetime):
                delta_ms = (point_dt - previous_dt).total_seconds() * 1000.0
                if delta_ms > expected_interval_ms * 1.5:
                    gaps.append(
                        _make_optical_waveform_gap(
                            reason="time_gap",
                            start_ts=previous_point.get("ts"),
                            end_ts=point.get("ts"),
                            gap_ms=delta_ms,
                            expected_interval_ms=expected_interval_ms,
                        )
                    )
        previous_point = point
    return gaps


def _optical_point_identity(point: dict[str, Any]) -> tuple[Any, ...]:
    return (
        point.get("ts"),
        point.get("value"),
        point.get("chunk_index"),
        point.get("packet_index"),
        point.get("packet_id"),
        point.get("sample_index"),
        point.get("frame_index"),
    )


def _optical_gap_identity(gap: dict[str, Any]) -> tuple[Any, ...]:
    return (
        gap.get("kind"),
        gap.get("reason"),
        gap.get("start_ts"),
        gap.get("end_ts"),
        gap.get("gap_ms"),
        gap.get("expected_interval_ms"),
        gap.get("chunk_index"),
        gap.get("packet_loss_count_delta"),
    )


def _downsample_optical_points_minmax(points: list[dict[str, Any]], max_points: int) -> tuple[list[dict[str, Any]], bool]:
    if len(points) <= max_points or max_points < 3:
        return points, False

    first_point = points[0]
    last_point = points[-1]
    interior = points[1:-1]
    remaining_budget = max_points - 2
    if remaining_budget <= 0:
        return [first_point, last_point][:max_points], True

    bucket_count = max(1, math.ceil(len(interior) / max(1, remaining_budget // 2 or 1)))
    bucket_size = max(1, math.ceil(len(interior) / bucket_count))
    downsampled: list[dict[str, Any]] = [first_point]
    seen: set[tuple[Any, ...]] = {_optical_point_identity(first_point)}

    for start in range(0, len(interior), bucket_size):
        if remaining_budget <= 0:
            break
        bucket = interior[start:start + bucket_size]
        if not bucket:
            continue

        if len(bucket) == 1 or remaining_budget == 1:
            candidate = bucket[len(bucket) // 2]
            identity = _optical_point_identity(candidate)
            if identity not in seen:
                downsampled.append(candidate)
                seen.add(identity)
                remaining_budget -= 1
            continue

        min_point = min(bucket, key=lambda point: point["value"])
        max_point = max(bucket, key=lambda point: point["value"])
        ordered_candidates = [min_point, max_point]
        ordered_candidates.sort(key=lambda point: point["_dt"])

        for candidate in ordered_candidates:
            if remaining_budget <= 0:
                break
            identity = _optical_point_identity(candidate)
            if identity in seen:
                continue
            downsampled.append(candidate)
            seen.add(identity)
            remaining_budget -= 1

    last_identity = _optical_point_identity(last_point)
    if last_identity not in seen:
        if len(downsampled) >= max_points:
            downsampled[-1] = last_point
        else:
            downsampled.append(last_point)

    return downsampled[:max_points], True


def _parse_optical_resolution_ms(value: str | None) -> float | None:
    if value is None:
        return None
    token = value.strip().lower()
    if not token or token == "raw":
        return None

    try:
        if token.endswith("ms"):
            return float(token[:-2])
        if token.endswith("s"):
            return float(token[:-1]) * 1000.0
        if token.endswith("m"):
            return float(token[:-1]) * 60_000.0
        return float(token)
    except ValueError:
        raise HTTPException(400, "resolution must be numeric or use units like 250ms or 1s")


def _parse_optical_series_query(value: str | None) -> list[str]:
    if value is None or not value.strip():
        return []

    result: list[str] = []
    seen: set[str] = set()
    for token in value.split(","):
        normalized = token.strip().lower()
        if not normalized or normalized in seen:
            continue
        if normalized in OPTICAL_PPG_SERIES_KEYS:
            result.append(normalized)
            seen.add(normalized)
            continue
        if normalized.startswith("ts1_") and normalized[4:] in OPTICAL_TS1_SERIES_FIELDS:
            result.append(normalized)
            seen.add(normalized)
            continue
        raise HTTPException(400, f"Unsupported series '{normalized}'")
    return result


def _compute_optical_waveform_delta(previous_payload: dict[str, Any], current_payload: dict[str, Any]) -> dict[str, Any]:
    previous_series_by_name = {
        item.get("series"): item
        for item in previous_payload.get("series") or []
        if isinstance(item, dict) and isinstance(item.get("series"), str)
    }

    delta_series: list[dict[str, Any]] = []
    for current_series in current_payload.get("series") or []:
        if not isinstance(current_series, dict):
            continue
        series_name = current_series.get("series")
        if not isinstance(series_name, str):
            continue

        previous_series = previous_series_by_name.get(series_name) or {}
        previous_point_ids = {
            _optical_point_identity(point)
            for point in previous_series.get("points") or []
            if isinstance(point, dict)
        }
        previous_gap_ids = {
            _optical_gap_identity(gap)
            for gap in previous_series.get("gaps") or []
            if isinstance(gap, dict)
        }

        new_points = [
            point
            for point in current_series.get("points") or []
            if isinstance(point, dict) and _optical_point_identity(point) not in previous_point_ids
        ]
        new_gaps = [
            gap
            for gap in current_series.get("gaps") or []
            if isinstance(gap, dict) and _optical_gap_identity(gap) not in previous_gap_ids
        ]

        if new_points or new_gaps:
            delta_series.append(
                {
                    "series": series_name,
                    "kind": current_series.get("kind"),
                    "label": current_series.get("label"),
                    "unit": current_series.get("unit"),
                    "points": new_points,
                    "gaps": new_gaps,
                    "meta": current_series.get("meta") or {},
                }
            )

    return {
        "measurement_run_id": current_payload.get("measurement_run_id"),
        "run": current_payload.get("run"),
        "filters": current_payload.get("filters") or {},
        "available_series": current_payload.get("available_series") or [],
        "series": delta_series,
        "delta": True,
    }


def _build_optical_waveform_payload(
    cur,
    *,
    measurement_run_id: str,
    from_dt: datetime | None,
    to_dt: datetime | None,
    max_points: int,
    requested_series: list[str],
    resolution_ms: float | None,
) -> dict[str, Any]:
    run_row = _fetch_optical_capture_run_row(cur, measurement_run_id)
    if not run_row:
        raise HTTPException(404, "Optical capture run not found")
    run_payload = _serialize_optical_capture_run_row(run_row)

    cur.execute(
        """
        SELECT chunk_index,
               chunk_started_at,
               chunk_ended_at,
               packet_count,
               sample_count,
               first_packet_id,
               last_packet_id,
               packet_loss_count_delta,
               payload_json
        FROM optical_capture_chunks
        WHERE measurement_run_id = %s
          AND (%s::timestamptz IS NULL OR COALESCE(chunk_ended_at, chunk_started_at) >= %s::timestamptz)
          AND (%s::timestamptz IS NULL OR COALESCE(chunk_started_at, chunk_ended_at) <= %s::timestamptz)
        ORDER BY chunk_started_at ASC NULLS FIRST, chunk_index ASC
        """,
        (measurement_run_id, from_dt, from_dt, to_dt, to_dt),
    )
    chunk_rows = cur.fetchall()
    chunk_started_by_index = {
        int(row[0]): (row[1] or row[2] or run_payload.get("started_at"))
        for row in chunk_rows
    }

    ppg_points_by_series, ppg_extra_gaps = _extract_optical_ppg_points(
        chunk_rows,
        sample_rate_hz=run_payload.get("sample_rate_hz"),
        from_dt=from_dt,
        to_dt=to_dt,
    )

    cur.execute(
        """
        SELECT chunk_index,
               frame_index,
               received_at,
               timestamp_ms,
               raw_line,
               checksum_ok,
               sht_temp_c,
               sht_rh_percent,
               ens_iaq,
               ens_eco2_ppm,
               ens_temp_c,
               mlx_ambient_c,
               mlx_object_c,
               heart_rate_bpm,
               spo2_percent,
               red_dc,
               quality,
               parsed_payload_json
        FROM optical_capture_ts1_frames
        WHERE measurement_run_id = %s
        ORDER BY received_at ASC NULLS LAST, chunk_index ASC, frame_index ASC
        """,
        (measurement_run_id,),
    )
    ts1_rows = cur.fetchall()

    ts1_points_by_field: dict[str, list[dict[str, Any]]] = {field_name: [] for field_name in OPTICAL_TS1_SERIES_FIELDS}
    for row in ts1_rows:
        chunk_index = int(row[0])
        frame_index = int(row[1])
        received_at = row[2] if isinstance(row[2], datetime) else None
        timestamp_ms = int(row[3]) if row[3] is not None else None
        point_dt = received_at or _parse_epoch_datetime(timestamp_ms) or chunk_started_by_index.get(chunk_index)
        if point_dt is None:
            continue
        if from_dt is not None and point_dt < from_dt:
            continue
        if to_dt is not None and point_dt > to_dt:
            continue

        field_values = {
            "sht_temp_c": _as_number(row[6]),
            "sht_rh_percent": _as_number(row[7]),
            "ens_iaq": _as_number(row[8]),
            "ens_eco2_ppm": _as_number(row[9]),
            "ens_temp_c": _as_number(row[10]),
            "mlx_ambient_c": _as_number(row[11]),
            "mlx_object_c": _as_number(row[12]),
            "heart_rate_bpm": _as_number(row[13]),
            "spo2_percent": _as_number(row[14]),
            "red_dc": _as_number(row[15]),
            "quality": _as_number(row[16]),
        }
        for field_name, numeric_value in field_values.items():
            if numeric_value is None or not math.isfinite(numeric_value):
                continue
            ts1_points_by_field[field_name].append(
                {
                    "_dt": point_dt,
                    "ts": _iso(point_dt),
                    "value": float(numeric_value),
                    "series": f"ts1_{field_name}",
                    "chunk_index": chunk_index,
                    "frame_index": frame_index,
                    "timestamp_ms": timestamp_ms,
                    "checksum_ok": row[5],
                    "raw_line": row[4],
                }
            )

    for field_name in OPTICAL_TS1_SERIES_FIELDS:
        ts1_points_by_field[field_name].sort(key=lambda point: (point["_dt"], point.get("chunk_index") or 0, point.get("frame_index") or 0))

    available_series: list[str] = []
    for series_name in OPTICAL_PPG_SERIES_KEYS:
        if ppg_points_by_series.get(series_name):
            available_series.append(series_name)
    for field_name in OPTICAL_TS1_SERIES_FIELDS:
        if ts1_points_by_field.get(field_name):
            available_series.append(f"ts1_{field_name}")

    selected_series = requested_series or available_series
    waveform_series: list[dict[str, Any]] = []

    for series_name in selected_series:
        if series_name in OPTICAL_PPG_SERIES_KEYS:
            original_points = ppg_points_by_series.get(series_name) or []
            if not original_points:
                waveform_series.append(
                    {
                        "series": series_name,
                        "kind": "ppg",
                        "label": "PPG IR" if series_name == "ppg_ir" else "PPG Red",
                        "points": [],
                        "gaps": list(ppg_extra_gaps.get(series_name) or []),
                        "meta": {
                            "original_point_count": 0,
                            "returned_point_count": 0,
                            "downsampled": False,
                            "sample_rate_hz": run_payload.get("sample_rate_hz"),
                        },
                    }
                )
                continue

            points_for_response = list(original_points)
            if resolution_ms is not None and resolution_ms > 0:
                points_for_response, _ = _downsample_optical_points_minmax(points_for_response, max_points)
            else:
                points_for_response, _ = _downsample_optical_points_minmax(points_for_response, max_points)
            expected_interval_ms = _estimate_optical_interval_ms(
                original_points,
                fallback_ms=(1000.0 / run_payload["sample_rate_hz"]) if run_payload.get("sample_rate_hz") else None,
            )
            gaps = _detect_optical_waveform_gaps(original_points, expected_interval_ms=expected_interval_ms)
            gaps.extend(list(ppg_extra_gaps.get(series_name) or []))
            waveform_series.append(
                {
                    "series": series_name,
                    "kind": "ppg",
                    "label": "PPG IR" if series_name == "ppg_ir" else "PPG Red",
                    "points": _strip_optical_waveform_points(points_for_response),
                    "gaps": gaps,
                    "meta": {
                        "original_point_count": len(original_points),
                        "returned_point_count": len(points_for_response),
                        "downsampled": len(points_for_response) < len(original_points),
                        "sample_rate_hz": run_payload.get("sample_rate_hz"),
                    },
                }
            )
            continue

        field_name = series_name[4:]
        original_points = ts1_points_by_field.get(field_name) or []
        returned_points = list(original_points)
        if len(returned_points) > max_points:
            stride = max(1, math.ceil(len(returned_points) / max_points))
            returned_points = returned_points[::stride][:max_points]
        expected_interval_ms = _estimate_optical_interval_ms(original_points)
        waveform_series.append(
            {
                "series": series_name,
                "kind": "ts1",
                "label": OPTICAL_TS1_SERIES_LABELS.get(series_name, series_name),
                "points": _strip_optical_waveform_points(returned_points),
                "gaps": _detect_optical_waveform_gaps(original_points, expected_interval_ms=expected_interval_ms),
                "meta": {
                    "original_point_count": len(original_points),
                    "returned_point_count": len(returned_points),
                    "downsampled": len(returned_points) < len(original_points),
                },
            }
        )

    return {
        "measurement_run_id": measurement_run_id,
        "run": run_payload,
        "filters": {
            "from_ts": _iso(from_dt),
            "to_ts": _iso(to_dt),
            "max_points": max_points,
            "series": selected_series,
            "resolution_ms": resolution_ms,
        },
        "available_series": available_series,
        "series": waveform_series,
    }


def _build_doctor_encounter_summaries(
    measurement_sessions: list[dict[str, Any]],
    image_history_items: list[dict[str, Any]],
    note_items: list[dict[str, Any]],
    tag_items: list[dict[str, Any]],
    report_items: list[dict[str, Any]],
    message_items: list[dict[str, Any]],
    clinical_summaries: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    min_dt = datetime.min.replace(tzinfo=timezone.utc)

    def ensure(encounter_id: str) -> dict[str, Any]:
        item = summaries.get(encounter_id)
        if item is not None:
            return item
        item = {
            "encounter_id": encounter_id,
            "started_at": None,
            "ended_at": None,
            "updated_at": None,
            "revision": 0,
            "etag": None,
            "measurement_count": 0,
            "image_count": 0,
            "note_count": 0,
            "tag_count": 0,
            "measurement_ids": [],
            "latest_measurement_id": None,
            "latest_measurement_label": None,
            "latest_measurement_mode": None,
            "latest_source": None,
            "device_name": None,
            "location_label": None,
            "chat_summary": None,
            "reason_for_consult": None,
            "patient_reported_symptoms": [],
            "clinician_assessment": None,
            "disposition": None,
            "follow_up_plan": None,
            "tags": [],
            "measurement_modes": [],
            "linked_measurement_run_ids": [],
            "waveform_measurement_run_ids": [],
            "has_waveform_measurements": False,
            "linked_image_ids": [],
            "linked_report_ids": [],
            "linked_message_ids": [],
            "_started_dt": None,
            "_ended_dt": None,
            "_latest_dt": None,
        }
        summaries[encounter_id] = item
        return item

    def touch(item: dict[str, Any], started: datetime | None = None, ended: datetime | None = None) -> None:
        if started is not None:
            if item["_started_dt"] is None or started < item["_started_dt"]:
                item["_started_dt"] = started
        if ended is not None:
            if item["_ended_dt"] is None or ended > item["_ended_dt"]:
                item["_ended_dt"] = ended

    for session in measurement_sessions:
        encounter_id = session.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        started_dt = _parse_iso_datetime(session.get("started_at"))
        ended_dt = _parse_iso_datetime(session.get("ended_at")) or started_dt
        touch(item, started_dt, ended_dt)
        item["measurement_count"] += 1

        measurement_id = session.get("measurement_id")
        if isinstance(measurement_id, str) and measurement_id and measurement_id not in item["measurement_ids"]:
            item["measurement_ids"].append(measurement_id)
        measurement_mode = session.get("measurement_mode")
        if isinstance(measurement_mode, str) and measurement_mode and measurement_mode not in item["measurement_modes"]:
            item["measurement_modes"].append(measurement_mode)
        measurement_run_id = session.get("measurement_run_id")
        if isinstance(measurement_run_id, str) and measurement_run_id and measurement_run_id not in item["linked_measurement_run_ids"]:
            item["linked_measurement_run_ids"].append(measurement_run_id)
            if (
                session.get("raw_waveform_available")
                or isinstance(session.get("waveform_api_path"), str)
                or isinstance(session.get("waveform_stream_path"), str)
            ) and measurement_run_id not in item["waveform_measurement_run_ids"]:
                item["waveform_measurement_run_ids"].append(measurement_run_id)
                item["has_waveform_measurements"] = True
        for run_id in session.get("measurement_run_ids") or []:
            if isinstance(run_id, str) and run_id and run_id not in item["linked_measurement_run_ids"]:
                item["linked_measurement_run_ids"].append(run_id)
                if (
                    session.get("raw_waveform_available")
                    or isinstance(session.get("waveform_api_path"), str)
                    or isinstance(session.get("waveform_stream_path"), str)
                ) and run_id not in item["waveform_measurement_run_ids"]:
                    item["waveform_measurement_run_ids"].append(run_id)
                    item["has_waveform_measurements"] = True

        latest_dt = ended_dt or started_dt
        if item["_latest_dt"] is None or (latest_dt is not None and latest_dt >= item["_latest_dt"]):
            item["_latest_dt"] = latest_dt
            item["latest_measurement_id"] = measurement_id if isinstance(measurement_id, str) else None
            item["latest_measurement_label"] = session.get("measurement_label") if isinstance(session.get("measurement_label"), str) else None
            item["latest_measurement_mode"] = measurement_mode if isinstance(measurement_mode, str) else None
            item["latest_source"] = session.get("source") if isinstance(session.get("source"), str) else None
            item["device_name"] = session.get("device_name") if isinstance(session.get("device_name"), str) else None
            item["location_label"] = session.get("location_label") if isinstance(session.get("location_label"), str) else None
            chat_result = session.get("chat_result") if isinstance(session.get("chat_result"), dict) else {}
            chat_summary = chat_result.get("assistant_summary") if isinstance(chat_result, dict) else None
            item["chat_summary"] = chat_summary if isinstance(chat_summary, str) and chat_summary.strip() else None

    for image_item in image_history_items:
        encounter_id = image_item.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        analyzed_dt = _parse_iso_datetime(image_item.get("analyzed_at"))
        created_dt = _parse_iso_datetime(image_item.get("created_at"))
        touch(item, analyzed_dt or created_dt, analyzed_dt or created_dt)
        item["image_count"] += 1
        for image_id in (image_item.get("source_image_id"), image_item.get("overlay_image_id")):
            if isinstance(image_id, int) and image_id not in item["linked_image_ids"]:
                item["linked_image_ids"].append(image_id)

    for note in note_items:
        encounter_id = note.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        note_dt = note.get("updated_at") if isinstance(note.get("updated_at"), datetime) else note.get("created_at")
        if isinstance(note_dt, datetime):
            touch(item, note_dt, note_dt)
        item["note_count"] += 1

    for tag in tag_items:
        encounter_id = tag.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        created_at = tag.get("created_at")
        if isinstance(created_at, datetime):
            touch(item, created_at, created_at)
        item["tag_count"] += 1
        tag_value = tag.get("tag")
        if isinstance(tag_value, str) and tag_value and tag_value not in item["tags"]:
            item["tags"].append(tag_value)

    for report in report_items:
        encounter_id = report.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        created_at = report.get("created_at")
        if isinstance(created_at, datetime):
            touch(item, created_at, created_at)
        report_id = report.get("report_id")
        if isinstance(report_id, str) and report_id and report_id not in item["linked_report_ids"]:
            item["linked_report_ids"].append(report_id)

    for message in message_items:
        encounter_id = message.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        created_at = message.get("created_at")
        if isinstance(created_at, datetime):
            touch(item, created_at, created_at)
        message_id = message.get("message_id")
        if isinstance(message_id, str) and message_id and message_id not in item["linked_message_ids"]:
            item["linked_message_ids"].append(message_id)

    for encounter_id, summary in clinical_summaries.items():
        if not isinstance(encounter_id, str) or not encounter_id:
            continue
        item = ensure(encounter_id)
        item["reason_for_consult"] = summary.get("reason_for_consult")
        item["patient_reported_symptoms"] = list(summary.get("patient_reported_symptoms") or [])
        item["clinician_assessment"] = summary.get("clinician_assessment")
        item["disposition"] = summary.get("disposition")
        item["follow_up_plan"] = summary.get("follow_up_plan")
        item["revision"] = int(summary.get("revision") or 0)
        item["etag"] = summary.get("etag")
        updated_at = summary.get("updated_at")
        if isinstance(updated_at, datetime):
            item["updated_at"] = updated_at
            touch(item, updated_at, updated_at)

    result: list[dict[str, Any]] = []
    for item in summaries.values():
        item["started_at"] = _iso(item.get("_started_dt"))
        item["ended_at"] = _iso(item.get("_ended_dt"))
        item["_sort_dt"] = item.get("_ended_dt") or item.get("_started_dt") or min_dt
        item.pop("_started_dt", None)
        item.pop("_ended_dt", None)
        item.pop("_latest_dt", None)
        result.append(item)

    result.sort(
        key=lambda item: (
            item.get("_sort_dt") or min_dt,
            item.get("started_at") or "",
            item.get("encounter_id") or "",
        ),
        reverse=True,
    )
    for item in result:
        item.pop("_sort_dt", None)
    return result

def build_measurement_history(user_id: str, limit: int, *, include_messages: bool = True) -> dict[str, Any]:
    try:
        validated_user_id = str(uuid.UUID(user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    canonical_measurement_ids = (
        "temperature",
        "ambient_temperature",
        "spo2",
        "heart_rate",
        "humidity",
        "co2",
    )
    measurement_summary_keys = {
        "temperature": "skin_temperature_c",
        "ambient_temperature": "ambient_temperature_c",
        "spo2": "spo2_percent",
        "heart_rate": "heart_rate_bpm",
        "humidity": "humidity_percent",
        "co2": "co2_ppm",
    }
    measurement_id_aliases = {
        "temperature": "temperature",
        "temp": "temperature",
        "skin_temperature": "temperature",
        "skin_temp": "temperature",
        "body_temp": "temperature",
        "ambient_temperature": "ambient_temperature",
        "ambient_temp": "ambient_temperature",
        "room_temperature": "ambient_temperature",
        "spo2": "spo2",
        "pulse_ox": "spo2",
        "pulse_oxygen": "spo2",
        "oxygen": "spo2",
        "o2": "spo2",
        "heart_rate": "heart_rate",
        "heart-rate": "heart_rate",
        "hr": "heart_rate",
        "pulse": "heart_rate",
        "humidity": "humidity",
        "humid": "humidity",
        "relative_humidity": "humidity",
        "rh": "humidity",
        "co2": "co2",
        "co2_ppm": "co2",
        "eco2": "co2",
        "eco2_ppm": "co2",
        "ppm": "co2",
    }

    def _coerce_bool(value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            token = value.strip().lower()
            if token in {"true", "1", "yes", "y", "on"}:
                return True
            if token in {"false", "0", "no", "n", "off"}:
                return False
        return None

    def _canonical_measurement_id(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        token = value.strip().lower().replace(" ", "_")
        if not token:
            return None
        return measurement_id_aliases.get(token)

    def _extract_explicit_measurement_id(payload: dict[str, Any]) -> str | None:
        summary_map = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        explicit_value = _coalesce_nonempty(
            payload.get("measurement_id"),
            summary_map.get("measurement_id"),
            payload.get("selected_measurement_id"),
            payload.get("selected_measurement"),
        )
        return _canonical_measurement_id(explicit_value)

    def _classify_sensor_source(payload: dict[str, Any], measurement_run_id: str | None) -> tuple[str, bool]:
        summary_map = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        source_value = _coalesce_nonempty(
            payload.get("source"),
            summary_map.get("source"),
            payload.get("event_source"),
            payload.get("capture_source"),
            payload.get("origin"),
        )
        source_token = source_value.strip().lower() if isinstance(source_value, str) else None
        user_selected = _coerce_bool(payload.get("is_user_selected"))
        if user_selected is None:
            user_selected = _coerce_bool(payload.get("user_selected"))
        if user_selected is None:
            user_selected = _coerce_bool(payload.get("selected"))

        if source_token in {"background_telemetry", "telemetry", "background", "ambient_background"}:
            user_selected = False
        elif source_token in {"measurement_capture", "user_measurement", "capture", "user_selected"}:
            user_selected = True

        if user_selected is None:
            user_selected = measurement_run_id is not None

        source = "measurement_capture" if user_selected else "background_telemetry"
        return source, user_selected

    def _event_measurement_ids(
        raw_kind: str | None,
        payload: dict[str, Any],
        explicit_measurement_id: str | None,
    ) -> list[str]:
        if explicit_measurement_id:
            return [explicit_measurement_id]

        kind_measurement_id = _canonical_measurement_id(raw_kind)
        if kind_measurement_id:
            return [kind_measurement_id]

        inferred: list[str] = []
        for measurement_id in canonical_measurement_ids:
            spec = TREND_SERIES_SPECS.get(measurement_id)
            if not isinstance(spec, dict):
                continue
            value = _extract_trend_value(raw_kind, payload, spec)
            if value is not None:
                inferred.append(measurement_id)
        return inferred

    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id=%s LIMIT 1;", (validated_user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "User not found")

        cur.execute(
            """
            SELECT us.id::text,
                   COALESCE(us.encounter_id, us.id)::text,
                   us.started_at,
                   us.ended_at,
                   us.meta,
                   d.device_external_id
            FROM user_sessions us
            LEFT JOIN devices d ON d.id = us.device_id
            WHERE us.user_id = %s
            ORDER BY us.started_at ASC, us.id ASC
            """,
            (validated_user_id,),
        )
        session_rows = cur.fetchall()

        cur.execute(
            """
            SELECT sm.session_id::text,
                   COALESCE(us.encounter_id, us.id)::text,
                   sm.role,
                   sm.content,
                   sm.ts,
                   sm.meta
            FROM session_messages sm
            JOIN user_sessions us ON us.id = sm.session_id
            WHERE us.user_id = %s
            ORDER BY sm.ts ASC, sm.id ASC
            """,
            (validated_user_id,),
        )
        message_rows = cur.fetchall()

        cur.execute(
            """
            SELECT sem.session_id::text,
                   COALESCE(us.encounter_id, us.id)::text,
                   sem.ts,
                   sem.emotions
            FROM session_emotions sem
            JOIN user_sessions us ON us.id = sem.session_id
            WHERE us.user_id = %s
            ORDER BY sem.ts ASC, sem.id ASC
            """,
            (validated_user_id,),
        )
        emotion_rows = cur.fetchall()

        cur.execute(
            """
            SELECT se.id,
                   COALESCE(se.encounter_id, us.encounter_id, us.id)::text,
                   se.session_id::text,
                   se.measurement_run_id::text,
                   se.ts,
                   se.kind,
                   se.seq,
                   se.data,
                   d.device_external_id
            FROM sensor_events se
            LEFT JOIN user_sessions us ON us.id = se.session_id
            LEFT JOIN devices d ON d.id = se.device_id
            WHERE (
                us.user_id = %s
                OR (
                    se.session_id IS NULL
                    AND se.encounter_id IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM user_sessions ux
                        WHERE ux.user_id = %s
                          AND ux.encounter_id = se.encounter_id
                    )
                )
            )
            ORDER BY se.ts ASC, se.id ASC
            """,
            (validated_user_id, validated_user_id),
        )
        sensor_rows = cur.fetchall()

        cur.execute(
            """
            SELECT ia.id,
                   ia.measurement_run_id::text,
                   COALESCE(ia.session_id::text, i.session_id::text),
                   COALESCE(ia.encounter_id::text, us.encounter_id::text, us.id::text),
                   ia.non_diagnostic,
                   ia.analysis,
                   ia.analyzed_at,
                   d.device_external_id
            FROM image_analyses ia
            JOIN images i ON i.id = ia.source_image_id
            LEFT JOIN user_sessions us ON us.id = COALESCE(ia.session_id, i.session_id)
            LEFT JOIN devices d ON d.id = COALESCE(ia.device_id, i.device_id)
            WHERE (
                COALESCE(ia.user_id, us.user_id, d.user_id) = %s
                OR (
                    COALESCE(ia.user_id, us.user_id, d.user_id) IS NULL
                    AND ia.encounter_id IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM user_sessions ux
                        WHERE ux.user_id = %s
                          AND ux.encounter_id = ia.encounter_id
                    )
                )
            )
            ORDER BY ia.analyzed_at ASC, ia.id ASC
            """,
            (validated_user_id, validated_user_id),
        )
        image_analysis_rows = cur.fetchall()

        cur.execute(
            """
            SELECT sa.measurement_run_id::text,
                   sa.session_id::text,
                   sa.encounter_id::text,
                   sa.analyzer,
                   sa.scope,
                   sa.status,
                   sa.score,
                   sa.confidence,
                   sa.summary,
                   sa.event_count,
                   sa.findings,
                   sa.latest_values,
                   sa.features,
                   sa.model,
                   sa.analyzed_at
            FROM sensor_analysis sa
            WHERE EXISTS (
                SELECT 1
                FROM user_sessions us
                WHERE us.user_id = %s
                  AND (
                      us.id = sa.session_id
                      OR (sa.encounter_id IS NOT NULL AND us.encounter_id = sa.encounter_id)
                  )
            )
            ORDER BY sa.analyzed_at ASC
            """,
            (validated_user_id,),
        )
        sensor_analysis_rows = cur.fetchall()

        cur.execute(
            """
            SELECT o.measurement_run_id::text,
                   o.patient_user_id::text,
                   COALESCE(o.encounter_id::text, us.encounter_id::text, us.id::text),
                   o.session_id::text,
                   o.device_external_id,
                   o.mode,
                   o.status,
                   o.started_at,
                   o.stopped_at,
                   o.sample_rate_hz,
                   o.requested_measurements,
                   o.raw_waveform_available,
                   o.total_chunks,
                   o.total_packets,
                   o.total_samples,
                   o.ts1_frame_count,
                   o.packet_loss_count,
                   o.checksum_failure_count,
                   o.malformed_packet_count,
                   o.latest_chunk_at
            FROM optical_capture_runs o
            LEFT JOIN user_sessions us ON us.id = o.session_id
            WHERE o.patient_user_id = %s
            ORDER BY o.started_at ASC, o.measurement_run_id ASC
            """,
            (validated_user_id,),
        )
        optical_run_rows = cur.fetchall()

    encounters: dict[str, dict[str, Any]] = {}
    session_index: dict[str, dict[str, Any]] = {}
    min_dt = datetime.min.replace(tzinfo=timezone.utc)

    def ensure_encounter(encounter_id: str) -> dict[str, Any]:
        encounter = encounters.get(encounter_id)
        if encounter is not None:
            return encounter

        encounter = {
            "encounter_id": encounter_id,
            "user_id": validated_user_id,
            "chat_session_id": None,
            "measurement_session_id": None,
            "started_at": None,
            "ended_at": None,
            "device_name": None,
            "feelings": [],
            "notes": None,
            "chat_result": None,
            "messages": [],
            "_started_dt": None,
            "_ended_dt": None,
            "_feelings_seen": set(),
            "_chat_result_ts": None,
            "_sessions": [],
        }
        encounters[encounter_id] = encounter
        return encounter

    def touch(encounter: dict[str, Any], ts: datetime | None) -> None:
        if ts is None:
            return
        if encounter["_started_dt"] is None or ts < encounter["_started_dt"]:
            encounter["_started_dt"] = ts
        if encounter["_ended_dt"] is None or ts > encounter["_ended_dt"]:
            encounter["_ended_dt"] = ts

    def add_feelings(encounter: dict[str, Any], raw: Any) -> None:
        for feeling in _extract_feelings(raw):
            if feeling in encounter["_feelings_seen"]:
                continue
            encounter["_feelings_seen"].add(feeling)
            encounter["feelings"].append(feeling)

    for session_id, encounter_id, started_at, ended_at, meta, device_name in session_rows:
        encounter = ensure_encounter(encounter_id)
        touch(encounter, started_at)
        touch(encounter, ended_at or started_at)

        meta = meta if isinstance(meta, dict) else {}
        if not encounter["device_name"] and device_name:
            encounter["device_name"] = device_name

        note = _coalesce_nonempty(meta.get("notes"), meta.get("note"))
        if note is not None and encounter["notes"] is None:
            encounter["notes"] = note

        add_feelings(encounter, meta.get("feelings"))

        session_info = {
            "session_id": session_id,
            "source": meta.get("source") if isinstance(meta.get("source"), str) else None,
            "started_at": started_at,
            "ended_at": ended_at,
            "device_name": device_name,
            "meta": meta,
            "has_messages": False,
            "has_sensor_events": False,
            "last_message_ts": None,
            "last_sensor_ts": None,
        }
        session_index[session_id] = session_info
        encounter["_sessions"].append(session_info)

        session_chat_result = _history_chat_result_from_meta(meta)
        candidate_ts = ended_at or started_at
        if session_chat_result is not None and (
            encounter["_chat_result_ts"] is None
            or (candidate_ts is not None and candidate_ts >= encounter["_chat_result_ts"])
        ):
            encounter["chat_result"] = session_chat_result
            encounter["_chat_result_ts"] = candidate_ts

    for session_id, encounter_id, role, content, created_at, meta in message_rows:
        encounter = ensure_encounter(encounter_id)
        touch(encounter, created_at)
        encounter["messages"].append(
            {
                "role": role,
                "text": content,
                "created_at": _iso(created_at),
            }
        )

        session_info = session_index.get(session_id)
        if session_info is not None:
            session_info["has_messages"] = True
            if session_info["last_message_ts"] is None or created_at > session_info["last_message_ts"]:
                session_info["last_message_ts"] = created_at

        if role == "assistant":
            message_chat_result = _history_chat_result_from_meta(meta, fallback_text=content)
            if message_chat_result is not None and (
                encounter["_chat_result_ts"] is None or created_at >= encounter["_chat_result_ts"]
            ):
                encounter["chat_result"] = message_chat_result
                encounter["_chat_result_ts"] = created_at

    for session_id, encounter_id, created_at, emotions in emotion_rows:
        encounter = ensure_encounter(encounter_id)
        touch(encounter, created_at)
        add_feelings(encounter, emotions)

        session_info = session_index.get(session_id)
        if session_info is not None and session_info["last_message_ts"] is None:
            session_info["last_message_ts"] = created_at

    measurement_groups: dict[tuple[str, str], dict[str, Any]] = {}

    for _, encounter_id, session_id, measurement_run_id, created_at, kind, seq, data, device_name in sensor_rows:
        encounter = ensure_encounter(encounter_id)
        touch(encounter, created_at)

        if not encounter["device_name"] and device_name:
            encounter["device_name"] = device_name

        session_info = session_index.get(session_id) if session_id else None
        if session_info is not None:
            session_info["has_sensor_events"] = True
            if session_info["last_sensor_ts"] is None or created_at > session_info["last_sensor_ts"]:
                session_info["last_sensor_ts"] = created_at

        payload = _normalized_sensor_payload(data if isinstance(data, dict) else {})
        explicit_measurement_id = _extract_explicit_measurement_id(payload)
        measurement_ids = _event_measurement_ids(kind, payload, explicit_measurement_id)

        if not measurement_ids:
            continue

        source, is_user_selected = _classify_sensor_source(payload, measurement_run_id)
        if not measurement_run_id:
            continue

        source = "measurement_capture"
        is_user_selected = True

        for measurement_id in measurement_ids:
            spec = TREND_SERIES_SPECS.get(measurement_id)
            if not isinstance(spec, dict):
                continue
            value = _extract_trend_value(kind, payload, spec)
            if value is None:
                continue

            key = (measurement_run_id, measurement_id)
            group = measurement_groups.get(key)
            if group is None:
                group = {
                    "measurement_run_id": measurement_run_id,
                    "measurement_id": measurement_id,
                    "encounter_id": encounter_id,
                    "measurement_session_id": session_id,
                    "device_name": device_name,
                    "source": source,
                    "is_user_selected": is_user_selected,
                    "summary_value": value,
                    "event_count": 0,
                    "kinds": set(),
                    "_latest_value_ts": created_at,
                    "_started_dt": created_at,
                    "_ended_dt": created_at,
                }
                measurement_groups[key] = group

            if session_id and not group.get("measurement_session_id"):
                group["measurement_session_id"] = session_id
            if device_name and not group.get("device_name"):
                group["device_name"] = device_name
            if created_at < group["_started_dt"]:
                group["_started_dt"] = created_at
            if created_at > group["_ended_dt"]:
                group["_ended_dt"] = created_at
            if created_at >= group["_latest_value_ts"]:
                group["summary_value"] = value
                group["_latest_value_ts"] = created_at
            if kind:
                group["kinds"].add(kind)
            group["event_count"] += 1

            if is_user_selected:
                group["is_user_selected"] = True
                group["source"] = "measurement_capture"
            elif group.get("is_user_selected") is None:
                group["is_user_selected"] = False
                group["source"] = source

    visual_history_rows: list[dict[str, Any]] = []
    visual_analysis_by_run_id: dict[str, dict[str, Any]] = {}
    visual_analysis_ts_by_run_id: dict[str, datetime] = {}
    for analysis_id, measurement_run_id, session_id, encounter_id, non_diagnostic, analysis_payload, analyzed_at, device_name in image_analysis_rows:
        if not encounter_id:
            continue

        encounter = ensure_encounter(encounter_id)
        touch(encounter, analyzed_at)

        if not encounter["device_name"] and device_name:
            encounter["device_name"] = device_name

        session_info = session_index.get(session_id) if session_id else None
        if session_info is not None:
            session_info["has_sensor_events"] = True
            if session_info["last_sensor_ts"] is None or analyzed_at > session_info["last_sensor_ts"]:
                session_info["last_sensor_ts"] = analyzed_at

        visual_analysis = _image_analysis_from_row(
            (analysis_id, measurement_run_id, session_id, encounter_id, bool(non_diagnostic), analysis_payload, analyzed_at)
        )
        visual_run_id = measurement_run_id or f"visual-analysis:{analysis_id}"
        existing_ts = visual_analysis_ts_by_run_id.get(visual_run_id)
        if existing_ts is None or analyzed_at >= existing_ts:
            visual_analysis_ts_by_run_id[visual_run_id] = analyzed_at
            visual_analysis_by_run_id[visual_run_id] = visual_analysis

        location_label = device_name or encounter.get("device_name")
        visual_history_rows.append(
            {
                "encounter_id": encounter["encounter_id"],
                "user_id": validated_user_id,
                "chat_session_id": encounter.get("chat_session_id"),
                "measurement_session_id": session_id or encounter.get("measurement_session_id"),
                "measurement_run_id": visual_run_id,
                "measurement_run_ids": [visual_run_id],
                "measurement_id": "visual_screening",
                "source": "measurement_capture",
                "is_user_selected": True,
                "started_at": _iso(analyzed_at),
                "ended_at": _iso(analyzed_at),
                "device_name": device_name or encounter.get("device_name"),
                "feelings": list(encounter.get("feelings") or []),
                "notes": encounter.get("notes"),
                "chat_result": encounter.get("chat_result"),
                "messages": list(encounter.get("messages") or []) if include_messages else [],
                "summary": {},
                "analysis": visual_analysis,
                "visual_analysis": visual_analysis,
                "findings": list(visual_analysis.get("patient_findings") or []),
                "location_label": location_label,
                "capture_location": location_label,
                "_sort_dt": analyzed_at,
            }
        )

    sensor_analysis_by_run_id: dict[str, dict[str, Any]] = {}
    sensor_analysis_ts_by_run_id: dict[str, datetime] = {}
    for sensor_analysis_row in sensor_analysis_rows:
        measurement_run_id = sensor_analysis_row[0]
        if not measurement_run_id:
            continue

        analysis_payload = _sensor_analysis_from_row(sensor_analysis_row)
        analyzed_at = sensor_analysis_row[14]
        analyzed_dt = analyzed_at if isinstance(analyzed_at, datetime) else min_dt

        existing_ts = sensor_analysis_ts_by_run_id.get(measurement_run_id)
        if existing_ts is None or analyzed_dt >= existing_ts:
            sensor_analysis_ts_by_run_id[measurement_run_id] = analyzed_dt
            sensor_analysis_by_run_id[measurement_run_id] = analysis_payload

    optical_history_rows: list[dict[str, Any]] = []
    optical_run_by_measurement_run_id: dict[str, dict[str, Any]] = {}
    for optical_run_row in optical_run_rows:
        optical_run = _serialize_optical_capture_run_row(optical_run_row)
        optical_run_by_measurement_run_id[str(optical_run.get("measurement_run_id") or "")] = optical_run
        encounter_id = optical_run.get("encounter_id")
        if not isinstance(encounter_id, str) or not encounter_id:
            continue

        encounter = ensure_encounter(encounter_id)
        run_started_at = optical_run.get("started_at")
        run_last_dt = optical_run.get("latest_chunk_at") or optical_run.get("stopped_at") or run_started_at
        touch(encounter, run_started_at if isinstance(run_started_at, datetime) else None)
        touch(encounter, run_last_dt if isinstance(run_last_dt, datetime) else None)

        device_name = optical_run.get("device_external_id") or encounter.get("device_name")
        if not encounter["device_name"] and device_name:
            encounter["device_name"] = device_name

        session_id = optical_run.get("session_id")
        session_info = session_index.get(session_id) if isinstance(session_id, str) else None
        if session_info is not None:
            session_info["has_sensor_events"] = True
            if isinstance(run_last_dt, datetime) and (
                session_info["last_sensor_ts"] is None or run_last_dt > session_info["last_sensor_ts"]
            ):
                session_info["last_sensor_ts"] = run_last_dt
            if not session_info.get("device_name") and device_name:
                session_info["device_name"] = device_name

        location_label = device_name or encounter.get("device_name")
        row = {
            "encounter_id": encounter["encounter_id"],
            "user_id": validated_user_id,
            "chat_session_id": encounter.get("chat_session_id"),
            "measurement_session_id": session_id or encounter.get("measurement_session_id"),
            "measurement_run_id": optical_run["measurement_run_id"],
            "measurement_run_ids": [optical_run["measurement_run_id"]],
            "measurement_id": OPTICAL_MEASUREMENT_ID,
            "measurement_mode": optical_run.get("mode"),
            "measurement_label": OPTICAL_MEASUREMENT_LABEL,
            "source": "optical_waveform_capture",
            "is_user_selected": True,
            "started_at": _iso(run_started_at),
            "ended_at": _iso(optical_run.get("stopped_at") or run_last_dt),
            "device_name": device_name,
            "capture_status": optical_run.get("status"),
            "raw_waveform_available": optical_run.get("raw_waveform_available"),
            "sample_rate_hz": optical_run.get("sample_rate_hz"),
            "packet_loss_count": optical_run.get("packet_loss_count"),
            "ts1_frame_count": optical_run.get("ts1_frame_count"),
            "total_chunks": optical_run.get("total_chunks"),
            "total_packets": optical_run.get("total_packets"),
            "total_samples": optical_run.get("total_samples"),
            "checksum_failure_count": optical_run.get("checksum_failure_count"),
            "malformed_packet_count": optical_run.get("malformed_packet_count"),
            "requested_measurements": list(optical_run.get("requested_measurements") or []),
            "waveform_api_path": optical_run.get("waveform_api_path"),
            "waveform_stream_path": optical_run.get("waveform_stream_path"),
            "feelings": list(encounter.get("feelings") or []),
            "notes": encounter.get("notes"),
            "chat_result": encounter.get("chat_result"),
            "messages": list(encounter.get("messages") or []) if include_messages else [],
            "summary": {},
            "location_label": location_label,
            "capture_location": location_label,
            "_sort_dt": run_last_dt or run_started_at or encounter.get("_sort_dt") or min_dt,
        }

        linked_sensor_analysis = sensor_analysis_by_run_id.get(optical_run["measurement_run_id"])
        if linked_sensor_analysis is not None:
            row["analysis"] = linked_sensor_analysis
            row["analysis_result"] = linked_sensor_analysis
            row["sensor_analysis"] = linked_sensor_analysis
            row["findings"] = list(
                linked_sensor_analysis.get("patient_findings")
                or _analysis_findings_text(linked_sensor_analysis.get("findings"))
            )

        linked_visual_analysis = visual_analysis_by_run_id.get(optical_run["measurement_run_id"])
        if linked_visual_analysis is not None:
            row["visual_analysis"] = linked_visual_analysis
            if linked_sensor_analysis is None:
                row["analysis"] = linked_visual_analysis
                row["analysis_result"] = linked_visual_analysis
                row["findings"] = list(
                    linked_visual_analysis.get("patient_findings")
                    or _analysis_findings_text(linked_visual_analysis.get("findings"))
                )

        optical_history_rows.append(row)

    sessions: list[dict[str, Any]] = []

    def session_sort_key(session_info: dict[str, Any]) -> datetime:
        return (
            session_info.get("last_sensor_ts")
            or session_info.get("last_message_ts")
            or session_info.get("ended_at")
            or session_info.get("started_at")
            or min_dt
        )

    for encounter in encounters.values():
        encounter["started_at"] = _iso(encounter["_started_dt"])
        encounter["ended_at"] = _iso(encounter["_ended_dt"])
        encounter["_sort_dt"] = encounter["_ended_dt"] or encounter["_started_dt"] or min_dt

        chat_candidates = [
            session_info
            for session_info in encounter["_sessions"]
            if session_info.get("has_messages") or session_info.get("source") == "api_chat"
        ]
        chat_candidates.sort(key=session_sort_key, reverse=True)
        if chat_candidates:
            encounter["chat_session_id"] = chat_candidates[0]["session_id"]
            if encounter["chat_result"] is None:
                fallback_chat_result = _history_chat_result_from_meta(chat_candidates[0].get("meta"))
                if fallback_chat_result is not None:
                    encounter["chat_result"] = fallback_chat_result

        measurement_candidates = [
            session_info
            for session_info in encounter["_sessions"]
            if session_info.get("has_sensor_events") or session_info.get("device_name")
        ]
        measurement_candidates.sort(key=session_sort_key, reverse=True)
        if measurement_candidates:
            encounter["measurement_session_id"] = measurement_candidates[0]["session_id"]
            if encounter["device_name"] is None:
                encounter["device_name"] = measurement_candidates[0].get("device_name")

    for group in measurement_groups.values():
        encounter = ensure_encounter(group["encounter_id"])
        summary_key = measurement_summary_keys.get(group["measurement_id"])
        summary: dict[str, Any] = {}
        if summary_key and group.get("summary_value") is not None:
            summary[summary_key] = _round_number(_as_number(group.get("summary_value")))

        linked_optical_run = optical_run_by_measurement_run_id.get(str(group.get("measurement_run_id") or ""))
        linked_waveform_available = bool(linked_optical_run.get("raw_waveform_available")) if linked_optical_run else False
        linked_waveform_api_path = linked_optical_run.get("waveform_api_path") if linked_optical_run else None
        linked_waveform_stream_path = linked_optical_run.get("waveform_stream_path") if linked_optical_run else None
        linked_capture_status = linked_optical_run.get("status") if linked_optical_run else None
        linked_sample_rate_hz = linked_optical_run.get("sample_rate_hz") if linked_optical_run else None
        linked_packet_loss_count = linked_optical_run.get("packet_loss_count") if linked_optical_run else None
        linked_ts1_frame_count = linked_optical_run.get("ts1_frame_count") if linked_optical_run else None
        linked_total_chunks = linked_optical_run.get("total_chunks") if linked_optical_run else None
        linked_total_packets = linked_optical_run.get("total_packets") if linked_optical_run else None
        linked_total_samples = linked_optical_run.get("total_samples") if linked_optical_run else None
        linked_checksum_failure_count = linked_optical_run.get("checksum_failure_count") if linked_optical_run else None
        linked_malformed_packet_count = linked_optical_run.get("malformed_packet_count") if linked_optical_run else None
        linked_requested_measurements = list(linked_optical_run.get("requested_measurements") or []) if linked_optical_run else []

        measurement_mode = OPTICAL_MEASUREMENT_MODE if linked_optical_run else STANDARD_OPTICAL_MEASUREMENT_MODE
        measurement_label = None
        trend_spec = TREND_SERIES_SPECS.get(group["measurement_id"]) or {}
        if isinstance(trend_spec, dict):
            candidate_label = trend_spec.get("label")
            if isinstance(candidate_label, str) and candidate_label.strip():
                measurement_label = candidate_label.strip()

        row_source = "measurement_capture" if group.get("is_user_selected") is not False else "background_telemetry"
        # Location label logic: prefer device_name, fallback to None
        location_label = group.get("device_name") or encounter.get("device_name")
        row = {
            "encounter_id": encounter["encounter_id"],
            "user_id": validated_user_id,
            "chat_session_id": encounter.get("chat_session_id"),
            "measurement_session_id": group.get("measurement_session_id") or encounter.get("measurement_session_id"),
            "measurement_run_id": group["measurement_run_id"],
            "measurement_run_ids": [group["measurement_run_id"]],
            "measurement_id": group["measurement_id"],
            "measurement_mode": measurement_mode,
            "measurement_label": measurement_label,
            "source": row_source,
            "is_user_selected": bool(group.get("is_user_selected", True)),
            "started_at": _iso(group.get("_started_dt")),
            "ended_at": _iso(group.get("_ended_dt")),
            "device_name": group.get("device_name") or encounter.get("device_name"),
            "capture_status": linked_capture_status,
            "raw_waveform_available": linked_waveform_available,
            "sample_rate_hz": linked_sample_rate_hz,
            "packet_loss_count": linked_packet_loss_count,
            "ts1_frame_count": linked_ts1_frame_count,
            "total_chunks": linked_total_chunks,
            "total_packets": linked_total_packets,
            "total_samples": linked_total_samples,
            "checksum_failure_count": linked_checksum_failure_count,
            "malformed_packet_count": linked_malformed_packet_count,
            "requested_measurements": linked_requested_measurements,
            "waveform_api_path": linked_waveform_api_path,
            "waveform_stream_path": linked_waveform_stream_path,
            "feelings": list(encounter.get("feelings") or []),
            "notes": encounter.get("notes"),
            "chat_result": encounter.get("chat_result"),
                "messages": list(encounter.get("messages") or []) if include_messages else [],
            "summary": summary,
            "location_label": location_label,
            "capture_location": location_label,
            "_sort_dt": group.get("_ended_dt") or group.get("_started_dt") or encounter.get("_sort_dt") or min_dt,
        }

        linked_sensor_analysis = sensor_analysis_by_run_id.get(group["measurement_run_id"])
        if linked_sensor_analysis is not None:
            row["analysis"] = linked_sensor_analysis
            row["analysis_result"] = linked_sensor_analysis
            row["sensor_analysis"] = linked_sensor_analysis
            row["findings"] = list(
                linked_sensor_analysis.get("patient_findings")
                or _analysis_findings_text(linked_sensor_analysis.get("findings"))
            )

        linked_visual_analysis = visual_analysis_by_run_id.get(group["measurement_run_id"])
        if linked_visual_analysis is not None:
            row["visual_analysis"] = linked_visual_analysis
            if linked_sensor_analysis is None:
                row["analysis"] = linked_visual_analysis
                row["analysis_result"] = linked_visual_analysis
                row["findings"] = list(
                    linked_visual_analysis.get("patient_findings")
                    or _analysis_findings_text(linked_visual_analysis.get("findings"))
                )

        sessions.append(row)

    sessions.extend(optical_history_rows)
    sessions.extend(visual_history_rows)

    sessions.sort(
        key=lambda encounter: (
            encounter.get("_sort_dt") or min_dt,
            encounter.get("started_at") or "",
        ),
        reverse=True,
    )
    for encounter in sessions:
        encounter.pop("_sort_dt", None)
    return {"sessions": sessions[:limit]}

def build_measurement_trends(user_id: str) -> dict[str, Any]:
    try:
        validated_user_id = str(uuid.UUID(user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    measurement_summary_keys = {
        "temperature": "skin_temperature_c",
        "ambient_temperature": "ambient_temperature_c",
        "spo2": "spo2_percent",
        "heart_rate": "heart_rate_bpm",
        "humidity": "humidity_percent",
        "co2": "co2_ppm",
    }

    canonical_points: dict[tuple[str, str], dict[str, Any]] = {}
    canonical_optical_run_metric_keys: set[tuple[str, str]] = set()

    def _put_point(*, trend_kind: str, run_id: str, ts: datetime, value: float, encounter_id: str | None) -> None:
        key = (trend_kind, run_id)
        existing = canonical_points.get(key)
        if existing is not None:
            existing_ts = existing.get("_ts")
            if isinstance(existing_ts, datetime) and existing_ts >= ts:
                return
        canonical_points[key] = {
            "kind": trend_kind,
            "measurement_run_id": run_id,
            "_ts": ts,
            "ts": _iso_utc(ts),
            "value": value,
            "encounter_id": encounter_id,
        }

    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id=%s LIMIT 1;", (validated_user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "User not found")

        cur.execute(
            """
            SELECT o.measurement_run_id::text,
                   COALESCE(o.encounter_id::text, us.encounter_id::text, us.id::text),
                   oa.status,
                   oa.analysis_payload,
                   oa.analyzed_at
            FROM optical_capture_runs o
            LEFT JOIN user_sessions us ON us.id = o.session_id
            LEFT JOIN optical_capture_run_analysis oa ON oa.measurement_run_id = o.measurement_run_id
            WHERE o.patient_user_id = %s
            ORDER BY COALESCE(oa.analyzed_at, o.stopped_at, o.latest_chunk_at, o.started_at) ASC,
                     o.measurement_run_id ASC
            """,
            (validated_user_id,),
        )
        optical_analysis_rows = cur.fetchall()

    for measurement_run_id, encounter_id, status, analysis_payload, analyzed_at in optical_analysis_rows:
        if not isinstance(measurement_run_id, str) or not measurement_run_id:
            continue
        payload = analysis_payload if isinstance(analysis_payload, dict) else {}
        final_optical = payload.get("final_optical") if isinstance(payload.get("final_optical"), dict) else None
        if final_optical is None:
            continue
        analysis_status = str(status or "").strip().lower()
        if analysis_status and analysis_status != OPTICAL_ANALYSIS_STATUS_COMPLETED:
            continue

        quality_token = str(final_optical.get("quality") or "").strip().lower()
        if quality_token == OPTICAL_FINAL_OPTICAL_QUALITY_INSUFFICIENT:
            continue

        ts = analyzed_at if isinstance(analyzed_at, datetime) else _utcnow()

        hr_value = _as_number(final_optical.get("heart_rate_bpm"))
        if hr_value is not None:
            _put_point(
                trend_kind="heart_rate",
                run_id=measurement_run_id,
                ts=ts,
                value=hr_value,
                encounter_id=encounter_id,
            )
            canonical_optical_run_metric_keys.add(("heart_rate", measurement_run_id))

        spo2_value = _as_number(final_optical.get("spo2_percent"))
        if spo2_value is not None:
            _put_point(
                trend_kind="spo2",
                run_id=measurement_run_id,
                ts=ts,
                value=spo2_value,
                encounter_id=encounter_id,
            )
            canonical_optical_run_metric_keys.add(("spo2", measurement_run_id))

    history_payload = build_measurement_history(validated_user_id, 1000, include_messages=False)
    for session in history_payload.get("sessions") or []:
        if not isinstance(session, dict):
            continue
        measurement_run_id = session.get("measurement_run_id")
        if not isinstance(measurement_run_id, str) or not measurement_run_id:
            continue

        trend_kind = session.get("measurement_id")
        if trend_kind not in TREND_SERIES_SPECS:
            continue

        if (trend_kind, measurement_run_id) in canonical_optical_run_metric_keys:
            continue

        summary_map = session.get("summary") if isinstance(session.get("summary"), dict) else {}
        summary_key = measurement_summary_keys.get(trend_kind)
        if not summary_key:
            continue

        value = _as_number(summary_map.get(summary_key))
        if value is None:
            continue

        ts = _parse_iso_datetime(session.get("ended_at")) or _parse_iso_datetime(session.get("started_at"))
        if ts is None:
            continue

        _put_point(
            trend_kind=trend_kind,
            run_id=measurement_run_id,
            ts=ts,
            value=value,
            encounter_id=session.get("encounter_id") if isinstance(session.get("encounter_id"), str) else None,
        )

    series_by_kind: dict[str, dict[str, Any]] = {}
    for point in sorted(
        canonical_points.values(),
        key=lambda item: (item.get("kind") or "", item.get("_ts") or datetime.min.replace(tzinfo=timezone.utc), item.get("measurement_run_id") or ""),
    ):
        trend_kind = point["kind"]
        spec = TREND_SERIES_SPECS.get(trend_kind)
        if not isinstance(spec, dict):
            continue
        value = _as_number(point.get("value"))
        if value is None:
            continue
        normalized_value = _extract_trend_value(trend_kind, {measurement_summary_keys.get(trend_kind, trend_kind): value}, spec)
        if normalized_value is None:
            continue

        series = series_by_kind.setdefault(
            trend_kind,
            {
                "kind": trend_kind,
                "label": spec.get("label"),
                "unit": spec.get("unit"),
                "normal_range": spec.get("normal_range"),
                "points": [],
            },
        )
        series["points"].append(
            {
                "ts": point["ts"],
                "value": normalized_value,
                "encounter_id": point.get("encounter_id"),
            }
        )

    return {
        "series": [
            series_by_kind[kind]
            for kind in TREND_SERIES_SPECS
            if kind in series_by_kind and series_by_kind[kind]["points"]
        ]
    }

@app.get("/auth/me", response_model=AuthSessionResp, response_model_exclude_none=True)
def auth_me(user_id: str = Depends(require_access_user_id)):
    return _build_auth_response_payload(user_id)


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
def get_profile(
    user_id: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)
    return _fetch_profile_payload(target_user_id)

@app.patch("/api/profile", response_model=UserProfileResp)
def update_profile(req: UserProfileUpdateReq, user_id: str = Depends(require_access_user_id)):
    patch = req.model_dump(exclude_unset=True)
    if not patch:
        return _fetch_profile_payload(user_id)

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


@app.get("/api/doctor/patients", response_model=DoctorPatientListResp)
def list_doctor_patients(
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    items = _list_doctor_patients(doctor_user_id)
    search_term = (search or "").strip().lower()
    if search_term:
        filtered: list[dict[str, Any]] = []
        for item in items:
            profile_data = _normalize_profile_map(item.get("profile"))
            haystack = " ".join(
                part
                for part in [
                    item.get("name"),
                    item.get("email"),
                    profile_data.get("external_patient_id"),
                ]
                if isinstance(part, str) and part.strip()
            ).lower()
            if search_term in haystack:
                filtered.append(item)
        items = filtered
    page, next_cursor = _paginate_offset_items(items, cursor=cursor, limit=limit)
    return {"items": page, "next_cursor": next_cursor}


@app.get("/api/doctor/patient-requests", response_model=DoctorPatientRequestListResp, response_model_exclude_none=True)
def list_doctor_patient_requests(
    status: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    with db() as conn, conn.cursor() as cur:
        items = _list_doctor_patient_requests(cur, doctor_user_id=doctor_user_id, status=status)

    search_term = (search or "").strip().lower()
    if search_term:
        filtered_items: list[dict[str, Any]] = []
        for item in items:
            profile_data = _normalize_profile_map(item.get("patient_profile"))
            haystack = " ".join(
                part
                for part in [
                    item.get("patient_name"),
                    item.get("patient_email"),
                    profile_data.get("external_patient_id"),
                    item.get("request_reason"),
                ]
                if isinstance(part, str) and part.strip()
            ).lower()
            if search_term in haystack:
                filtered_items.append(item)
        items = filtered_items

    page, next_cursor = _paginate_offset_items(items, cursor=cursor, limit=limit)
    return {"items": page, "next_cursor": next_cursor}


@app.post("/api/doctor/patient-requests/{request_id}/claim", response_model=DoctorPatientRequestClaimResp, response_model_exclude_none=True)
def claim_doctor_patient_request(
    request_id: str,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_request_id = _validated_uuid_str(request_id, "request_id")
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pdr.id::text,
                   pdr.patient_user_id::text,
                   pdr.status,
                   pdr.request_reason,
                   pdr.request_priority,
                   'patient_portal'::text AS request_source,
                   COALESCE((
                       SELECT max(m.created_at)
                       FROM consult_case_messages m
                       WHERE m.patient_user_id = pdr.patient_user_id
                         AND m.sender_type = 'patient'
                   ), pdr.updated_at, pdr.created_at) AS last_patient_message_at,
                   pdr.claimed_by_doctor_user_id::text,
                   NULL::text,
                   pdr.consult_case_id::text,
                   pdr.created_at,
                   pdr.updated_at,
                   pdr.claimed_at,
                   pdr.cancelled_at,
                   """ + _pending_request_queue_position_expr("pdr") + """ AS queue_position
            FROM patient_doctor_requests pdr
            WHERE pdr.id = %s
            FOR UPDATE
            """,
            (resolved_request_id,),
        )
        request_row = cur.fetchone()
        if not request_row:
            raise HTTPException(404, "Doctor request not found")
        request_item_base = _patient_doctor_request_item_from_row(request_row)
        request_status = request_item_base["status"]
        patient_user_id = request_item_base["patient_user_id"]

        cur.execute(
            """
            SELECT u.name,
                   u.email,
                   up.profile,
                   du.name
            FROM users u
            LEFT JOIN user_profile up ON up.user_id = u.id
            LEFT JOIN users du ON du.id = %s
            WHERE u.id = %s
            LIMIT 1
            """,
            (request_item_base.get("claimed_by_doctor_user_id"), patient_user_id),
        )
        patient_context_row = cur.fetchone()
        if not patient_context_row:
            raise HTTPException(404, "Patient not found")
        request_item = {
            "request_id": request_item_base["request_id"],
            "patient_user_id": patient_user_id,
            "patient_name": patient_context_row[0],
            "patient_email": patient_context_row[1],
            "patient_profile": patient_context_row[2] if isinstance(patient_context_row[2], dict) else {},
            "status": request_item_base["status"],
            "request_reason": request_item_base["request_reason"],
            "request_priority": request_item_base.get("request_priority"),
            "request_source": request_item_base.get("request_source"),
            "last_patient_message_at": request_item_base.get("last_patient_message_at"),
            "claimed_by_doctor_user_id": request_item_base["claimed_by_doctor_user_id"],
            "claimed_by_doctor_name": patient_context_row[3] if request_item_base.get("claimed_by_doctor_user_id") else None,
            "consult_case_id": request_item_base["consult_case_id"],
            "created_at": request_item_base["created_at"],
            "updated_at": request_item_base["updated_at"],
            "claimed_at": request_item_base["claimed_at"],
            "cancelled_at": request_item_base["cancelled_at"],
            "queue_position": request_item_base.get("queue_position"),
            "estimated_wait_minutes": request_item_base.get("estimated_wait_minutes"),
        }

        if request_status == PATIENT_DOCTOR_REQUEST_CANCELLED:
            raise HTTPException(409, "Doctor request has been cancelled")
        if request_status == PATIENT_DOCTOR_REQUEST_CLAIMED:
            if request_item.get("claimed_by_doctor_user_id") != doctor_user_id:
                raise HTTPException(409, "Doctor request has already been claimed")
            consult_case = _ensure_consult_case(
                cur,
                doctor_user_id=doctor_user_id,
                patient_user_id=patient_user_id,
                consult_case_id=request_item.get("consult_case_id"),
                create_if_missing=True,
            )
        else:
            cur.execute(
                """
                INSERT INTO doctor_patient_assignments (doctor_user_id, patient_user_id)
                VALUES (%s, %s)
                ON CONFLICT (doctor_user_id, patient_user_id) DO NOTHING
                """,
                (doctor_user_id, patient_user_id),
            )
            consult_case = _ensure_consult_case(
                cur,
                doctor_user_id=doctor_user_id,
                patient_user_id=patient_user_id,
                create_if_missing=True,
            )
            cur.execute(
                """
                UPDATE consult_cases
                SET consult_status_reason = COALESCE(consult_status_reason, %s),
                    last_clinician_action_at = now(),
                    last_clinician_action_type = 'consult_created',
                    updated_at = now(),
                    revision = revision + 1
                WHERE id = %s
                RETURNING updated_at, revision, consult_status_reason, last_clinician_action_at, last_clinician_action_type
                """,
                ("Patient request claimed.", consult_case["consult_case_id"]),
            )
            consult_update_row = cur.fetchone()
            consult_case["updated_at"] = consult_update_row[0]
            consult_case["revision"] = int(consult_update_row[1] or 0)
            consult_case["consult_status_reason"] = consult_update_row[2]
            consult_case["last_clinician_action_at"] = consult_update_row[3]
            consult_case["last_clinician_action_type"] = consult_update_row[4]
            cur.execute(
                """
                UPDATE patient_doctor_requests
                SET status = %s,
                    claimed_by_doctor_user_id = %s,
                    claimed_at = COALESCE(claimed_at, now()),
                    consult_case_id = %s,
                    updated_at = now()
                WHERE id = %s
                RETURNING id::text,
                          patient_user_id::text,
                          status,
                          request_reason,
                          request_priority,
                          'patient_portal'::text AS request_source,
                          COALESCE((
                              SELECT max(m.created_at)
                              FROM consult_case_messages m
                              WHERE m.patient_user_id = patient_doctor_requests.patient_user_id
                                AND m.sender_type = 'patient'
                          ), updated_at, created_at) AS last_patient_message_at,
                          claimed_by_doctor_user_id::text,
                          (SELECT name FROM users WHERE id = claimed_by_doctor_user_id),
                          consult_case_id::text,
                          created_at,
                          updated_at,
                          claimed_at,
                          cancelled_at,
                          """ + _pending_request_queue_position_expr("patient_doctor_requests") + """ AS queue_position
                """,
                (
                    PATIENT_DOCTOR_REQUEST_CLAIMED,
                    doctor_user_id,
                    consult_case["consult_case_id"],
                    resolved_request_id,
                ),
            )
            claimed_row = cur.fetchone()
            if not claimed_row:
                raise HTTPException(500, "Failed to claim doctor request")
            _insert_consult_case_event(
                cur,
                consult_case_id=consult_case["consult_case_id"],
                doctor_user_id=doctor_user_id,
                patient_user_id=patient_user_id,
                event_type="consult_updated",
                summary="Doctor claimed patient request.",
                metadata={"request_id": resolved_request_id},
            )
            request_item = _patient_doctor_request_item_from_row(claimed_row)
            request_item = {
                "request_id": request_item["request_id"],
                "patient_user_id": request_item["patient_user_id"],
                "patient_name": patient_context_row[0],
                "patient_email": patient_context_row[1],
                "patient_profile": patient_context_row[2] if isinstance(patient_context_row[2], dict) else {},
                "status": request_item["status"],
                "request_reason": request_item["request_reason"],
                "request_priority": request_item.get("request_priority"),
                "request_source": request_item.get("request_source"),
                "last_patient_message_at": request_item.get("last_patient_message_at"),
                "claimed_by_doctor_user_id": request_item["claimed_by_doctor_user_id"],
                "claimed_by_doctor_name": request_item["claimed_by_doctor_name"],
                "consult_case_id": request_item["consult_case_id"],
                "created_at": request_item["created_at"],
                "updated_at": request_item["updated_at"],
                "claimed_at": request_item["claimed_at"],
                "cancelled_at": request_item["cancelled_at"],
                "queue_position": request_item.get("queue_position"),
                "estimated_wait_minutes": request_item.get("estimated_wait_minutes"),
            }

        cur.execute(
            """
            SELECT u.id::text,
                   u.email,
                   u.name,
                   u.role,
                   dpa.created_at,
                   up.profile,
                   up.updated_at
            FROM doctor_patient_assignments dpa
            JOIN users u ON u.id = dpa.patient_user_id
            LEFT JOIN user_profile up ON up.user_id = u.id
            WHERE dpa.doctor_user_id = %s
              AND dpa.patient_user_id = %s
            LIMIT 1
            """,
            (doctor_user_id, patient_user_id),
        )
        patient_row = cur.fetchone()
        if not patient_row:
            raise HTTPException(500, "Doctor-patient assignment not found after claim")
        patient_item = _doctor_patient_item_from_row(patient_row)

        if request_status == PATIENT_DOCTOR_REQUEST_CLAIMED and request_item.get("consult_case_id") is None:
            request_item["consult_case_id"] = consult_case["consult_case_id"]

    return {
        "request": request_item,
        "patient": patient_item,
        "consult": _consult_summary_from_case(consult_case),
    }


@app.get("/api/doctor/patients/queue", response_model=DoctorQueueResp, response_model_exclude_none=True)
def list_doctor_patient_queue(
    consult_status: str | None = Query(None),
    priority: str | None = Query(None),
    risk_flag: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    with db() as conn, conn.cursor() as cur:
        queue_items = _filter_doctor_queue_items(
            _list_doctor_queue_items(cur, doctor_user_id=doctor_user_id),
            consult_status=consult_status,
            priority=priority,
            risk_flag=risk_flag,
            search=search,
        )
    page, next_cursor = _paginate_offset_items(queue_items, cursor=cursor, limit=limit)
    return {"items": page, "next_cursor": next_cursor}


@app.get("/api/doctor/patients/queue/summary", response_model=DoctorQueueSummaryResp, response_model_exclude_none=True)
def get_doctor_patient_queue_summary(
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    with db() as conn, conn.cursor() as cur:
        return _build_doctor_queue_summary_from_items(_list_doctor_queue_items(cur, doctor_user_id=doctor_user_id))


@app.get("/api/doctor/patients/queue/changes", response_model=DoctorQueueChangesResp, response_model_exclude_none=True)
def get_doctor_patient_queue_changes(
    since: str = Query(...),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    since_dt = _parse_iso_datetime(since)
    if since_dt is None:
        raise HTTPException(400, "since must be an ISO 8601 timestamp")
    with db() as conn, conn.cursor() as cur:
        return _build_doctor_queue_changes_payload(cur, doctor_user_id=doctor_user_id, since_dt=since_dt)


@app.get("/api/doctor/notifications/summary", response_model=DoctorNotificationSummaryResp, response_model_exclude_none=True)
def get_doctor_notification_summary(
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    with db() as conn, conn.cursor() as cur:
        return _build_doctor_notification_summary(cur, doctor_user_id=doctor_user_id)


@app.get("/api/doctor/patients/queue/stream")
async def stream_doctor_patient_queue_changes(
    request: Request,
    since: str | None = Query(None),
    poll_seconds: float = Query(STREAM_DEFAULT_POLL_SECONDS, ge=1.0, le=STREAM_MAX_POLL_SECONDS),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    since_dt = _parse_iso_datetime(since) if since else _utcnow()
    if since and since_dt is None:
        raise HTTPException(400, "since must be an ISO 8601 timestamp")
    effective_poll_seconds = _stream_poll_seconds(poll_seconds)

    async def event_stream():
        nonlocal since_dt
        last_heartbeat = time.monotonic()
        yield _sse_frame("ready", {"server_time": _utcnow(), "poll_seconds": effective_poll_seconds})
        while True:
            if await request.is_disconnected():
                break

            def _load_payload() -> dict[str, Any]:
                with db() as conn, conn.cursor() as cur:
                    return _build_doctor_queue_changes_payload(cur, doctor_user_id=doctor_user_id, since_dt=since_dt)

            payload = await run_in_threadpool(_load_payload)
            has_change = bool(
                payload.get("updated_consults")
                or payload.get("updated_items")
                or payload.get("removed_consults")
                or payload.get("new_requests")
                or payload.get("new_unread_messages")
            )
            if has_change:
                yield _sse_frame("changes", payload)
                since_dt = payload.get("server_time") or _utcnow()
                last_heartbeat = time.monotonic()
            elif time.monotonic() - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                yield b": heartbeat\n\n"
                last_heartbeat = time.monotonic()

            await asyncio.sleep(effective_poll_seconds)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/doctor/patients/assign", response_model=DoctorPatientListItem)
def assign_doctor_patient(
    req: DoctorPatientAssignReq,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    patient_user_id = (req.patient_user_id or "").strip() or None
    patient_email = (req.patient_email or "").strip() or None

    if not patient_user_id and not patient_email:
        raise HTTPException(400, "Provide patient_user_id or patient_email")
    if patient_user_id and patient_email:
        raise HTTPException(400, "Provide only one of patient_user_id or patient_email")

    resolved_patient_user_id: str | None = None

    with db() as conn, conn.cursor() as cur:
        if patient_user_id:
            try:
                resolved_patient_user_id = str(uuid.UUID(patient_user_id))
            except ValueError as exc:
                raise HTTPException(400, "patient_user_id must be a valid UUID") from exc
            cur.execute(
                "SELECT id::text, role FROM users WHERE id = %s LIMIT 1;",
                (resolved_patient_user_id,),
            )
        else:
            cur.execute(
                "SELECT id::text, role FROM users WHERE lower(email) = lower(%s) LIMIT 1;",
                (patient_email,),
            )

        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Patient not found")

        resolved_patient_user_id = row[0]
        patient_role = _normalized_user_role(row[1])

        if patient_role != USER_ROLE_PATIENT:
            raise HTTPException(400, "Selected user is not a patient")
        if resolved_patient_user_id == doctor_user_id:
            raise HTTPException(400, "Doctor cannot be assigned as their own patient")

        cur.execute(
            """
            INSERT INTO doctor_patient_assignments (doctor_user_id, patient_user_id)
            VALUES (%s, %s)
            ON CONFLICT (doctor_user_id, patient_user_id) DO NOTHING
            """,
            (doctor_user_id, resolved_patient_user_id),
        )

        cur.execute(
            """
            SELECT u.id::text,
                   u.email,
                   u.name,
                   u.role,
                   dpa.created_at,
                   up.profile,
                   up.updated_at
            FROM doctor_patient_assignments dpa
            JOIN users u ON u.id = dpa.patient_user_id
            LEFT JOIN user_profile up ON up.user_id = u.id
            WHERE dpa.doctor_user_id = %s
              AND dpa.patient_user_id = %s
            LIMIT 1
            """,
            (doctor_user_id, resolved_patient_user_id),
        )
        assigned_row = cur.fetchone()

    if not assigned_row:
        raise HTTPException(500, "Failed to create doctor-patient assignment")

    return _doctor_patient_item_from_row(assigned_row)


@app.delete("/api/doctor/patients/{patient_user_id}")
def remove_doctor_patient(
    patient_user_id: str,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    try:
        resolved_patient_user_id = str(uuid.UUID(patient_user_id))
    except ValueError as exc:
        raise HTTPException(400, "patient_user_id must be a valid UUID") from exc

    with db() as conn, conn.cursor() as cur:
        closed_consult = None
        cur.execute(
            """
            DELETE FROM doctor_patient_assignments
            WHERE doctor_user_id = %s
              AND patient_user_id = %s
            """,
            (doctor_user_id, resolved_patient_user_id),
        )
        removed = cur.rowcount
        if removed:
            closed_consult = _close_consult_case_for_doctor_removal(
                cur,
                doctor_user_id=doctor_user_id,
                patient_user_id=resolved_patient_user_id,
            )

    if not removed:
        raise HTTPException(404, "Doctor-patient assignment not found")

    return {
        "ok": True,
        "patient_user_id": resolved_patient_user_id,
        "closed_consult_id": closed_consult["consult_case_id"] if closed_consult else None,
    }


@app.get("/api/doctor/patients/{patient_user_id}/notes", response_model=DoctorPatientNoteListResp)
def list_doctor_patient_notes(
    patient_user_id: str,
    limit: int = Query(50, ge=1, le=200),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    return {
        "items": _list_doctor_patient_notes(
            doctor_user_id,
            resolved_patient_user_id,
            limit=limit,
        )
    }


@app.post("/api/doctor/patients/{patient_user_id}/notes", response_model=DoctorPatientNoteItem)
def create_doctor_patient_note(
    patient_user_id: str,
    req: DoctorPatientNoteReq,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    note_text = req.note_text.strip()
    if not note_text:
        raise HTTPException(400, "note_text is required")

    resolved_encounter_id = None
    if req.encounter_id:
        resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, req.encounter_id)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO doctor_patient_notes (doctor_user_id, patient_user_id, encounter_id, note_text)
            VALUES (%s, %s, %s, %s)
            RETURNING id::text,
                      doctor_user_id::text,
                      patient_user_id::text,
                      encounter_id::text,
                      note_text,
                      created_at,
                      updated_at
            """,
            (doctor_user_id, resolved_patient_user_id, resolved_encounter_id, note_text),
        )
        row = cur.fetchone()

    return _doctor_patient_note_item_from_row(row)


@app.delete("/api/doctor/patient-notes/{note_id}")
def delete_doctor_patient_note(
    note_id: str,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    try:
        resolved_note_id = str(uuid.UUID(note_id))
    except ValueError as exc:
        raise HTTPException(400, "note_id must be a valid UUID") from exc

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM doctor_patient_notes
            WHERE id = %s
              AND doctor_user_id = %s
            """,
            (resolved_note_id, doctor_user_id),
        )
        deleted = cur.rowcount

    if not deleted:
        raise HTTPException(404, "Doctor note not found")

    return {"ok": True, "note_id": resolved_note_id}


@app.get("/api/doctor/patients/{patient_user_id}/encounter-tags", response_model=DoctorEncounterTagListResp)
def list_doctor_encounter_tags(
    patient_user_id: str,
    encounter_id: str | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_encounter_id = None
    if encounter_id:
        resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, encounter_id)

    return {
        "items": _list_doctor_encounter_tags(
            doctor_user_id,
            resolved_patient_user_id,
            encounter_id=resolved_encounter_id,
            limit=limit,
        )
    }


@app.post("/api/doctor/patients/{patient_user_id}/encounter-tags", response_model=DoctorEncounterTagItem)
def create_doctor_encounter_tag(
    patient_user_id: str,
    req: DoctorEncounterTagReq,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, req.encounter_id)
    tag = req.tag.strip()
    if not tag:
        raise HTTPException(400, "tag is required")

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO doctor_encounter_tags (doctor_user_id, patient_user_id, encounter_id, tag)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (doctor_user_id, patient_user_id, encounter_id, tag)
            DO UPDATE SET tag = EXCLUDED.tag
            RETURNING id::text,
                      doctor_user_id::text,
                      patient_user_id::text,
                      encounter_id::text,
                      tag,
                      created_at
            """,
            (doctor_user_id, resolved_patient_user_id, resolved_encounter_id, tag),
        )
        row = cur.fetchone()

    return _doctor_encounter_tag_item_from_row(row)


@app.delete("/api/doctor/encounter-tags/{tag_id}")
def delete_doctor_encounter_tag(
    tag_id: str,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    try:
        resolved_tag_id = str(uuid.UUID(tag_id))
    except ValueError as exc:
        raise HTTPException(400, "tag_id must be a valid UUID") from exc

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM doctor_encounter_tags
            WHERE id = %s
              AND doctor_user_id = %s
            """,
            (resolved_tag_id, doctor_user_id),
        )
        deleted = cur.rowcount

    if not deleted:
        raise HTTPException(404, "Encounter tag not found")

    return {"ok": True, "tag_id": resolved_tag_id}


@app.patch("/api/doctor/patients/{patient_user_id}/consult", response_model_exclude_none=True)
def update_doctor_patient_consult(
    patient_user_id: str,
    req: DoctorConsultUpdateReq,
    if_match: str | None = Header(None, alias="If-Match"),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    updates = req.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(400, "No consult fields provided")

    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case_id=req.consult_case_id,
            create_if_missing=True,
        )
        current_revision = int(consult_case.get("revision") or 0)
        current_etag = _compute_consult_etag(
            consult_case.get("consult_case_id"),
            current_revision,
            consult_case.get("updated_at"),
        )
        request_etag = _normalized_if_match_etag(if_match)
        if req.revision is not None and int(req.revision) != current_revision:
            return _revision_conflict_response(
                "This consult was updated by another clinician.",
                current_revision=current_revision,
                current_etag=current_etag,
            )
        if request_etag and request_etag != current_etag:
            return _revision_conflict_response(
                "This consult was updated by another clinician.",
                current_revision=current_revision,
                current_etag=current_etag,
            )

        set_clauses = ["updated_at = now()", "revision = revision + 1"]
        params: list[Any] = []
        events: list[tuple[str, str, dict[str, Any]]] = []
        previous_status = consult_case.get("consult_status")
        previous_priority = consult_case.get("priority")
        previous_escalation = consult_case.get("escalation_status")
        previous_follow_up_due = consult_case.get("next_follow_up_due_at")
        now = _utcnow()
        action_type = "consult_updated"

        if "consult_status" in updates:
            new_status = _normalized_consult_status(updates.get("consult_status"))
            set_clauses.append("consult_status = %s")
            params.append(new_status)
            consult_case["consult_status"] = new_status
            if new_status == CONSULT_STATUS_CLOSED:
                set_clauses.append("closed_at = COALESCE(closed_at, now())")
                action_type = "consult_closed"
                events.append(("case_closed", "Case closed.", {"previous_status": previous_status, "current_status": new_status}))
            elif previous_status == CONSULT_STATUS_CLOSED and new_status != CONSULT_STATUS_CLOSED:
                set_clauses.append("closed_at = NULL")
                set_clauses.append("reopened_at = now()")
                action_type = "consult_reopened"
                events.append(("case_reopened", "Case reopened.", {"previous_status": previous_status, "current_status": new_status}))
            elif new_status != previous_status:
                events.append(("consult_status_changed", f"Consult status changed to {new_status.replace('_', ' ')}.", {"previous_status": previous_status, "current_status": new_status}))

        if "consult_status_reason" in updates:
            consult_status_reason = str(updates.get("consult_status_reason") or "").strip() or None
            set_clauses.append("consult_status_reason = %s")
            params.append(consult_status_reason)
            consult_case["consult_status_reason"] = consult_status_reason

        if "priority" in updates:
            new_priority = _normalized_consult_priority(updates.get("priority"))
            set_clauses.append("priority = %s")
            params.append(new_priority)
            consult_case["priority"] = new_priority
            if new_priority != previous_priority:
                events.append(("priority_changed", f"Priority changed to {new_priority}.", {"previous_priority": previous_priority, "current_priority": new_priority}))
                if new_priority == CONSULT_PRIORITY_CRITICAL:
                    set_clauses.append("last_critical_event_at = now()")

        if "escalation_status" in updates:
            new_escalation = _normalized_escalation_status(updates.get("escalation_status"))
            set_clauses.append("escalation_status = %s")
            params.append(new_escalation)
            consult_case["escalation_status"] = new_escalation
            if new_escalation != previous_escalation:
                events.append(("escalation_changed", f"Escalation changed to {new_escalation}.", {"previous_escalation": previous_escalation, "current_escalation": new_escalation}))
                if new_escalation == CONSULT_ESCALATION_ESCALATED:
                    set_clauses.append("last_critical_event_at = now()")
                elif new_escalation == CONSULT_ESCALATION_TRANSFERRED:
                    action_type = "handoff_completed"

        if "closed_reason" in updates:
            closed_reason = str(updates.get("closed_reason") or "").strip() or None
            set_clauses.append("closed_reason = %s")
            params.append(closed_reason)
            consult_case["closed_reason"] = closed_reason

        if "handoff_requested" in updates:
            handoff_requested = bool(updates.get("handoff_requested"))
            set_clauses.append("handoff_requested = %s")
            params.append(handoff_requested)
            consult_case["handoff_requested"] = handoff_requested
            if handoff_requested:
                action_type = "handoff_requested"
                events.append(("consult_updated", "Handoff requested.", {"handoff_requested": True}))

        if "handoff_target_clinician_id" in updates:
            handoff_target_clinician_id = _validated_uuid_str(updates.get("handoff_target_clinician_id"), "handoff_target_clinician_id")
            if handoff_target_clinician_id:
                cur.execute("SELECT name, role FROM users WHERE id = %s LIMIT 1", (handoff_target_clinician_id,))
                target_row = cur.fetchone()
                if not target_row:
                    raise HTTPException(404, "Target clinician not found")
                if _normalized_user_role(target_row[1]) != USER_ROLE_DOCTOR:
                    raise HTTPException(400, "Target clinician must be a doctor")
                consult_case["handoff_target_clinician_name"] = target_row[0]
            else:
                consult_case["handoff_target_clinician_name"] = None
            set_clauses.append("handoff_target_clinician_id = %s")
            params.append(handoff_target_clinician_id)
            consult_case["handoff_target_clinician_id"] = handoff_target_clinician_id
            if handoff_target_clinician_id:
                action_type = "handoff_requested"
                if "handoff_requested" not in updates:
                    set_clauses.append("handoff_requested = TRUE")
                    consult_case["handoff_requested"] = True
                events.append(
                    (
                        "handoff_completed" if consult_case.get("escalation_status") == CONSULT_ESCALATION_TRANSFERRED else "consult_updated",
                        f"Handoff target set to {consult_case.get('handoff_target_clinician_name') or 'doctor'}.",
                        {
                            "handoff_target_clinician_id": handoff_target_clinician_id,
                            "handoff_target_clinician_name": consult_case.get("handoff_target_clinician_name"),
                        },
                    )
                )

        if "next_action_due_at" in updates:
            set_clauses.append("next_action_due_at = %s")
            params.append(updates.get("next_action_due_at"))
            consult_case["next_action_due_at"] = updates.get("next_action_due_at")

        if "next_follow_up_due_at" in updates:
            set_clauses.append("next_follow_up_due_at = %s")
            params.append(updates.get("next_follow_up_due_at"))
            consult_case["next_follow_up_due_at"] = updates.get("next_follow_up_due_at")
            if updates.get("next_follow_up_due_at") != previous_follow_up_due:
                events.append(("follow_up_requested", "Follow-up updated.", {"previous_follow_up_due_at": _iso_utc(previous_follow_up_due), "current_follow_up_due_at": _iso_utc(updates.get("next_follow_up_due_at"))}))

        if "last_action_summary" in updates:
            summary = str(updates.get("last_action_summary") or "").strip() or None
            set_clauses.append("last_action_summary = %s")
            params.append(summary)
            consult_case["last_action_summary"] = summary
            if summary:
                events.append(("consult_updated", summary, {"last_action_summary": summary}))

        set_clauses.append("last_clinician_action_at = now()")
        set_clauses.append("last_clinician_action_type = %s")
        params.append(action_type)

        params.append(consult_case["consult_case_id"])
        cur.execute(
            f"""
            UPDATE consult_cases
            SET {', '.join(set_clauses)}
            WHERE id = %s
            RETURNING updated_at,
                      revision,
                      closed_at,
                      last_critical_event_at,
                      consult_status_reason,
                      closed_reason,
                      handoff_requested,
                      handoff_target_clinician_id::text,
                      reopened_at,
                      last_clinician_action_at,
                      last_clinician_action_type
            """,
            params,
        )
        updated_row = cur.fetchone()
        consult_case["updated_at"] = updated_row[0]
        consult_case["revision"] = int(updated_row[1] or 0)
        consult_case["closed_at"] = updated_row[2]
        consult_case["last_critical_event_at"] = updated_row[3]
        consult_case["consult_status_reason"] = updated_row[4]
        consult_case["closed_reason"] = updated_row[5]
        consult_case["handoff_requested"] = bool(updated_row[6])
        consult_case["handoff_target_clinician_id"] = updated_row[7]
        consult_case["reopened_at"] = updated_row[8]
        consult_case["last_clinician_action_at"] = updated_row[9]
        consult_case["last_clinician_action_type"] = updated_row[10]

        for event_type, summary, metadata in events or [("consult_updated", "Consult updated.", {})]:
            _insert_consult_case_event(
                cur,
                consult_case_id=consult_case["consult_case_id"],
                doctor_user_id=doctor_user_id,
                patient_user_id=resolved_patient_user_id,
                event_type=event_type,
                summary=summary,
                metadata=metadata,
            )

        triage = _compute_consult_metrics(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case=consult_case,
        )
        freshness = _freshness_summary_from_case(consult_case)

    return {
        "consult": _consult_summary_from_case(consult_case),
        "triage": triage,
        "freshness": freshness,
    }


@app.patch("/api/doctor/patients/{patient_user_id}/encounters/{encounter_id}", response_model_exclude_none=True)
def update_doctor_patient_encounter_summary(
    patient_user_id: str,
    encounter_id: str,
    req: DoctorEncounterClinicalUpdateReq,
    if_match: str | None = Header(None, alias="If-Match"),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, encounter_id)
    updates = req.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(400, "No encounter fields provided")

    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            create_if_missing=True,
        )
        cur.execute(
            """
            SELECT reason_for_consult,
                   patient_reported_symptoms,
                   clinician_assessment,
                   disposition,
                   follow_up_plan,
                   updated_at,
                   revision
            FROM encounter_clinical_summaries
            WHERE patient_user_id = %s
              AND encounter_id = %s
            LIMIT 1
            """,
            (resolved_patient_user_id, resolved_encounter_id),
        )
        existing_row = cur.fetchone()
        current_revision = int(existing_row[6] or 0) if existing_row else 0
        current_updated_at = existing_row[5] if existing_row else None
        current_etag = _compute_encounter_etag(resolved_encounter_id, current_revision, current_updated_at)
        request_etag = _normalized_if_match_etag(if_match)
        if req.revision is not None and int(req.revision) != current_revision:
            return _revision_conflict_response(
                "This encounter was updated by another clinician.",
                current_revision=current_revision,
                current_etag=current_etag,
            )
        if request_etag and request_etag != current_etag:
            return _revision_conflict_response(
                "This encounter was updated by another clinician.",
                current_revision=current_revision,
                current_etag=current_etag,
            )
        symptoms = _normalize_symptom_list(updates.get("patient_reported_symptoms")) if "patient_reported_symptoms" in updates else []
        cur.execute(
            """
            INSERT INTO encounter_clinical_summaries (
                patient_user_id,
                encounter_id,
                doctor_user_id,
                reason_for_consult,
                patient_reported_symptoms,
                clinician_assessment,
                disposition,
                follow_up_plan,
                revision,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, now())
            ON CONFLICT (patient_user_id, encounter_id)
            DO UPDATE SET
                doctor_user_id = EXCLUDED.doctor_user_id,
                reason_for_consult = COALESCE(EXCLUDED.reason_for_consult, encounter_clinical_summaries.reason_for_consult),
                patient_reported_symptoms = CASE WHEN %s THEN EXCLUDED.patient_reported_symptoms ELSE encounter_clinical_summaries.patient_reported_symptoms END,
                clinician_assessment = COALESCE(EXCLUDED.clinician_assessment, encounter_clinical_summaries.clinician_assessment),
                disposition = COALESCE(EXCLUDED.disposition, encounter_clinical_summaries.disposition),
                follow_up_plan = COALESCE(EXCLUDED.follow_up_plan, encounter_clinical_summaries.follow_up_plan),
                revision = encounter_clinical_summaries.revision + 1,
                updated_at = now()
            RETURNING reason_for_consult,
                      patient_reported_symptoms,
                      clinician_assessment,
                      disposition,
                      follow_up_plan,
                      updated_at,
                      revision
            """,
            (
                resolved_patient_user_id,
                resolved_encounter_id,
                doctor_user_id,
                updates.get("reason_for_consult"),
                psyjson.Json(symptoms),
                updates.get("clinician_assessment"),
                updates.get("disposition"),
                updates.get("follow_up_plan"),
                "patient_reported_symptoms" in updates,
            ),
        )
        row = cur.fetchone()
        cur.execute(
            """
            UPDATE consult_cases
            SET updated_at = now(),
                revision = revision + 1,
                last_action_summary = %s,
                last_clinician_action_at = now(),
                last_clinician_action_type = 'encounter_updated'
            WHERE id = %s
            RETURNING updated_at, revision
            """,
            ("Encounter summary updated.", consult_case["consult_case_id"]),
        )
        updated_case_row = cur.fetchone()
        consult_case["updated_at"] = updated_case_row[0]
        consult_case["revision"] = int(updated_case_row[1] or 0)
        consult_case["last_action_summary"] = "Encounter summary updated."
        _insert_consult_case_event(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            event_type="encounter_updated",
            summary="Encounter summary updated.",
            encounter_id=resolved_encounter_id,
            metadata={"encounter_id": resolved_encounter_id},
        )

    return {
        "encounter_id": resolved_encounter_id,
        "reason_for_consult": row[0],
        "patient_reported_symptoms": _normalize_symptom_list(row[1]),
        "clinician_assessment": row[2],
        "disposition": row[3],
        "follow_up_plan": row[4],
        "updated_at": row[5],
        "revision": int(row[6] or 0),
        "etag": _compute_encounter_etag(resolved_encounter_id, int(row[6] or 0), row[5]),
    }


@app.get("/api/doctor/patients/{patient_user_id}/timeline", response_model=DoctorTimelineResp, response_model_exclude_none=True)
def get_doctor_patient_timeline(
    patient_user_id: str,
    consult_case_id: str | None = Query(None),
    encounter_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, encounter_id) if encounter_id else None
    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        items = _build_patient_timeline_events(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case=consult_case,
            encounter_id=resolved_encounter_id,
        )
    page, next_cursor = _paginate_offset_items(items, cursor=cursor, limit=limit)
    return {"items": page, "next_cursor": next_cursor}


@app.get("/api/doctor/patients/{patient_user_id}/messages", response_model=ConsultCaseMessageListResp, response_model_exclude_none=True)
def list_doctor_patient_messages(
    patient_user_id: str,
    consult_case_id: str | None = Query(None),
    encounter_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    cursor: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, encounter_id) if encounter_id else None
    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        items = _list_consult_case_messages(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=resolved_patient_user_id,
            encounter_id=resolved_encounter_id,
        )
    page, next_cursor = _paginate_offset_items(items, cursor=cursor, limit=limit)
    return {"items": page, "next_cursor": next_cursor}


@app.get("/api/doctor/patients/{patient_user_id}/messages/summary", response_model=ConsultCaseMessageSummaryResp, response_model_exclude_none=True)
def get_doctor_patient_message_summary(
    patient_user_id: str,
    consult_case_id: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        return _build_consult_message_summary(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=resolved_patient_user_id,
        )


@app.post("/api/doctor/patients/{patient_user_id}/messages", response_model=ConsultCaseMessageItem, response_model_exclude_none=True)
def create_doctor_patient_message(
    patient_user_id: str,
    req: ConsultCaseMessageReq,
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_encounter_id = _validate_patient_encounter_id(resolved_patient_user_id, req.encounter_id) if req.encounter_id else None
    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case_id=req.consult_case_id,
            create_if_missing=True,
        )
        return _create_consult_case_message(
            cur,
            consult_case=consult_case,
            sender_type=USER_ROLE_DOCTOR,
            sender_user_id=doctor_user_id,
            body=req.body,
            message_type=req.message_type,
            requires_acknowledgement=req.requires_acknowledgement,
            attachments=req.attachments,
            encounter_id=resolved_encounter_id,
        )


@app.post("/api/doctor/patients/{patient_user_id}/messages/{message_id}/read", response_model=ConsultCaseMessageItem, response_model_exclude_none=True)
def read_doctor_patient_message(
    patient_user_id: str,
    message_id: str,
    consult_case_id: str | None = Query(None),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    resolved_message_id = _validated_uuid_str(message_id, "message_id")
    with db() as conn, conn.cursor() as cur:
        consult_case = _ensure_consult_case(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        return _mark_consult_case_message_read(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=resolved_patient_user_id,
            message_id=resolved_message_id,
            reader_sender_type=USER_ROLE_DOCTOR,
        )


@app.get("/api/doctor/patients/{patient_user_id}/dashboard", response_model=DoctorPatientDashboardResp, response_model_exclude_none=True)
def get_doctor_patient_dashboard(
    patient_user_id: str,
    history_limit: int = Query(20, ge=1, le=100),
    report_limit: int = Query(20, ge=1, le=100),
    image_limit: int = Query(20, ge=1, le=100),
    note_limit: int = Query(20, ge=1, le=200),
    tag_limit: int = Query(200, ge=1, le=500),
    doctor_user_id: str = Depends(require_doctor_user_id),
):
    resolved_patient_user_id = _require_assigned_patient_user_id(doctor_user_id, patient_user_id)
    patient_item = _get_doctor_patient_item(doctor_user_id, resolved_patient_user_id)
    profile_payload = _fetch_profile_payload(resolved_patient_user_id)
    report_items_all = _list_reports_for_user(resolved_patient_user_id, report_limit + 1)
    report_truncated = len(report_items_all) > report_limit
    report_items = report_items_all[:report_limit]

    measurement_payload_all = build_measurement_history(
        resolved_patient_user_id,
        history_limit + 1,
        include_messages=False,
    )
    measurement_sessions_all = list(measurement_payload_all.get("sessions") or [])
    measurement_truncated = len(measurement_sessions_all) > history_limit
    measurement_payload = {"sessions": measurement_sessions_all[:history_limit]}

    trends_payload = build_measurement_trends(resolved_patient_user_id)
    note_items_all = _list_doctor_patient_notes(doctor_user_id, resolved_patient_user_id, limit=note_limit + 1)
    note_truncated = len(note_items_all) > note_limit
    note_items = note_items_all[:note_limit]

    tag_items_all = _list_doctor_encounter_tags(doctor_user_id, resolved_patient_user_id, limit=tag_limit + 1)
    tag_truncated = len(tag_items_all) > tag_limit
    tag_items = tag_items_all[:tag_limit]

    with db() as conn, conn.cursor() as cur:
        consult_case, triage, chart_summary, freshness = _build_doctor_patient_context(
            cur,
            doctor_user_id=doctor_user_id,
            patient_user_id=resolved_patient_user_id,
        )
        clinical_summaries = _fetch_encounter_clinical_summaries(cur, patient_user_id=resolved_patient_user_id)
        message_items = _list_consult_case_messages(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=resolved_patient_user_id,
        )
        image_rows = _fetch_image_analysis_history_rows(
            cur,
            user_id=resolved_patient_user_id,
            limit=image_limit + 1,
        )

    image_truncated = len(image_rows) > image_limit
    image_items = [
        _image_analysis_history_item_from_row(
            row,
            include_previews=True,
            target_user_id=resolved_patient_user_id,
        )
        for row in image_rows[:image_limit]
    ]
    encounter_summaries = _build_doctor_encounter_summaries(
        measurement_payload["sessions"],
        image_items,
        note_items,
        tag_items,
        report_items,
        message_items,
        clinical_summaries,
    )

    return {
        "patient": patient_item,
        "profile": profile_payload["profile"],
        "profile_updated_at": profile_payload["updated_at"],
        "chart_summary": chart_summary,
        "consult": _consult_summary_from_case(consult_case),
        "triage": triage,
        "freshness": freshness,
        "meta": {
            "report_limit": report_limit,
            "reports_returned": len(report_items),
            "reports_truncated": report_truncated,
            "history_limit": history_limit,
            "measurements_returned": len(measurement_payload["sessions"]),
            "measurements_truncated": measurement_truncated,
            "trends_series_count": len(trends_payload.get("series") or []),
            "image_limit": image_limit,
            "image_history_returned": len(image_items),
            "image_history_truncated": image_truncated,
            "note_limit": note_limit,
            "notes_returned": len(note_items),
            "notes_truncated": note_truncated,
            "tag_limit": tag_limit,
            "encounter_tags_returned": len(tag_items),
            "encounter_tags_truncated": tag_truncated,
        },
        "reports": report_items,
        "measurements": measurement_payload,
        "trends": trends_payload,
        "image_history": {"items": image_items},
        "encounters": encounter_summaries,
        "notes": note_items,
        "encounter_tags": tag_items,
    }


@app.get("/api/patient/consults", response_model=PatientConsultListResp, response_model_exclude_none=True)
def list_patient_consults(
    include_closed: bool = Query(False),
    patient_user_id: str = Depends(require_access_user_id),
):
    with db() as conn, conn.cursor() as cur:
        consult_cases = _list_patient_consults(
            cur,
            patient_user_id=patient_user_id,
            include_closed=include_closed,
            ensure_assignment_cases=True,
        )
        items = [_build_patient_consult_item(cur, case) for case in consult_cases]
        pending_request = _fetch_open_patient_doctor_request(cur, patient_user_id)
    return {
        "items": items,
        "pending_request": pending_request,
    }


@app.get("/api/patient/consults/changes", response_model=PatientConsultChangesResp, response_model_exclude_none=True)
def get_patient_consult_changes(
    since: str = Query(...),
    patient_user_id: str = Depends(require_access_user_id),
):
    since_dt = _parse_iso_datetime(since)
    if since_dt is None:
        raise HTTPException(400, "since must be an ISO 8601 timestamp")
    with db() as conn, conn.cursor() as cur:
        return _build_patient_consult_changes_payload(cur, patient_user_id=patient_user_id, since_dt=since_dt)


@app.get("/api/patient/notifications/summary", response_model=PatientNotificationSummaryResp, response_model_exclude_none=True)
def get_patient_notification_summary(
    patient_user_id: str = Depends(require_access_user_id),
):
    with db() as conn, conn.cursor() as cur:
        return _build_patient_notification_summary(cur, patient_user_id=patient_user_id)


@app.get("/api/patient/consults/stream")
async def stream_patient_consult_changes(
    request: Request,
    since: str | None = Query(None),
    poll_seconds: float = Query(STREAM_DEFAULT_POLL_SECONDS, ge=1.0, le=STREAM_MAX_POLL_SECONDS),
    patient_user_id: str = Depends(require_access_user_id),
):
    since_dt = _parse_iso_datetime(since) if since else _utcnow()
    if since and since_dt is None:
        raise HTTPException(400, "since must be an ISO 8601 timestamp")
    effective_poll_seconds = _stream_poll_seconds(poll_seconds)

    async def event_stream():
        nonlocal since_dt
        last_heartbeat = time.monotonic()
        yield _sse_frame("ready", {"server_time": _utcnow(), "poll_seconds": effective_poll_seconds})
        while True:
            if await request.is_disconnected():
                break

            def _load_payload() -> dict[str, Any]:
                with db() as conn, conn.cursor() as cur:
                    return _build_patient_consult_changes_payload(cur, patient_user_id=patient_user_id, since_dt=since_dt)

            payload = await run_in_threadpool(_load_payload)
            has_change = bool(
                payload.get("updated_consults")
                or payload.get("updated_items")
                or payload.get("removed_consults")
                or payload.get("pending_request_updated")
            )
            if has_change:
                yield _sse_frame("changes", payload)
                since_dt = payload.get("server_time") or _utcnow()
                last_heartbeat = time.monotonic()
            elif time.monotonic() - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                yield b": heartbeat\n\n"
                last_heartbeat = time.monotonic()

            await asyncio.sleep(effective_poll_seconds)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/patient/doctor-request", response_model=PatientDoctorRequestItem, response_model_exclude_none=True)
def create_patient_doctor_request(
    req: PatientDoctorRequestReq,
    patient_user_id: str = Depends(require_access_user_id),
):
    request_reason = str(req.request_reason or "").strip() or None
    with db() as conn, conn.cursor() as cur:
        if _list_patient_assigned_doctor_ids(cur, patient_user_id):
            raise HTTPException(409, "Doctor already assigned")
        active_consults = _list_patient_consults(
            cur,
            patient_user_id=patient_user_id,
            include_closed=False,
            ensure_assignment_cases=False,
        )
        if active_consults:
            raise HTTPException(409, "Active consult already exists")
        existing_request = _fetch_open_patient_doctor_request(cur, patient_user_id)
        if existing_request is not None:
            raise HTTPException(409, "Pending doctor request already exists")
        cur.execute(
            """
            INSERT INTO patient_doctor_requests (patient_user_id, status, request_reason, request_priority, created_at, updated_at)
            VALUES (%s, %s, %s, %s, now(), now())
            """,
            (patient_user_id, PATIENT_DOCTOR_REQUEST_PENDING, request_reason, CONSULT_PRIORITY_ROUTINE),
        )
        created_request = _fetch_open_patient_doctor_request(cur, patient_user_id)
    if created_request is None:
        raise HTTPException(500, "Failed to create doctor request")
    return created_request


@app.post("/api/patient/doctor-request/{request_id}/cancel", response_model=PatientDoctorRequestItem, response_model_exclude_none=True)
def cancel_patient_doctor_request(
    request_id: str,
    patient_user_id: str = Depends(require_access_user_id),
):
    resolved_request_id = _validated_uuid_str(request_id, "request_id")
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text,
                   patient_user_id::text,
                   status,
                   request_reason,
                   request_priority,
                   'patient_portal'::text AS request_source,
                   COALESCE((
                       SELECT max(m.created_at)
                       FROM consult_case_messages m
                       WHERE m.patient_user_id = patient_doctor_requests.patient_user_id
                         AND m.sender_type = 'patient'
                   ), updated_at, created_at) AS last_patient_message_at,
                   claimed_by_doctor_user_id::text,
                   (SELECT name FROM users WHERE id = claimed_by_doctor_user_id),
                   consult_case_id::text,
                   created_at,
                   updated_at,
                   claimed_at,
                   cancelled_at,
                   """ + _pending_request_queue_position_expr("patient_doctor_requests") + """ AS queue_position
            FROM patient_doctor_requests
            WHERE id = %s
              AND patient_user_id = %s
            FOR UPDATE
            """,
            (resolved_request_id, patient_user_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Doctor request not found")
        request_item = _patient_doctor_request_item_from_row(row)
        if request_item["status"] != PATIENT_DOCTOR_REQUEST_PENDING:
            raise HTTPException(409, "Only pending doctor requests can be cancelled")
        cur.execute(
            """
            UPDATE patient_doctor_requests
            SET status = %s,
                cancelled_at = now(),
                updated_at = now()
            WHERE id = %s
            RETURNING id::text,
                      patient_user_id::text,
                      status,
                      request_reason,
                      request_priority,
                      'patient_portal'::text AS request_source,
                      COALESCE((
                          SELECT max(m.created_at)
                          FROM consult_case_messages m
                          WHERE m.patient_user_id = patient_doctor_requests.patient_user_id
                            AND m.sender_type = 'patient'
                      ), updated_at, created_at) AS last_patient_message_at,
                      claimed_by_doctor_user_id::text,
                      (SELECT name FROM users WHERE id = claimed_by_doctor_user_id),
                      consult_case_id::text,
                      created_at,
                      updated_at,
                      claimed_at,
                      cancelled_at,
                      """ + _pending_request_queue_position_expr("patient_doctor_requests") + """ AS queue_position
            """,
            (PATIENT_DOCTOR_REQUEST_CANCELLED, resolved_request_id),
        )
        updated_row = cur.fetchone()
    return _patient_doctor_request_item_from_row(updated_row)


@app.get("/api/patient/messages", response_model=ConsultCaseMessageListResp, response_model_exclude_none=True)
def list_patient_messages(
    consult_case_id: str | None = Query(None),
    encounter_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    cursor: str | None = Query(None),
    patient_user_id: str = Depends(require_access_user_id),
):
    resolved_encounter_id = _validate_patient_encounter_id(patient_user_id, encounter_id) if encounter_id else None
    with db() as conn, conn.cursor() as cur:
        consult_case = _resolve_patient_consult_case(
            cur,
            patient_user_id=patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        items = _list_consult_case_messages(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=patient_user_id,
            encounter_id=resolved_encounter_id,
        )
    page, next_cursor = _paginate_offset_items(items, cursor=cursor, limit=limit)
    return {"items": page, "next_cursor": next_cursor}


@app.post("/api/patient/messages", response_model=ConsultCaseMessageItem, response_model_exclude_none=True)
def create_patient_message(
    req: ConsultCaseMessageReq,
    patient_user_id: str = Depends(require_access_user_id),
):
    resolved_encounter_id = _validate_patient_encounter_id(patient_user_id, req.encounter_id) if req.encounter_id else None
    with db() as conn, conn.cursor() as cur:
        consult_case = _resolve_patient_consult_case(
            cur,
            patient_user_id=patient_user_id,
            consult_case_id=req.consult_case_id,
            create_if_missing=True,
        )
        return _create_consult_case_message(
            cur,
            consult_case=consult_case,
            sender_type=USER_ROLE_PATIENT,
            sender_user_id=patient_user_id,
            body=req.body,
            message_type=req.message_type,
            requires_acknowledgement=req.requires_acknowledgement,
            attachments=req.attachments,
            encounter_id=resolved_encounter_id,
        )


@app.post("/api/patient/messages/{message_id}/read", response_model=ConsultCaseMessageItem, response_model_exclude_none=True)
def read_patient_message(
    message_id: str,
    consult_case_id: str | None = Query(None),
    patient_user_id: str = Depends(require_access_user_id),
):
    resolved_message_id = _validated_uuid_str(message_id, "message_id")
    with db() as conn, conn.cursor() as cur:
        consult_case = _resolve_patient_consult_case(
            cur,
            patient_user_id=patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        return _mark_consult_case_message_read(
            cur,
            consult_case_id=consult_case["consult_case_id"],
            patient_user_id=patient_user_id,
            message_id=resolved_message_id,
            reader_sender_type=USER_ROLE_PATIENT,
        )


@app.post("/api/patient/messages/read-all", response_model=PatientMessageReadAllResp, response_model_exclude_none=True)
def read_all_patient_messages(
    consult_case_id: str = Query(...),
    patient_user_id: str = Depends(require_access_user_id),
):
    with db() as conn, conn.cursor() as cur:
        consult_case = _resolve_patient_consult_case(
            cur,
            patient_user_id=patient_user_id,
            consult_case_id=consult_case_id,
            create_if_missing=True,
        )
        cur.execute(
            """
            UPDATE consult_case_messages
            SET read_at = COALESCE(read_at, now())
            WHERE consult_case_id = %s
              AND patient_user_id = %s
              AND sender_type = 'doctor'
              AND read_at IS NULL
            RETURNING id::text
            """,
            (consult_case["consult_case_id"], patient_user_id),
        )
        rows = cur.fetchall()
        read_count = len(rows)
        updated_at = _utcnow()
        if read_count:
            _insert_consult_case_event(
                cur,
                consult_case_id=consult_case["consult_case_id"],
                doctor_user_id=consult_case["doctor_user_id"],
                patient_user_id=patient_user_id,
                event_type="message_read",
                summary=f"Patient marked {read_count} message(s) as read.",
                metadata={"reader_type": USER_ROLE_PATIENT, "read_count": read_count, "bulk": True},
            )
    return {
        "consult_case_id": consult_case["consult_case_id"],
        "read_count": read_count,
        "updated_at": updated_at,
    }


@app.post("/api/consult-attachments/upload", response_model=ConsultAttachmentUploadResp, response_model_exclude_none=True)
async def upload_consult_attachment(
    consult_case_id: str = Form(...),
    file: UploadFile = File(...),
    auth_user_id: str = Depends(require_access_user_id),
):
    data = await file.read()
    if not data:
        raise HTTPException(400, "Empty file")

    max_bytes = CONSULT_ATTACHMENT_MAX_MB * 1024 * 1024
    if len(data) > max_bytes:
        raise HTTPException(413, f"File too large (max {CONSULT_ATTACHMENT_MAX_MB} MB)")

    safe_name = os.path.basename(file.filename or "attachment")
    if not safe_name:
        safe_name = "attachment"
    content_type = (file.content_type or "application/octet-stream").strip() or "application/octet-stream"
    ext = os.path.splitext(safe_name)[-1].lower()
    kind = "image" if content_type.lower().startswith("image/") or ext in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"} else "file"
    sha = hashlib.sha256(data).hexdigest()
    day = time.strftime("%Y/%m/%d")

    with db() as conn, conn.cursor() as cur:
        consult_case, sender_type = _resolve_consult_case_participant(
            cur,
            consult_case_id=consult_case_id,
            auth_user_id=auth_user_id,
            allow_assigned_doctor_read=False,
        )
        key = f"consult-attachments/{consult_case['consult_case_id']}/{day}/{sha}-{uuid.uuid4().hex[:8]}-{safe_name}"
        try:
            S3.put_object(
                Bucket=BKT,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
        except Exception as exc:
            raise HTTPException(502, "Unable to store consult attachment") from exc

        cur.execute(
            """
            INSERT INTO consult_attachments (
                consult_case_id,
                doctor_user_id,
                patient_user_id,
                uploaded_by_user_id,
                uploaded_by_sender_type,
                bucket,
                object_key,
                filename,
                content_type,
                size_bytes,
                sha256,
                kind
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id::text,
                      consult_case_id::text,
                      kind,
                      filename,
                      content_type,
                      size_bytes,
                      created_at
            """,
            (
                consult_case["consult_case_id"],
                consult_case["doctor_user_id"],
                consult_case["patient_user_id"],
                auth_user_id,
                sender_type,
                BKT,
                key,
                safe_name,
                content_type,
                len(data),
                sha,
                kind,
            ),
        )
        row = cur.fetchone()
    return _consult_attachment_item_from_row(row)


@app.get("/api/consult-attachments/{attachment_id}/content")
def get_consult_attachment_content(
    attachment_id: str,
    auth_user_id: str = Depends(require_access_user_id),
):
    resolved_attachment_id = _validated_uuid_str(attachment_id, "attachment_id")
    with db() as conn, conn.cursor() as cur:
        attachment_item, row = _fetch_consult_attachment_for_user(
            cur,
            attachment_id=resolved_attachment_id,
            auth_user_id=auth_user_id,
        )

    attachment_bytes = _read_image_object_bytes(bucket=row[8], key=row[9])
    if not attachment_bytes:
        raise HTTPException(404, "Stored attachment content is unavailable")

    filename = str(attachment_item["filename"]).replace("\"", "")
    return Response(
        content=attachment_bytes,
        media_type=str(attachment_item["content_type"]),
        headers={
            "Cache-Control": "private, no-store",
            "Content-Disposition": f'inline; filename="{filename}"',
        },
    )


@app.get("/api/consult-attachments/{attachment_id}/preview")
def get_consult_attachment_preview(
    attachment_id: str,
    auth_user_id: str = Depends(require_access_user_id),
):
    resolved_attachment_id = _validated_uuid_str(attachment_id, "attachment_id")
    with db() as conn, conn.cursor() as cur:
        attachment_item, row = _fetch_consult_attachment_for_user(
            cur,
            attachment_id=resolved_attachment_id,
            auth_user_id=auth_user_id,
        )

    preview_kind = _attachment_preview_kind(
        kind=attachment_item.get("kind"),
        content_type=attachment_item.get("content_type"),
        filename=attachment_item.get("filename"),
    )
    if not preview_kind:
        raise HTTPException(404, "Preview is unavailable for this attachment")

    attachment_bytes = _read_image_object_bytes(bucket=row[8], key=row[9])
    if not attachment_bytes:
        raise HTTPException(404, "Stored attachment content is unavailable")

    if preview_kind == "image":
        try:
            preview_bytes = _build_image_preview_bytes(attachment_bytes)
        except Exception as exc:
            raise HTTPException(422, "Unable to render image preview") from exc
        return Response(
            content=preview_bytes,
            media_type="image/jpeg",
            headers={"Cache-Control": "private, no-store"},
        )

    preview_bytes = _build_attachment_text_preview_bytes(attachment_bytes)
    return Response(
        content=preview_bytes,
        media_type="text/plain; charset=utf-8",
        headers={"Cache-Control": "private, no-store"},
    )

@app.post("/api/reports/generate")
def generate_report(
    req: ReportGenerateReq,
    user_id: str = Depends(require_access_user_id),
):
    resolved_encounter_id = _validated_uuid_str(req.encounter_id, "encounter_id")
    if resolved_encounter_id:
        _validate_patient_encounter_id(user_id, resolved_encounter_id)
    payload = build_report_payload(
        user_id=user_id,
        limit_sessions=req.limit_sessions,
        limit_events=req.limit_events,
    )
    if resolved_encounter_id:
        payload["encounter_id"] = resolved_encounter_id
    payload_json = _json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    payload_hash = hashlib.sha256(payload_json).hexdigest()
    return {
        "user_id": user_id,
        "generated_at": _utcnow().isoformat(),
        "payload_hash": payload_hash,
        "encounter_id": resolved_encounter_id,
        "payload": payload,
    }

@app.post("/api/reports/run")
async def run_report(
    req: ReportRunReq,
    user_id: str = Depends(require_access_user_id),
):
    resolved_encounter_id = _validated_uuid_str(req.encounter_id, "encounter_id")
    if resolved_encounter_id:
        _validate_patient_encounter_id(user_id, resolved_encounter_id)
    payload = build_report_payload(
        user_id=user_id,
        limit_sessions=req.limit_sessions,
        limit_events=req.limit_events,
    )
    if resolved_encounter_id:
        payload["encounter_id"] = resolved_encounter_id
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

    consult_case_id: str | None = None
    with db() as conn, conn.cursor() as cur:
        if resolved_encounter_id:
            try:
                consult_case = _resolve_patient_consult_case(
                    cur,
                    patient_user_id=user_id,
                    create_if_missing=False,
                )
                consult_case_id = consult_case.get("consult_case_id")
            except HTTPException:
                consult_case_id = None
        cur.execute(
            """
            INSERT INTO reports (user_id, model, payload_hash, report_md, status, encounter_id, consult_case_id)
            VALUES (%s, %s, %s, %s, 'generated', %s, %s)
            RETURNING id::text, created_at, encounter_id::text, consult_case_id::text
            """,
            (user_id, model_name, payload_hash, report_md, resolved_encounter_id, consult_case_id),
        )
        report_id, created_at, stored_encounter_id, stored_consult_case_id = cur.fetchone()

    return {
        "user_id": user_id,
        "report_id": report_id,
        "generated_at": created_at.isoformat(),
        "model": model_name,
        "payload_hash": payload_hash,
        "encounter_id": stored_encounter_id,
        "consult_case_id": stored_consult_case_id,
        "report_md": report_md,
    }

@app.get("/api/reports", response_model=list[ReportListItem])
def list_reports(
    limit: int = Query(20, ge=1, le=100),
    user_id: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, created_at, model, status, payload_hash, encounter_id::text, consult_case_id::text
            FROM reports
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (target_user_id, limit),
        )
        rows = cur.fetchall()
    return [
        _report_list_item_from_row(r)
        for r in rows
    ]

@app.get("/api/reports/{report_id}", response_model=ReportDetail)
def get_report(
    report_id: str,
    user_id: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    try:
        uuid.UUID(report_id)
    except ValueError:
        raise HTTPException(400, "report_id must be a valid UUID")

    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id::text, created_at, model, status, payload_hash, encounter_id::text, consult_case_id::text, report_md
            FROM reports
            WHERE id = %s AND user_id = %s
            """,
            (report_id, target_user_id),
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
        "encounter_id": row[5],
        "consult_case_id": row[6],
        "report_md": row[7],
    }

@app.post("/api/devices/register", response_model=DeviceRegisterResp)
def register_device(
    req: DeviceRegisterReq,
    auth_user_id: str = Depends(require_access_user_id),
):
    with db() as conn, conn.cursor() as cur:
        if auth_user_id != req.user_id:
            logger.warning(
                "device register auth mismatch auth_user_id=%s req_user_id=%s device_external_id=%s",
                auth_user_id,
                req.user_id,
                req.device_external_id,
            )
            raise HTTPException(403, "user_id does not match the authenticated user")

        # Shared-device registration: ensure the device exists and refresh metadata,
        # but do not transfer exclusive ownership on re-registration.
        cur.execute(
            """
            INSERT INTO devices (user_id, device_external_id, platform, last_seen_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (device_external_id)
            DO UPDATE SET platform=COALESCE(EXCLUDED.platform, devices.platform),
                          last_seen_at=now()
            RETURNING id::text;
            """,
            (auth_user_id, req.device_external_id, req.platform),
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
def start_session(
    req: StartSessionReq,
    auth_user_id: str = Depends(require_access_user_id),
):
    encounter_id = _validated_uuid_str(req.encounter_id, "encounter_id") or str(uuid.uuid4())
    session_meta = dict(req.meta or {})
    session_meta.setdefault("source", "api_sessions_start")

    with db() as conn, conn.cursor() as cur:
        if auth_user_id != req.user_id:
            logger.warning(
                "session start auth mismatch auth_user_id=%s req_user_id=%s device_external_id=%s encounter_id=%s",
                auth_user_id,
                req.user_id,
                req.device_external_id,
                encounter_id,
            )
            raise HTTPException(403, "user_id does not match the authenticated user")

        cur.execute("SELECT 1 FROM users WHERE id=%s LIMIT 1;", (req.user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "User not found")

        cur.execute(
            """
            SELECT 1
            FROM user_sessions
            WHERE encounter_id = %s AND user_id <> %s
            LIMIT 1
            """,
            (encounter_id, req.user_id),
        )
        if cur.fetchone():
            raise HTTPException(400, "encounter_id belongs to another user")

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
            INSERT INTO user_sessions (user_id, device_id, started_at, meta, encounter_id)
            VALUES (%s, %s, now(), %s::jsonb, %s)
            RETURNING id::text;
            """,
            (auth_user_id, device_id, _json.dumps(session_meta), encounter_id),
        )
        (session_id,) = cur.fetchone()

    return {"session_id": session_id, "device_id": device_id, "encounter_id": encounter_id}

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
    session_id = _validated_uuid_str(req.session_id, "session_id")
    measurement_run_id = _validated_uuid_str(req.measurement_run_id, "measurement_run_id")
    encounter_id = _validated_uuid_str(req.encounter_id, "encounter_id")
    resolved_encounter_id = encounter_id
    fallback_ts = req.ts or datetime.now(timezone.utc)
    shared_data = dict(req.data or {})
    readings = req.readings or []
    analysis: dict[str, Any] | None = None

    if readings:
        events = [
            {
                "ts": reading.ts or fallback_ts,
                "seq": reading.seq,
                "data": _normalized_sensor_payload({**shared_data, **(reading.data or {})}),
            }
            for reading in readings
        ]
    else:
        events = [{"ts": fallback_ts, "seq": req.seq, "data": _normalized_sensor_payload(shared_data)}]

    logger.info(
        "sensor_event received device_external_id=%s session_id=%s encounter_id=%s measurement_run_id=%s kind=%s seq=%s ts=%s data_keys=%s readings=%s",
        req.device_external_id,
        session_id,
        encounter_id,
        measurement_run_id,
        req.kind,
        req.seq,
        req.ts.isoformat() if req.ts else None,
        len(shared_data),
        len(events),
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

            if session_id:
                cur.execute(
                    """
                    SELECT COALESCE(encounter_id, id)::text
                    FROM user_sessions
                    WHERE id=%s AND (device_id=%s OR device_id IS NULL)
                    LIMIT 1;
                    """,
                    (session_id, device_id),
                )
                row = cur.fetchone()
                if not row:
                    logger.warning(
                        "sensor_event invalid_session session_id=%s device_id=%s",
                        session_id,
                        device_id,
                    )
                    observe_sensor_event(req.kind, "invalid_session")
                    raise HTTPException(
                        400,
                        {"code": "INVALID_SESSION", "message": "Invalid session_id for this device"},
                    )
                session_encounter_id = row[0]
                if resolved_encounter_id and session_encounter_id and resolved_encounter_id != session_encounter_id:
                    logger.warning(
                        "sensor_event invalid_encounter session_id=%s encounter_id=%s expected_encounter_id=%s",
                        session_id,
                        resolved_encounter_id,
                        session_encounter_id,
                    )
                    observe_sensor_event(req.kind, "invalid_encounter")
                    raise HTTPException(
                        400,
                        {"code": "INVALID_ENCOUNTER", "message": "encounter_id does not match session_id"},
                    )
                resolved_encounter_id = session_encounter_id or resolved_encounter_id
            elif resolved_encounter_id:
                cur.execute(
                    """
                    SELECT 1
                    FROM user_sessions
                    WHERE encounter_id=%s AND (device_id=%s OR device_id IS NULL)
                    LIMIT 1;
                    """,
                    (resolved_encounter_id, device_id),
                )
                if not cur.fetchone():
                    logger.warning(
                        "sensor_event invalid_encounter encounter_id=%s device_id=%s",
                        resolved_encounter_id,
                        device_id,
                    )
                    observe_sensor_event(req.kind, "invalid_encounter")
                    raise HTTPException(
                        400,
                        {"code": "INVALID_ENCOUNTER", "message": "Invalid encounter_id for this device"},
                    )

            stored_count = 0
            duplicate_count = 0

            for event in events:
                event_seq = event.get("seq")
                event_data = event.get("data") if isinstance(event.get("data"), dict) else {}

                if measurement_run_id and event_seq is not None:
                    # For explicit measurement runs, de-dupe within the run (same kind + reading seq)
                    # so we don't drop valid readings due to global (device_id, kind, seq) collisions.
                    event_seq_token = str(event_seq)
                    cur.execute(
                        """
                        SELECT 1
                        FROM sensor_events
                        WHERE measurement_run_id=%s
                          AND kind=%s
                          AND (
                                seq::text = %s
                                OR data->>'_reading_seq' = %s
                              )
                        LIMIT 1
                        """,
                        (measurement_run_id, req.kind, event_seq_token, event_seq_token),
                    )

                    if cur.fetchone():
                        inserted = False
                    else:
                        payload = dict(event_data)
                        payload.setdefault("_reading_seq", event_seq)
                        cur.execute(
                            """
                            INSERT INTO sensor_events (session_id, measurement_run_id, encounter_id, device_id, ts, kind, seq, data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                            """,
                            (
                                session_id,
                                measurement_run_id,
                                resolved_encounter_id,
                                device_id,
                                event["ts"],
                                req.kind,
                                None,
                                _json.dumps(payload),
                            ),
                        )
                        inserted = cur.rowcount == 1
                else:
                    cur.execute(
                        """
                        INSERT INTO sensor_events (session_id, measurement_run_id, encounter_id, device_id, ts, kind, seq, data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (device_id, kind, seq) WHERE seq IS NOT NULL DO NOTHING
                        """,
                        (
                            session_id,
                            measurement_run_id,
                            resolved_encounter_id,
                            device_id,
                            event["ts"],
                            req.kind,
                            event_seq,
                            _json.dumps(event_data),
                        ),
                    )
                    inserted = cur.rowcount == 1

                stored_count += int(inserted)
                duplicate_count += int(not inserted)
                observe_sensor_event(req.kind, "stored" if inserted else "duplicate")

            try:
                analysis_scope, analysis_events = _load_sensor_events_for_analysis(
                    cur,
                    device_id=device_id,
                    session_id=session_id,
                    encounter_id=resolved_encounter_id,
                    measurement_run_id=measurement_run_id,
                    anchor_ts=max((event["ts"] for event in events), default=fallback_ts),
                )
                analysis = _analyze_sensor_events(
                    analysis_events,
                    scope=analysis_scope,
                    session_id=session_id,
                    encounter_id=resolved_encounter_id,
                    measurement_run_id=measurement_run_id,
                )
                analysis = _apply_baseline_model(
                    cur,
                    analysis=analysis,
                    device_id=device_id,
                    session_id=session_id,
                    encounter_id=resolved_encounter_id,
                    measurement_run_id=measurement_run_id,
                )
                analysis = _apply_public_dataset_model(analysis)
                analysis = _apply_personalization_model(
                    cur,
                    analysis=analysis,
                    device_id=device_id,
                    session_id=session_id,
                    encounter_id=resolved_encounter_id,
                    measurement_run_id=measurement_run_id,
                )
                _persist_sensor_analysis(cur, device_id=device_id, analysis=analysis)
            except Exception:
                logger.exception(
                    "sensor_analysis failed device_external_id=%s session_id=%s encounter_id=%s measurement_run_id=%s kind=%s",
                    req.device_external_id,
                    session_id,
                    resolved_encounter_id,
                    measurement_run_id,
                    req.kind,
                )

    except HTTPException:
        raise
    except psycopg.Error:
        observe_sensor_event(req.kind, "db_error")
        logger.exception(
            "sensor_event db_error device_external_id=%s session_id=%s encounter_id=%s measurement_run_id=%s kind=%s seq=%s",
            req.device_external_id,
            session_id,
            encounter_id,
            measurement_run_id,
            req.kind,
            req.seq,
        )
        raise HTTPException(500, {"code": "DB_ERROR", "message": "Database error"})
    except Exception:
        observe_sensor_event(req.kind, "internal_error")
        logger.exception(
            "sensor_event unexpected_error device_external_id=%s session_id=%s encounter_id=%s measurement_run_id=%s kind=%s seq=%s",
            req.device_external_id,
            session_id,
            encounter_id,
            measurement_run_id,
            req.kind,
            req.seq,
        )
        raise HTTPException(500, {"code": "INTERNAL_ERROR", "message": "Internal server error"})

    # If analysis has insufficient data, omit the analysis object from the response
    # (so the client does not enter a persistent "Need more readings" state) and
    # instead include lightweight diagnostic fields for debugging.
    insufficient_diag: dict[str, Any] | None = None
    if analysis is not None and analysis.get("status") == "insufficient_data":
        features = analysis.get("features") or {}
        logger.info(
            "sensor_analysis suppressed insufficient_data device_external_id=%s measurement_run_id=%s "
            "run_event_count=%d recognized_metric_count=%d sensor_kinds_received=%s",
            req.device_external_id,
            measurement_run_id,
            analysis.get("event_count", 0),
            features.get("metric_count", 0),
            features.get("sensor_kinds", []),
        )
        insufficient_diag = {
            "run_event_count": analysis.get("event_count", 0),
            "recognized_metric_count": features.get("metric_count", 0),
            "sensor_kinds_received": features.get("sensor_kinds", []),
            "min_samples_threshold": SENSOR_MODEL_MIN_SAMPLES,
        }
        analysis = None

    if len(events) == 1:
        resp: dict[str, Any] = {"ok": True, "stored": stored_count == 1, "analysis": analysis}
    else:
        resp = {
            "ok": True,
            "received": len(events),
            "stored": stored_count,
            "duplicates": duplicate_count,
            "analysis": analysis,
        }
    if insufficient_diag is not None:
        resp.update(insufficient_diag)
    return resp

@app.get("/api/sensors/last")
def last(
    device_external_id: str,
    kind: str,
    n: int = Query(10, ge=1, le=100),
    session_id: str | None = None,
    measurement_run_id: str | None = None,
):
    session_id = _validated_uuid_str(session_id, "session_id")
    measurement_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    where = [
        "device_id = (SELECT id FROM devices WHERE device_external_id=%s)",
        "kind = %s",
    ]
    params: list[Any] = [device_external_id, kind]

    if session_id:
        where.append("session_id = %s")
        params.append(session_id)

    if measurement_run_id:
        where.append("measurement_run_id = %s")
        params.append(measurement_run_id)

    params.append(n)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT ts, seq, data, measurement_run_id::text
            FROM sensor_events
            WHERE {' AND '.join(where)}
            ORDER BY ts DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

    return [
        {"ts": r[0].isoformat(), "seq": r[1], "data": r[2], "measurement_run_id": r[3]}
        for r in rows
    ]


@app.post(
    "/api/measurements/optical/runs",
    response_model=OpticalCaptureRunResp,
    response_model_exclude_none=True,
)
def create_optical_capture_run(
    req: OpticalCaptureRunCreateReq,
    patient_user_id: str = Depends(require_access_user_id),
):
    session_id = _validated_uuid_str(req.session_id, "session_id")
    if not session_id:
        raise HTTPException(400, "session_id is required")

    encounter_id = _validated_uuid_str(req.encounter_id, "encounter_id")
    requested_run_id = _validated_uuid_str(req.measurement_run_id, "measurement_run_id")
    if req.mode != OPTICAL_MEASUREMENT_MODE:
        raise HTTPException(400, f"mode must be '{OPTICAL_MEASUREMENT_MODE}'")

    started_at = req.started_at or _utcnow()
    requested_measurements = _normalize_requested_measurements(req.requested_measurements)

    with db() as conn, conn.cursor() as cur:
        _, resolved_encounter_id = _validate_optical_capture_run_create_context(
            cur,
            patient_user_id=patient_user_id,
            session_id=session_id,
            encounter_id=encounter_id,
            device_external_id=req.device_external_id,
        )

        if requested_run_id:
            existing_row = _fetch_optical_capture_run_row(cur, requested_run_id)
            if existing_row:
                existing_payload = _serialize_optical_capture_run_row(existing_row)
                if (
                    existing_payload["patient_user_id"] != patient_user_id
                    or existing_payload["session_id"] != session_id
                    or existing_payload["device_external_id"] != req.device_external_id
                    or existing_payload["mode"] != OPTICAL_MEASUREMENT_MODE
                ):
                    raise HTTPException(409, "measurement_run_id already belongs to another optical capture run")
                return {
                    "run": existing_payload,
                    "created": False,
                    "already_exists": True,
                    "already_stopped": existing_payload["status"] == OPTICAL_RUN_STATUS_STOPPED,
                }

        cur.execute(
            """
            INSERT INTO optical_capture_runs (
                measurement_run_id,
                patient_user_id,
                encounter_id,
                session_id,
                device_external_id,
                mode,
                status,
                started_at,
                sample_rate_hz,
                requested_measurements
            )
            VALUES (
                COALESCE(%s::uuid, gen_random_uuid()),
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb
            )
            RETURNING measurement_run_id::text
            """,
            (
                requested_run_id,
                patient_user_id,
                resolved_encounter_id,
                session_id,
                req.device_external_id,
                OPTICAL_MEASUREMENT_MODE,
                OPTICAL_RUN_STATUS_ACTIVE,
                started_at,
                req.sample_rate_hz,
                _json.dumps(requested_measurements),
            ),
        )
        run_id_row = cur.fetchone()
        measurement_run_id = run_id_row[0] if run_id_row else None

        if not measurement_run_id:
            raise HTTPException(500, "Failed to create optical capture run")

        run_row = _fetch_optical_capture_run_row(cur, measurement_run_id)
        if not run_row:
            raise HTTPException(500, "Created optical capture run could not be loaded")

    return {
        "run": _serialize_optical_capture_run_row(run_row),
        "created": True,
        "already_exists": False,
        "already_stopped": False,
    }


@app.post(
    "/api/measurements/optical/runs/{measurement_run_id}/chunks",
    response_model=OpticalCaptureChunkResp,
    response_model_exclude_none=True,
)
def append_optical_capture_chunk(
    measurement_run_id: str,
    req: OpticalCaptureChunkReq,
    patient_user_id: str = Depends(require_access_user_id),
):
    validated_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    if not validated_run_id:
        raise HTTPException(400, "measurement_run_id is required")

    chunk_started_at = req.chunk_started_at or req.chunk_ended_at or _utcnow()
    chunk_ended_at = req.chunk_ended_at or req.chunk_started_at or chunk_started_at
    if chunk_ended_at < chunk_started_at:
        raise HTTPException(400, "chunk_ended_at cannot be before chunk_started_at")

    payload_json = jsonable_encoder(req, exclude_none=True)
    ppg_packets = req.ppg_packets or []
    ts1_frames = req.ts1_frames or []
    packet_count = len(ppg_packets)
    sample_count = sum(_estimate_optical_ppg_packet_sample_count(packet) for packet in ppg_packets)
    first_packet_id = _extract_optical_packet_id(ppg_packets[0]) if ppg_packets else None
    last_packet_id = _extract_optical_packet_id(ppg_packets[-1]) if ppg_packets else None
    normalized_ts1_frames = [
        _normalize_ts1_frame_payload(frame, fallback_received_at=chunk_started_at)
        for frame in ts1_frames
    ]
    derived_checksum_failures = sum(1 for frame in normalized_ts1_frames if frame.get("checksum_ok") is False)
    checksum_failure_count_delta = max(int(req.checksum_failure_count_delta or 0), derived_checksum_failures)
    malformed_packet_count_delta = int(req.malformed_packet_count_delta or 0)
    packet_loss_count_delta = int(req.packet_loss_count_delta or 0)
    raw_waveform_available = bool(packet_count or normalized_ts1_frames)
    latest_chunk_at = chunk_ended_at or chunk_started_at

    with db() as conn, conn.cursor() as cur:
        run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not run_row:
            raise HTTPException(404, "Optical capture run not found")

        if run_row[1] != patient_user_id:
            raise HTTPException(403, "Only the run owner can append optical capture chunks")
        run_payload = _serialize_optical_capture_run_row(run_row)

        cur.execute(
            """
            SELECT chunk_started_at,
                   chunk_ended_at,
                   packet_count,
                   sample_count,
                   ts1_frame_count,
                   first_packet_id,
                   last_packet_id,
                   packet_loss_count_delta,
                   payload_json
            FROM optical_capture_chunks
            WHERE measurement_run_id = %s
              AND chunk_index = %s
            LIMIT 1
            """,
            (validated_run_id, req.chunk_index),
        )
        existing_chunk_row = cur.fetchone()
        if existing_chunk_row:
            same_payload = (
                existing_chunk_row[0] == chunk_started_at
                and existing_chunk_row[1] == chunk_ended_at
                and int(existing_chunk_row[2] or 0) == packet_count
                and int(existing_chunk_row[3] or 0) == sample_count
                and int(existing_chunk_row[4] or 0) == len(normalized_ts1_frames)
                and existing_chunk_row[5] == first_packet_id
                and existing_chunk_row[6] == last_packet_id
                and int(existing_chunk_row[7] or 0) == packet_loss_count_delta
                and (existing_chunk_row[8] if isinstance(existing_chunk_row[8], dict) else {}) == payload_json
            )
            if not same_payload:
                raise HTTPException(409, "chunk_index already exists with different payload")
            return {
                "measurement_run_id": validated_run_id,
                "chunk_index": req.chunk_index,
                "stored": False,
                "duplicate": True,
                "run": run_payload,
            }

        if run_payload["status"] != OPTICAL_RUN_STATUS_ACTIVE:
            raise HTTPException(409, "Optical capture run is not active")

        cur.execute(
            """
            INSERT INTO optical_capture_chunks (
                measurement_run_id,
                chunk_index,
                chunk_started_at,
                chunk_ended_at,
                packet_count,
                sample_count,
                ts1_frame_count,
                first_packet_id,
                last_packet_id,
                packet_loss_count_delta,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                validated_run_id,
                req.chunk_index,
                chunk_started_at,
                chunk_ended_at,
                packet_count,
                sample_count,
                len(normalized_ts1_frames),
                first_packet_id,
                last_packet_id,
                packet_loss_count_delta,
                _json.dumps(payload_json),
            ),
        )

        for frame_index, frame_payload in enumerate(normalized_ts1_frames):
            cur.execute(
                """
                INSERT INTO optical_capture_ts1_frames (
                    measurement_run_id,
                    chunk_index,
                    frame_index,
                    received_at,
                    timestamp_ms,
                    raw_line,
                    checksum_ok,
                    sht_temp_c,
                    sht_rh_percent,
                    ens_iaq,
                    ens_eco2_ppm,
                    ens_temp_c,
                    mlx_ambient_c,
                    mlx_object_c,
                    heart_rate_bpm,
                    spo2_percent,
                    red_dc,
                    quality,
                    parsed_payload_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                """,
                (
                    validated_run_id,
                    req.chunk_index,
                    frame_index,
                    frame_payload.get("received_at"),
                    frame_payload.get("timestamp_ms"),
                    frame_payload.get("raw_line"),
                    frame_payload.get("checksum_ok"),
                    frame_payload.get("sht_temp_c"),
                    frame_payload.get("sht_rh_percent"),
                    frame_payload.get("ens_iaq"),
                    frame_payload.get("ens_eco2_ppm"),
                    frame_payload.get("ens_temp_c"),
                    frame_payload.get("mlx_ambient_c"),
                    frame_payload.get("mlx_object_c"),
                    frame_payload.get("heart_rate_bpm"),
                    frame_payload.get("spo2_percent"),
                    frame_payload.get("red_dc"),
                    frame_payload.get("quality"),
                    _json.dumps(frame_payload.get("parsed_payload_json") or {}),
                ),
            )

        cur.execute(
            """
            UPDATE optical_capture_runs
            SET raw_waveform_available = optical_capture_runs.raw_waveform_available OR %s,
                total_chunks = optical_capture_runs.total_chunks + 1,
                total_packets = optical_capture_runs.total_packets + %s,
                total_samples = optical_capture_runs.total_samples + %s,
                ts1_frame_count = optical_capture_runs.ts1_frame_count + %s,
                packet_loss_count = optical_capture_runs.packet_loss_count + %s,
                checksum_failure_count = optical_capture_runs.checksum_failure_count + %s,
                malformed_packet_count = optical_capture_runs.malformed_packet_count + %s,
                latest_chunk_at = GREATEST(COALESCE(optical_capture_runs.latest_chunk_at, %s), %s)
            WHERE measurement_run_id = %s
            """,
            (
                raw_waveform_available,
                packet_count,
                sample_count,
                len(normalized_ts1_frames),
                packet_loss_count_delta,
                checksum_failure_count_delta,
                malformed_packet_count_delta,
                latest_chunk_at,
                latest_chunk_at,
                validated_run_id,
            ),
        )

        updated_run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not updated_run_row:
            raise HTTPException(500, "Updated optical capture run could not be loaded")

    return {
        "measurement_run_id": validated_run_id,
        "chunk_index": req.chunk_index,
        "stored": True,
        "duplicate": False,
        "run": _serialize_optical_capture_run_row(updated_run_row),
    }


@app.post(
    "/api/measurements/optical/runs/{measurement_run_id}/stop",
    response_model=OpticalCaptureRunResp,
    response_model_exclude_none=True,
)
def stop_optical_capture_run(
    measurement_run_id: str,
    req: OpticalCaptureStopReq,
    auth_user_id: str = Depends(require_access_user_id),
):
    validated_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    if not validated_run_id:
        raise HTTPException(400, "measurement_run_id is required")

    with db() as conn, conn.cursor() as cur:
        run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not run_row:
            raise HTTPException(404, "Optical capture run not found")

        if run_row[1] != auth_user_id:
            raise HTTPException(403, "Only the run owner can stop the optical capture run")
        run_payload = _serialize_optical_capture_run_row(run_row)
        if run_payload["status"] == OPTICAL_RUN_STATUS_STOPPED:
            analysis_payload = _resolve_optical_capture_run_analysis(cur, run_payload=run_payload)
            return {
                "run": run_payload,
                "analysis": analysis_payload,
                "created": False,
                "already_exists": True,
                "already_stopped": True,
            }

        stopped_at = req.stopped_at or _utcnow()
        if stopped_at < run_payload["started_at"]:
            raise HTTPException(400, "stopped_at cannot be before started_at")

        cur.execute(
            """
            UPDATE optical_capture_runs
            SET status = %s,
                stopped_at = %s
            WHERE measurement_run_id = %s
            """,
            (OPTICAL_RUN_STATUS_STOPPED, stopped_at, validated_run_id),
        )
        updated_run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not updated_run_row:
            raise HTTPException(500, "Stopped optical capture run could not be loaded")
        updated_run_payload = _serialize_optical_capture_run_row(updated_run_row)
        analysis_payload = _resolve_optical_capture_run_analysis(cur, run_payload=updated_run_payload)

    return {
        "run": updated_run_payload,
        "analysis": analysis_payload,
        "created": False,
        "already_exists": True,
        "already_stopped": False,
    }


@app.get(
    "/api/measurements/optical/runs/{measurement_run_id}",
    response_model=OpticalCaptureRunResp,
    response_model_exclude_none=True,
)
def get_optical_capture_run(
    measurement_run_id: str,
    auth_user_id: str = Depends(require_access_user_id),
):
    validated_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    if not validated_run_id:
        raise HTTPException(400, "measurement_run_id is required")

    with db() as conn, conn.cursor() as cur:
        run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not run_row:
            raise HTTPException(404, "Optical capture run not found")
        _authorize_user_read_access(auth_user_id, run_row[1])
        run_payload = _serialize_optical_capture_run_row(run_row)
        analysis_payload = _resolve_optical_capture_run_analysis(cur, run_payload=run_payload)

    return {
        "run": run_payload,
        "analysis": analysis_payload,
        "created": False,
        "already_exists": True,
        "already_stopped": bool(run_row[8] or run_row[6] == OPTICAL_RUN_STATUS_STOPPED),
    }


@app.get(
    "/api/measurement-runs/{measurement_run_id}/analysis",
    response_model=MeasurementRunAnalysisResp,
    response_model_exclude_none=True,
)
def get_measurement_run_analysis(
    measurement_run_id: str,
    auth_user_id: str = Depends(require_access_user_id),
):
    validated_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    if not validated_run_id:
        raise HTTPException(400, "measurement_run_id is required")

    with db() as conn, conn.cursor() as cur:
        run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not run_row:
            raise HTTPException(404, "Optical capture run not found")
        _authorize_user_read_access(auth_user_id, run_row[1])
        run_payload = _serialize_optical_capture_run_row(run_row)
        return _resolve_optical_capture_run_analysis(cur, run_payload=run_payload)


@app.get(
    "/api/measurements/optical/runs/{measurement_run_id}/waveform",
    response_model=OpticalWaveformResp,
    response_model_exclude_none=True,
)
def get_optical_capture_waveform(
    measurement_run_id: str,
    from_ts: str | None = Query(None),
    to_ts: str | None = Query(None),
    max_points: int = Query(OPTICAL_WAVEFORM_DEFAULT_MAX_POINTS, ge=10, le=OPTICAL_WAVEFORM_MAX_POINTS),
    series: str | None = Query(None),
    resolution: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    validated_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    if not validated_run_id:
        raise HTTPException(400, "measurement_run_id is required")

    from_dt = _parse_datetime_like(from_ts) if from_ts else None
    if from_ts and from_dt is None:
        raise HTTPException(400, "from_ts must be an ISO 8601 timestamp or epoch milliseconds")
    to_dt = _parse_datetime_like(to_ts) if to_ts else None
    if to_ts and to_dt is None:
        raise HTTPException(400, "to_ts must be an ISO 8601 timestamp or epoch milliseconds")
    if from_dt and to_dt and to_dt < from_dt:
        raise HTTPException(400, "to_ts cannot be before from_ts")

    requested_series = _parse_optical_series_query(series)
    resolution_ms = _parse_optical_resolution_ms(resolution)

    with db() as conn, conn.cursor() as cur:
        run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
        if not run_row:
            raise HTTPException(404, "Optical capture run not found")
        _authorize_user_read_access(auth_user_id, run_row[1])
        return _build_optical_waveform_payload(
            cur,
            measurement_run_id=validated_run_id,
            from_dt=from_dt,
            to_dt=to_dt,
            max_points=max_points,
            requested_series=requested_series,
            resolution_ms=resolution_ms,
        )


@app.get("/api/measurements/optical/runs/{measurement_run_id}/stream")
async def stream_optical_capture_waveform(
    request: Request,
    measurement_run_id: str,
    from_ts: str | None = Query(None),
    to_ts: str | None = Query(None),
    max_points: int = Query(OPTICAL_WAVEFORM_DEFAULT_MAX_POINTS, ge=10, le=OPTICAL_WAVEFORM_MAX_POINTS),
    series: str | None = Query(None),
    resolution: str | None = Query(None),
    poll_seconds: float = Query(STREAM_DEFAULT_POLL_SECONDS, ge=1.0, le=STREAM_MAX_POLL_SECONDS),
    auth_user_id: str = Depends(require_access_user_id),
):
    validated_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")
    if not validated_run_id:
        raise HTTPException(400, "measurement_run_id is required")

    from_dt = _parse_datetime_like(from_ts) if from_ts else None
    if from_ts and from_dt is None:
        raise HTTPException(400, "from_ts must be an ISO 8601 timestamp or epoch milliseconds")
    to_dt = _parse_datetime_like(to_ts) if to_ts else None
    if to_ts and to_dt is None:
        raise HTTPException(400, "to_ts must be an ISO 8601 timestamp or epoch milliseconds")
    if from_dt and to_dt and to_dt < from_dt:
        raise HTTPException(400, "to_ts cannot be before from_ts")

    requested_series = _parse_optical_series_query(series)
    resolution_ms = _parse_optical_resolution_ms(resolution)
    effective_poll_seconds = _stream_poll_seconds(poll_seconds)

    def _load_stream_payload() -> tuple[tuple[Any, ...], dict[str, Any]]:
        with db() as conn, conn.cursor() as cur:
            run_row = _fetch_optical_capture_run_row(cur, validated_run_id)
            if not run_row:
                raise HTTPException(404, "Optical capture run not found")
            _authorize_user_read_access(auth_user_id, run_row[1])
            run_payload = _serialize_optical_capture_run_row(run_row)
            signature = (
                run_payload["status"],
                _iso(run_payload.get("stopped_at")),
                _iso(run_payload.get("latest_chunk_at")),
                run_payload["total_chunks"],
                run_payload["total_packets"],
                run_payload["total_samples"],
                run_payload["ts1_frame_count"],
                run_payload["packet_loss_count"],
                run_payload["checksum_failure_count"],
                run_payload["malformed_packet_count"],
            )
            waveform_payload = _build_optical_waveform_payload(
                cur,
                measurement_run_id=validated_run_id,
                from_dt=from_dt,
                to_dt=to_dt,
                max_points=max_points,
                requested_series=requested_series,
                resolution_ms=resolution_ms,
            )
            return signature, waveform_payload

    initial_signature, initial_payload = await run_in_threadpool(_load_stream_payload)

    async def event_stream():
        last_signature = initial_signature
        last_payload = initial_payload
        last_heartbeat = time.monotonic()
        yield _sse_frame(
            "ready",
            {
                "server_time": _utcnow(),
                "poll_seconds": effective_poll_seconds,
                "measurement_run_id": validated_run_id,
            },
        )
        yield _sse_frame("snapshot", initial_payload)

        while True:
            if await request.is_disconnected():
                break
            try:
                current_signature, current_payload = await run_in_threadpool(_load_stream_payload)
            except HTTPException as exc:
                yield _sse_frame("error", {"detail": exc.detail, "status_code": exc.status_code})
                break

            if current_signature != last_signature:
                delta_payload = _compute_optical_waveform_delta(last_payload, current_payload)
                delta_payload["server_time"] = _utcnow()
                yield _sse_frame("changes", delta_payload)
                last_signature = current_signature
                last_payload = current_payload
                last_heartbeat = time.monotonic()
            elif time.monotonic() - last_heartbeat >= STREAM_HEARTBEAT_SECONDS:
                yield b": heartbeat\n\n"
                last_heartbeat = time.monotonic()

            await asyncio.sleep(effective_poll_seconds)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

@app.get(
    "/api/measurements/history",
    response_model=MeasurementHistoryResp,
    response_model_exclude_none=True,
)
def measurement_history(
    user_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)
    return build_measurement_history(
        user_id=target_user_id,
        limit=limit,
        include_messages=auth_user_id == target_user_id,
    )

@app.get("/api/measurements/trends", response_model=MeasurementTrendsResp)
def measurement_trends(
    user_id: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)
    return build_measurement_trends(user_id=target_user_id)

@app.get("/api/measurements/analysis", response_model=MeasurementAnalysisResp)
def measurement_analysis(
    user_id: str | None = Query(None),
    measurement_run_id: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)

    validated_run_id = None
    if measurement_run_id is not None:
        try:
            validated_run_id = str(uuid.UUID(measurement_run_id))
        except ValueError as exc:
            raise HTTPException(400, "measurement_run_id must be a valid UUID") from exc

    where = [
        """
        EXISTS (
            SELECT 1
            FROM user_sessions us
            WHERE us.user_id = %s
              AND (
                  us.id = sa.session_id
                  OR (sa.encounter_id IS NOT NULL AND us.encounter_id = sa.encounter_id)
              )
        )
        """
    ]
    params: list[Any] = [target_user_id]

    if validated_run_id:
        where.append("sa.measurement_run_id = %s")
        params.append(validated_run_id)

    params.append(limit)

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT sa.measurement_run_id::text,
                   sa.session_id::text,
                   sa.encounter_id::text,
                   sa.analyzer,
                   sa.scope,
                   sa.status,
                   sa.score,
                   sa.confidence,
                   sa.summary,
                   sa.event_count,
                   sa.findings,
                   sa.latest_values,
                   sa.features,
                   sa.model,
                   sa.analyzed_at
            FROM sensor_analysis sa
            WHERE {' AND '.join(where)}
            ORDER BY sa.analyzed_at DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

    return {"analyses": [_sensor_analysis_from_row(row) for row in rows]}


def _histogram_percentile(histogram: list[int], quantile: float) -> int:
    if not histogram:
        return 0
    total = sum(int(value) for value in histogram)
    if total <= 0:
        return 0
    target = max(0.0, min(1.0, quantile)) * total
    cumulative = 0
    for idx, count in enumerate(histogram):
        cumulative += int(count)
        if cumulative >= target:
            return idx
    return len(histogram) - 1


def _extract_binary_regions(mask_image: Image.Image, score_image: Image.Image, *, min_pixels: int) -> list[dict[str, Any]]:
    width, height = mask_image.size
    mask_px = mask_image.load()
    score_px = score_image.load()
    visited = bytearray(width * height)
    regions: list[dict[str, Any]] = []

    for y in range(height):
        row_offset = y * width
        for x in range(width):
            idx = row_offset + x
            if visited[idx] or mask_px[x, y] == 0:
                continue

            stack = [(x, y)]
            visited[idx] = 1

            min_x = max_x = x
            min_y = max_y = y
            area = 0
            score_sum = 0.0

            while stack:
                cx, cy = stack.pop()
                if mask_px[cx, cy] == 0:
                    continue

                area += 1
                score_sum += float(score_px[cx, cy])
                if cx < min_x:
                    min_x = cx
                if cx > max_x:
                    max_x = cx
                if cy < min_y:
                    min_y = cy
                if cy > max_y:
                    max_y = cy

                for nx, ny in ((cx - 1, cy), (cx + 1, cy), (cx, cy - 1), (cx, cy + 1)):
                    if nx < 0 or ny < 0 or nx >= width or ny >= height:
                        continue
                    nidx = ny * width + nx
                    if visited[nidx]:
                        continue
                    visited[nidx] = 1
                    if mask_px[nx, ny] > 0:
                        stack.append((nx, ny))

            if area < min_pixels:
                continue

            regions.append(
                {
                    "min_x": min_x,
                    "min_y": min_y,
                    "max_x": max_x,
                    "max_y": max_y,
                    "area": area,
                    "score_mean": (score_sum / area) if area else 0.0,
                }
            )

    return regions


def _classify_visual_region(image: Image.Image, bbox: tuple[int, int, int, int]) -> tuple[str, str]:
    x1, y1, x2, y2 = bbox
    if x2 <= x1 or y2 <= y1:
        return "visible_skin_variation", "Visual variation detected."

    crop = image.crop((x1, y1, x2, y2))
    if crop.width == 0 or crop.height == 0:
        return "visible_skin_variation", "Visual variation detected."

    stat = ImageStat.Stat(crop)
    r_mean, g_mean, b_mean = stat.mean[:3]
    r_std, g_std, b_std = stat.stddev[:3]

    # Convert mean RGB to HSV (values in 0..1)
    h, s, v = colorsys.rgb_to_hsv(r_mean / 255.0, g_mean / 255.0, b_mean / 255.0)
    hue_deg = h * 360.0
    texture_score = (r_std + g_std + b_std) / 3.0  # mean per-channel std dev

    # Approximate Lab a* (red-green axis): positive = erythema / redness
    red_dominance = r_mean - (g_mean + b_mean) / 2.0

    is_red_hue = hue_deg < 20 or hue_deg > 340  # near 0° / 360°
    is_reddish  = red_dominance > 10.0 and s > 0.10 and is_red_hue
    is_dark     = v < 0.35
    is_brown    = 15 < hue_deg < 55 and s > 0.18 and v < 0.52

    if is_reddish:
        return (
            "localized_redness",
            f"Elevated red-channel dominance (+{red_dominance:.1f}) and warm hue "
            f"({hue_deg:.0f}°) suggest erythema or local inflammation.",
        )
    if is_dark or is_brown:
        tone = "dark" if is_dark else "brown-toned"
        return (
            "pigmented_area",
            f"Region is {tone} (brightness {v:.2f}, saturation {s:.2f}); "
            "consistent with elevated melanin or pigmentation concentration.",
        )
    if texture_score > 28.0:
        return (
            "textured_lesion",
            f"High intra-region texture variance (σ={texture_score:.1f}) may indicate "
            "surface irregularity or heterogeneous lesion texture.",
        )
    return (
        "visible_skin_variation",
        f"Subtle visual variation (brightness {v:.2f}); visual review recommended.",
    )


def _draw_findings_overlay(image: Image.Image, findings: list[dict[str, Any]]) -> Image.Image:
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    stroke_width = max(2, int(round(min(image.size) * 0.005)))
    color_map = {
        "localized_redness": (255, 99, 71),
        "pigmented_area": (255, 191, 0),
        "visible_skin_variation": (80, 170, 255),
        "textured_lesion": (180, 100, 255),
    }

    for finding in findings:
        bbox = finding.get("bbox") or []
        if len(bbox) != 4:
            continue
        x1, y1, x2, y2 = [int(v) for v in bbox]
        label = str(finding.get("label") or "visible_skin_variation")
        confidence = float(finding.get("confidence") or 0.0)
        color = color_map.get(label, (80, 170, 255))

        draw.rectangle((x1, y1, x2, y2), outline=color, width=stroke_width)

        caption = f"{label.replace('_', ' ')} {int(round(confidence * 100))}%"
        if hasattr(draw, "textbbox"):
            left, top, right, bottom = draw.textbbox((0, 0), caption, font=font)
            text_w = right - left
            text_h = bottom - top
        else:
            text_w, text_h = font.getsize(caption)

        text_x = x1
        text_y = max(0, y1 - text_h - 8)
        box_right = min(image.width, text_x + text_w + 8)
        box_bottom = min(image.height, text_y + text_h + 6)
        draw.rectangle((text_x, text_y, box_right, box_bottom), fill=color)
        draw.text((text_x + 4, text_y + 3), caption, fill=(15, 15, 15), font=font)

    return image


def _encode_preview_base64(image_bytes: bytes, max_side: int = 1024) -> str:
    with Image.open(io.BytesIO(image_bytes)) as raw:
        image = raw.convert("RGB")
        width, height = image.size
        if max(width, height) > max_side:
            ratio = max_side / float(max(width, height))
            resized = (max(1, int(round(width * ratio))), max(1, int(round(height * ratio))))
            resampling = getattr(Image, "Resampling", Image)
            image = image.resize(resized, resample=resampling.LANCZOS)

        output = io.BytesIO()
        image.save(output, format="JPEG", quality=85, optimize=True)
        return base64.b64encode(output.getvalue()).decode("ascii")


def _read_image_object_bytes(*, bucket: str | None, key: str | None) -> bytes | None:
    if not bucket or not key:
        return None
    try:
        body = S3.get_object(Bucket=bucket, Key=key)["Body"]
        return body.read()
    except Exception as exc:
        logger.debug("Unable to read image object s3://%s/%s: %s", bucket, key, exc)
        return None


def _delete_image_object(*, bucket: str | None, key: str | None) -> None:
    if not bucket or not key:
        return
    try:
        S3.delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        logger.warning("Unable to delete image object s3://%s/%s: %s", bucket, key, exc)


def _build_image_preview_base64(*, bucket: str | None, key: str | None) -> str | None:
    image_bytes = _read_image_object_bytes(bucket=bucket, key=key)
    if not image_bytes:
        return None
    try:
        return _encode_preview_base64(image_bytes, max_side=IMAGE_PREVIEW_MAX_SIDE)
    except Exception as exc:
        logger.debug("Unable to build image preview for s3://%s/%s: %s", bucket, key, exc)
        return None


# ---------------------------------------------------------------------------
# Computer-vision backbone  (MobileNetV3-Small, ImageNet-pretrained)
# Loaded lazily on first analysis request; falls back gracefully if torchvision
# is not yet installed in the container.
# ---------------------------------------------------------------------------
_CV_MODEL: Any = None
_CV_MODEL_LOADED: bool = False


def _load_cv_model() -> Any:
    """Return a torchvision MobileNetV3-Small in eval mode, or None on failure."""
    global _CV_MODEL, _CV_MODEL_LOADED
    if _CV_MODEL_LOADED:
        return _CV_MODEL
    _CV_MODEL_LOADED = True
    try:
        import torchvision.models as tvm
        weights = tvm.MobileNet_V3_Small_Weights.IMAGENET1K_V1
        model = tvm.mobilenet_v3_small(weights=weights)
        model.eval()
        _CV_MODEL = model
        logger.info("CV backbone (MobileNetV3-Small) loaded successfully.")
    except Exception as exc:
        logger.warning("CV backbone unavailable (%s); using heuristic fallback.", exc)
        _CV_MODEL = None
    return _CV_MODEL


def _compute_cnn_saliency(image: Image.Image) -> "Image.Image | None":
    """
    Run MobileNetV3-Small on *image* and return a grayscale PIL image (same size
    as *image*) whose pixel values represent the mean spatial activation from the
    final conv block.  Returns None when the backbone is unavailable.
    """
    model = _load_cv_model()
    if model is None:
        return None
    try:
        import torch
        import torchvision.transforms.functional as tvf

        resampling = getattr(Image, "Resampling", Image)
        resized = image.resize((224, 224), resample=resampling.BILINEAR)
        inp = tvf.to_tensor(resized)  # [3, 224, 224]  float32 0..1
        inp = tvf.normalize(inp, mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        inp = inp.unsqueeze(0)        # [1, 3, 224, 224]

        captured: dict[str, Any] = {}

        def _hook(_mod: Any, _inp: Any, out: Any) -> None:
            captured["feat"] = out.detach()

        handle = model.features[-1].register_forward_hook(_hook)
        with torch.no_grad():
            model(inp)
        handle.remove()

        feat = captured.get("feat")     # [1, C, H', W']
        if feat is None:
            return None

        sal = feat[0].mean(dim=0)       # [H', W']
        sal = torch.relu(sal)
        s_min, s_max = float(sal.min()), float(sal.max())
        if s_max - s_min < 1e-8:
            return None

        sal_np = ((sal - s_min) / (s_max - s_min) * 255.0).to(torch.uint8).numpy()
        sal_img = Image.fromarray(sal_np, mode="L")
        sal_img = sal_img.resize(image.size, resample=resampling.BILINEAR)
        return sal_img
    except Exception as exc:
        logger.debug("CNN saliency failed: %s", exc)
        return None


def _analyze_visual_image(image_bytes: bytes) -> dict[str, Any]:
    try:
        with Image.open(io.BytesIO(image_bytes)) as raw:
            image = ImageOps.exif_transpose(raw).convert("RGB")
    except Exception as exc:
        raise HTTPException(400, "Uploaded content is not a valid image") from exc

    width, height = image.size
    if width < 24 or height < 24:
        raise HTTPException(400, "Image is too small to analyze")

    max_overlay_side = 2200
    if max(width, height) > max_overlay_side:
        ratio = max_overlay_side / float(max(width, height))
        resized = (max(1, int(round(width * ratio))), max(1, int(round(height * ratio))))
        resampling = getattr(Image, "Resampling", Image)
        image = image.resize(resized, resample=resampling.LANCZOS)
        width, height = image.size

    analysis_max_side = 640
    if max(width, height) > analysis_max_side:
        ratio = analysis_max_side / float(max(width, height))
        analysis_size = (max(1, int(round(width * ratio))), max(1, int(round(height * ratio))))
        resampling = getattr(Image, "Resampling", Image)
        analysis_image = image.resize(analysis_size, resample=resampling.BILINEAR)
    else:
        analysis_image = image.copy()

    analysis_width, analysis_height = analysis_image.size
    scale_x = width / float(analysis_width)
    scale_y = height / float(analysis_height)

    # --- Region-of-interest detection -------------------------------------------
    # Primary: CNN spatial saliency from MobileNetV3-Small backbone.
    # Fallback: Gaussian residual heuristic (used when torchvision is not yet
    # installed or the model fails to load).
    cnn_sal = _compute_cnn_saliency(analysis_image)
    using_cnn = cnn_sal is not None

    if using_cnn:
        score_image: Image.Image = cnn_sal  # type: ignore[assignment]
        # Keep top 18 % of activations as candidate regions.
        threshold = max(30, _histogram_percentile(cnn_sal.histogram(), 0.82))
        mask = cnn_sal.point(lambda v: 255 if v >= threshold else 0, mode="L")
        mask = mask.filter(ImageFilter.MaxFilter(5)).filter(ImageFilter.MinFilter(3))
    else:
        gray = analysis_image.convert("L")
        blur_radius = max(2.0, min(8.0, max(analysis_width, analysis_height) / 140.0))
        blurred = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        score_image = ImageChops.difference(gray, blurred)
        threshold = max(20, _histogram_percentile(score_image.histogram(), 0.965))
        mask = score_image.point(lambda value: 255 if value >= threshold else 0, mode="L")
        mask = mask.filter(ImageFilter.MaxFilter(3)).filter(ImageFilter.MinFilter(3))

    min_pixels = max(30, int(analysis_width * analysis_height * 0.0005))
    regions = _extract_binary_regions(mask, score_image, min_pixels=min_pixels)
    regions.sort(key=lambda item: (float(item.get("score_mean") or 0.0), int(item.get("area") or 0)), reverse=True)

    findings: list[dict[str, Any]] = []
    full_area = float(width * height)

    for region in regions:
        x1 = max(0, min(width - 1, int(round(region["min_x"] * scale_x))))
        y1 = max(0, min(height - 1, int(round(region["min_y"] * scale_y))))
        x2 = max(x1 + 1, min(width, int(round((region["max_x"] + 1) * scale_x))))
        y2 = max(y1 + 1, min(height, int(round((region["max_y"] + 1) * scale_y))))

        box_area = float((x2 - x1) * (y2 - y1))
        area_ratio = box_area / full_area if full_area > 0 else 0.0
        if area_ratio <= 0.0004 or area_ratio >= 0.45:
            continue

        label, note = _classify_visual_region(image, (x1, y1, x2, y2))
        score_norm = float(region.get("score_mean") or 0.0) / 255.0
        if using_cnn:
            # CNN score normalization: backbone activations give a more reliable signal.
            confidence = max(0.35, min(0.95, 0.38 + score_norm * 0.47 + min(area_ratio * 1.5, 0.10)))
        else:
            confidence = max(0.30, min(0.98, 0.35 + score_norm * 0.45 + min(area_ratio * 3.0, 0.18)))

        findings.append(
            {
                "label": label,
                "confidence": round(confidence, 3),
                "bbox": [x1, y1, x2, y2],
                "area_ratio": round(area_ratio, 5),
                "note": note,
                "method": "cnn-saliency" if using_cnn else "heuristic",
            }
        )

        if len(findings) >= 3:
            break

    findings.sort(key=lambda item: float(item.get("confidence") or 0.0), reverse=True)
    overlay_image = _draw_findings_overlay(image.copy(), findings)
    output = io.BytesIO()
    overlay_image.save(output, format="JPEG", quality=90, optimize=True)

    if findings:
        summary = f"{len(findings)} visible area(s) of interest highlighted for clinician review. This output is non-diagnostic."
        status = "review_recommended"
    else:
        summary = "No prominent visual area of interest detected by the automated non-diagnostic screen."
        status = "no_prominent_finding"

    return {
        "status": status,
        "summary": summary,
        "width": width,
        "height": height,
        "findings": findings,
        "overlay_bytes": output.getvalue(),
    }


class ImageAnalysisFinding(BaseModel):
    label: str
    confidence: float = Field(ge=0.0, le=1.0)
    bbox: list[int] = Field(default_factory=list)
    area_ratio: float = Field(ge=0.0, le=1.0)
    note: str | None = None
    method: str | None = None


class ImageAnalysisResp(BaseModel):
    status: str
    non_diagnostic: bool = True
    summary: str
    source_image_id: int | None = None
    source_mime: str | None = None
    overlay_image_id: int | None = None
    overlay_mime: str | None = None
    width: int
    height: int
    findings: list[ImageAnalysisFinding] = Field(default_factory=list)
    overlay_preview_base64: str | None = None


class ImageAnalysisHistoryItem(BaseModel):
    analysis_id: int
    source_image_id: int
    overlay_image_id: int | None = None
    source_image_content_url: str | None = None
    overlay_image_content_url: str | None = None
    source_preview_base64: str | None = None
    overlay_preview_base64: str | None = None
    session_id: str | None = None
    encounter_id: str | None = None
    measurement_run_id: str | None = None
    status: str
    summary: str
    findings_json: list[dict[str, Any]] = Field(default_factory=list)
    findings: list[dict[str, Any]] = Field(default_factory=list)
    non_diagnostic: bool = True
    analyzed_at: str | None = None
    created_at: str | None = None
    retention_policy: str | None = None
    retention_expires_at: str | None = None
    retention_meta: dict[str, Any] = Field(default_factory=dict)


class ImageAnalysisHistoryResp(BaseModel):
    items: list[ImageAnalysisHistoryItem] = Field(default_factory=list)


class ImageAnalysisByImageResp(BaseModel):
    analysis: ImageAnalysisHistoryItem


class ImageAnalysisDeleteResp(BaseModel):
    ok: bool = True
    analysis_id: int
    source_image_id: int
    overlay_image_id: int | None = None

DoctorPatientDashboardResp.model_rebuild()

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
    device_external_id: str | None = Form(None),
    session_id: str | None = Form(None),
    auth_user_id: str | None = Depends(optional_access_user_id),
):
    data = await file.read()
    sha = hashlib.sha256(data).hexdigest()
    ext = os.path.splitext(file.filename or "")[-1].lower() or ".jpg"

    source_device_external_id = (device_external_id or "").strip() or None
    source_session_id = _validated_uuid_str(session_id, "session_id") if session_id else None

    device_id: str | None = None
    with db() as conn, conn.cursor() as cur:
        if source_device_external_id:
            cur.execute("SELECT id::text FROM devices WHERE device_external_id=%s LIMIT 1;", (source_device_external_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Device not registered")
            device_id = row[0]
        elif not auth_user_id:
            raise HTTPException(401, "device_external_id is required unless authenticated user token is provided")

        if source_session_id:
            if auth_user_id:
                cur.execute("SELECT 1 FROM user_sessions WHERE id=%s AND user_id=%s LIMIT 1;", (source_session_id, auth_user_id))
                if not cur.fetchone():
                    raise HTTPException(403, "session_id does not belong to authenticated user")
            else:
                cur.execute("SELECT 1 FROM user_sessions WHERE id=%s LIMIT 1;", (source_session_id,))
                if not cur.fetchone():
                    raise HTTPException(400, "Unknown session_id")
        elif auth_user_id:
            source_session_id = _resolve_latest_user_device_session_id(
                cur,
                user_id=auth_user_id,
                device_id=device_id,
            )

    day = time.strftime("%Y/%m/%d")
    key_prefix = source_device_external_id or (f"user-{auth_user_id[:8]}" if auth_user_id else "manual")
    key = f"{key_prefix}/{day}/{sha}{ext}"

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
            ON CONFLICT (bucket, key)
            DO UPDATE SET
                session_id = COALESCE(images.session_id, EXCLUDED.session_id),
                mime = COALESCE(images.mime, EXCLUDED.mime),
                width = COALESCE(images.width, EXCLUDED.width),
                height = COALESCE(images.height, EXCLUDED.height)
            RETURNING id::text, ts;
            """,
            (source_session_id, device_id, BKT, key, sha, file.content_type, w, h),
        )
        img_id, ts = cur.fetchone()

    return {"id": img_id, "device_id": device_id, "session_id": source_session_id, "key": key, "ts": ts.isoformat()}


@app.post("/api/images/analyze", response_model=ImageAnalysisResp)
async def analyze_image(
    image_id: int | None = Form(None),
    file: UploadFile | None = File(None),
    device_external_id: str | None = Form(None),
    session_id: str | None = Form(None),
    measurement_run_id: str | None = Form(None),
    include_overlay_base64: bool = Form(True),
    auth_user_id: str | None = Depends(optional_access_user_id),
):
    if image_id is None and file is None:
        raise HTTPException(400, "Provide either image_id or file")
    if image_id is not None and file is not None:
        raise HTTPException(400, "Use only one source: image_id or file")

    source_image_id: int | None = None
    source_key: str | None = None
    source_mime: str | None = None
    source_device_id: str | None = None
    source_session_id: str | None = None
    source_device_external_id: str | None = None
    source_bytes: bytes | None = None
    source_image_ts: datetime | None = None
    requested_session_id = _validated_uuid_str(session_id, "session_id") if session_id else None
    source_measurement_run_id = _validated_uuid_str(measurement_run_id, "measurement_run_id")

    if image_id is not None:
        with db() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.id,
                       i.ts,
                       i.bucket,
                       i.key,
                       i.mime,
                       i.device_id::text,
                       i.session_id::text,
                       d.device_external_id
                FROM images i
                LEFT JOIN devices d ON d.id = i.device_id
                WHERE i.id = %s
                LIMIT 1;
                """,
                (image_id,),
            )
            row = cur.fetchone()

            if row:
                source_image_id = int(row[0])
                source_image_ts = row[1]
                source_bucket = row[2]
                source_key = row[3]
                source_mime = row[4]
                source_device_id = row[5]
                source_session_id = row[6]
                source_device_external_id = row[7]

                if requested_session_id:
                    if auth_user_id:
                        cur.execute("SELECT 1 FROM user_sessions WHERE id=%s AND user_id=%s LIMIT 1;", (requested_session_id, auth_user_id))
                        if not cur.fetchone():
                            raise HTTPException(403, "session_id does not belong to authenticated user")
                    else:
                        cur.execute("SELECT 1 FROM user_sessions WHERE id=%s LIMIT 1;", (requested_session_id,))
                        if not cur.fetchone():
                            raise HTTPException(400, "Unknown session_id")
                    source_session_id = requested_session_id
                elif not source_session_id and auth_user_id:
                    source_session_id = _resolve_latest_user_device_session_id(
                        cur,
                        user_id=auth_user_id,
                        device_id=source_device_id,
                    )

                if source_session_id and source_image_id is not None:
                    cur.execute(
                        """
                        UPDATE images
                        SET session_id = COALESCE(session_id, %s::uuid)
                        WHERE id = %s
                        """,
                        (source_session_id, source_image_id),
                    )

        if not row:
            raise HTTPException(404, "image_id not found")

        try:
            source_bytes = S3.get_object(Bucket=source_bucket, Key=source_key)["Body"].read()
        except Exception as exc:
            raise HTTPException(502, "Unable to fetch image from object storage") from exc

        if not source_bytes:
            raise HTTPException(404, "Stored image is empty")

    else:
        assert file is not None
        source_bytes = await file.read()
        if not source_bytes:
            raise HTTPException(400, "Empty image file")

        source_device_external_id = (device_external_id or "").strip() or None

        source_session_id = requested_session_id
        source_mime = file.content_type or "application/octet-stream"

        with db() as conn, conn.cursor() as cur:
            if source_device_external_id:
                cur.execute("SELECT id::text FROM devices WHERE device_external_id=%s LIMIT 1;", (source_device_external_id,))
                device_row = cur.fetchone()
                if not device_row:
                    raise HTTPException(404, "Device not registered")
                source_device_id = device_row[0]
            elif not auth_user_id:
                raise HTTPException(401, "device_external_id is required unless authenticated user token is provided")

            if source_session_id:
                if auth_user_id:
                    cur.execute("SELECT 1 FROM user_sessions WHERE id=%s AND user_id=%s LIMIT 1;", (source_session_id, auth_user_id))
                    if not cur.fetchone():
                        raise HTTPException(403, "session_id does not belong to authenticated user")
                else:
                    cur.execute("SELECT 1 FROM user_sessions WHERE id=%s LIMIT 1;", (source_session_id,))
                    if not cur.fetchone():
                        raise HTTPException(400, "Unknown session_id")
            elif auth_user_id:
                source_session_id = _resolve_latest_user_device_session_id(
                    cur,
                    user_id=auth_user_id,
                    device_id=source_device_id,
                )

    analysis = _analyze_visual_image(source_bytes)
    width = int(analysis["width"])
    height = int(analysis["height"])
    findings = analysis["findings"]
    overlay_bytes = analysis["overlay_bytes"]

    if image_id is None:
        ext = os.path.splitext(file.filename or "")[-1].lower() if file else ""
        if ext not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
            ext = ".jpg"

        source_sha = hashlib.sha256(source_bytes).hexdigest()
        day = time.strftime("%Y/%m/%d")
        source_prefix = source_device_external_id or (f"user-{auth_user_id[:8]}" if auth_user_id else "manual")
        source_key = f"{source_prefix}/{day}/{source_sha}-{uuid.uuid4().hex[:8]}{ext}"

        try:
            S3.put_object(
                Bucket=BKT,
                Key=source_key,
                Body=source_bytes,
                ContentType=source_mime or "application/octet-stream",
            )
        except Exception as exc:
            raise HTTPException(502, "Unable to store source image") from exc

        with db() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO images(session_id, device_id, bucket, key, sha256, mime, width, height)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, ts;
                """,
                (source_session_id, source_device_id, BKT, source_key, source_sha, source_mime, width, height),
            )
            source_image_id, source_image_ts = cur.fetchone()
            source_image_id = int(source_image_id)

    overlay_sha = hashlib.sha256(overlay_bytes).hexdigest()
    day = time.strftime("%Y/%m/%d")
    overlay_prefix = source_device_external_id or (f"user-{auth_user_id[:8]}" if auth_user_id else "unassigned")
    overlay_key = f"{overlay_prefix}/{day}/{overlay_sha}-{uuid.uuid4().hex[:8]}-overlay.jpg"

    analyzed_at = _utcnow()

    try:
        S3.put_object(
            Bucket=BKT,
            Key=overlay_key,
            Body=overlay_bytes,
            ContentType="image/jpeg",
        )
    except Exception as exc:
        raise HTTPException(502, "Unable to store analysis overlay") from exc

    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO images(session_id, device_id, bucket, key, sha256, mime, width, height)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (source_session_id, source_device_id, BKT, overlay_key, overlay_sha, "image/jpeg", width, height),
        )
        overlay_image_id = int(cur.fetchone()[0])

        resolved_encounter_id = _resolve_session_encounter_id(cur, session_id=source_session_id)
        resolved_measurement_run_id = _resolve_image_measurement_run_id(
            cur,
            measurement_run_id=source_measurement_run_id,
            session_id=source_session_id,
            anchor_ts=source_image_ts,
        )
        visual_analysis = _build_visual_history_analysis(
            status=analysis["status"],
            summary=analysis["summary"],
            findings=findings,
            source_image_id=source_image_id,
            overlay_image_id=overlay_image_id,
            analyzed_at=analyzed_at,
        )
        resolved_user_id = _resolve_image_analysis_user_id(
            cur,
            auth_user_id=auth_user_id,
            session_id=source_session_id,
            device_id=source_device_id,
        )
        retention_policy, retention_expires_at, retention_meta = _build_image_analysis_retention(analyzed_at)
        _persist_image_analysis(
            cur,
            source_image_id=source_image_id,
            overlay_image_id=overlay_image_id,
            user_id=resolved_user_id,
            session_id=source_session_id,
            encounter_id=resolved_encounter_id,
            device_id=source_device_id,
            measurement_run_id=resolved_measurement_run_id,
            analysis=visual_analysis,
            analyzed_at=analyzed_at,
            retention_policy=retention_policy,
            retention_expires_at=retention_expires_at,
            retention_meta=retention_meta,
        )

    overlay_preview_base64 = _encode_preview_base64(overlay_bytes) if include_overlay_base64 else None

    return {
        "status": analysis["status"],
        "non_diagnostic": True,
        "summary": analysis["summary"],
        "source_image_id": source_image_id,
        "source_mime": source_mime,
        "overlay_image_id": overlay_image_id,
        "overlay_mime": "image/jpeg",
        "width": width,
        "height": height,
        "findings": findings,
        "overlay_preview_base64": overlay_preview_base64,
    }


def _fetch_image_analysis_history_rows(
    cur,
    *,
    user_id: str,
    limit: int,
    image_id: int | None = None,
) -> list[tuple[Any, ...]]:
    where = [
        """
        (
            COALESCE(ia.user_id, us.user_id, d.user_id) = %s::uuid
            OR (
                COALESCE(ia.user_id, us.user_id, d.user_id) IS NULL
                AND ia.encounter_id IS NOT NULL
                AND EXISTS (
                    SELECT 1
                    FROM user_sessions ux
                    WHERE ux.user_id = %s::uuid
                      AND ux.encounter_id = ia.encounter_id
                )
            )
        )
        """
    ]
    params: list[Any] = [user_id, user_id]

    if image_id is not None:
        where.append("(ia.source_image_id = %s OR ia.overlay_image_id = %s)")
        params.extend([image_id, image_id])

    params.append(limit)

    cur.execute(
        f"""
        SELECT ia.id,
               ia.source_image_id,
               ia.overlay_image_id,
               COALESCE(ia.session_id::text, src.session_id::text),
               COALESCE(ia.encounter_id::text, us.encounter_id::text, us.id::text),
               ia.measurement_run_id::text,
               ia.status,
               ia.summary,
               ia.non_diagnostic,
               COALESCE(ia.findings_json, ia.findings, '[]'::jsonb),
               ia.created_at,
               ia.analyzed_at,
               ia.retention_policy,
               ia.retention_expires_at,
               ia.retention_meta,
               ia.analysis,
               src.bucket,
               src.key,
               src.mime,
               ovl.bucket,
               ovl.key,
               ovl.mime
        FROM image_analyses ia
        JOIN images src ON src.id = ia.source_image_id
        LEFT JOIN images ovl ON ovl.id = ia.overlay_image_id
        LEFT JOIN user_sessions us ON us.id = COALESCE(ia.session_id, src.session_id)
        LEFT JOIN devices d ON d.id = COALESCE(ia.device_id, src.device_id)
        WHERE {' AND '.join(where)}
        ORDER BY ia.analyzed_at DESC, ia.id DESC
        LIMIT %s
        """,
        params,
    )
    return cur.fetchall()


def _fetch_owned_image_analysis_delete_row(
    cur,
    *,
    user_id: str,
    image_id: int,
) -> tuple[Any, ...] | None:
    cur.execute(
        """
        SELECT ia.id,
               ia.source_image_id,
               ia.overlay_image_id,
               src.bucket,
               src.key,
               ovl.bucket,
               ovl.key
        FROM image_analyses ia
        JOIN images src ON src.id = ia.source_image_id
        LEFT JOIN images ovl ON ovl.id = ia.overlay_image_id
        LEFT JOIN user_sessions us ON us.id = COALESCE(ia.session_id, src.session_id)
        LEFT JOIN devices d ON d.id = COALESCE(ia.device_id, src.device_id)
        WHERE (
                COALESCE(ia.user_id, us.user_id, d.user_id) = %s::uuid
                OR (
                    COALESCE(ia.user_id, us.user_id, d.user_id) IS NULL
                    AND ia.encounter_id IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM user_sessions ux
                        WHERE ux.user_id = %s::uuid
                          AND ux.encounter_id = ia.encounter_id
                    )
                )
              )
          AND (ia.source_image_id = %s OR ia.overlay_image_id = %s)
        ORDER BY ia.analyzed_at DESC, ia.id DESC
        LIMIT 1
        """,
        (user_id, user_id, image_id, image_id),
    )
    return cur.fetchone()


@app.get(
    "/api/images/history",
    response_model=ImageAnalysisHistoryResp,
    response_model_exclude_none=True,
)
def image_analysis_history(
    user_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)
    include_previews = auth_user_id != target_user_id

    with db() as conn, conn.cursor() as cur:
        rows = _fetch_image_analysis_history_rows(
            cur,
            user_id=target_user_id,
            limit=limit,
        )

    return {
        "items": [
            _image_analysis_history_item_from_row(
                row,
                include_previews=include_previews,
                target_user_id=target_user_id,
            )
            for row in rows
        ]
    }


@app.get(
    "/api/images/analysis",
    response_model=ImageAnalysisByImageResp,
    response_model_exclude_none=True,
)
def image_analysis_by_image_id(
    image_id: int = Query(..., ge=1),
    user_id: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)
    include_previews = auth_user_id != target_user_id

    with db() as conn, conn.cursor() as cur:
        rows = _fetch_image_analysis_history_rows(
            cur,
            user_id=target_user_id,
            image_id=image_id,
            limit=1,
        )

    if not rows:
        raise HTTPException(404, "No visual analysis found for image_id")

    return {
        "analysis": _image_analysis_history_item_from_row(
            rows[0],
            include_previews=include_previews,
            target_user_id=target_user_id,
        )
    }


@app.delete(
    "/api/images/{image_id}",
    response_model=ImageAnalysisDeleteResp,
    response_model_exclude_none=True,
)
def delete_image_analysis(
    image_id: int,
    auth_user_id: str = Depends(require_access_user_id),
):
    if image_id < 1:
        raise HTTPException(400, "image_id must be positive")

    with db() as conn, conn.cursor() as cur:
        row = _fetch_owned_image_analysis_delete_row(
            cur,
            user_id=auth_user_id,
            image_id=image_id,
        )
        if not row:
            raise HTTPException(404, "No visual analysis found for image_id")

        analysis_id = int(row[0])
        source_image_id = int(row[1])
        overlay_image_id = int(row[2]) if row[2] is not None else None
        source_bucket = row[3]
        source_key = row[4]
        overlay_bucket = row[5]
        overlay_key = row[6]

        cur.execute("DELETE FROM image_analyses WHERE id = %s", (analysis_id,))
        if overlay_image_id is not None:
            cur.execute("DELETE FROM images WHERE id = %s", (overlay_image_id,))
        cur.execute("DELETE FROM images WHERE id = %s", (source_image_id,))

    for bucket, key in {(source_bucket, source_key), (overlay_bucket, overlay_key)}:
        _delete_image_object(bucket=bucket, key=key)

    return {
        "ok": True,
        "analysis_id": analysis_id,
        "source_image_id": source_image_id,
        "overlay_image_id": overlay_image_id,
    }


@app.get("/api/images/{image_id}/content")
def image_content(
    image_id: int,
    user_id: str | None = Query(None),
    auth_user_id: str = Depends(require_access_user_id),
):
    if image_id < 1:
        raise HTTPException(400, "image_id must be positive")

    target_user_id = user_id or auth_user_id
    try:
        target_user_id = str(uuid.UUID(target_user_id))
    except ValueError as exc:
        raise HTTPException(400, "user_id must be a valid UUID") from exc

    _authorize_user_read_access(auth_user_id, target_user_id)

    with db() as conn, conn.cursor() as cur:
        rows = _fetch_image_analysis_history_rows(
            cur,
            user_id=target_user_id,
            image_id=image_id,
            limit=1,
        )

    if not rows:
        raise HTTPException(404, "Image not found for this user")

    row = rows[0]
    source_image_id = int(row[1])
    overlay_image_id = int(row[2]) if row[2] is not None else None
    if image_id == source_image_id:
        bucket = row[16]
        key = row[17]
        mime = row[18] or "application/octet-stream"
    elif overlay_image_id is not None and image_id == overlay_image_id:
        bucket = row[19]
        key = row[20]
        mime = row[21] or "image/jpeg"
    else:
        raise HTTPException(404, "Image not found for this user")

    image_bytes = _read_image_object_bytes(bucket=bucket, key=key)
    if not image_bytes:
        raise HTTPException(404, "Stored image content is unavailable")

    return Response(
        content=image_bytes,
        media_type=mime,
        headers={"Cache-Control": "private, no-store"},
    )

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
    encounter_id = _validated_uuid_str(req.encounter_id, "encounter_id")
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

            if encounter_id:
                cur.execute(
                    """
                    SELECT 1
                    FROM user_sessions
                    WHERE encounter_id = %s AND user_id <> %s
                    LIMIT 1;
                    """,
                    (encounter_id, req.user_id),
                )
                if cur.fetchone():
                    raise HTTPException(400, "encounter_id belongs to another user")

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
                    """
                    SELECT COALESCE(encounter_id, id)::text
                    FROM user_sessions
                    WHERE id=%s AND user_id=%s
                    LIMIT 1;
                    """,
                    (session_id, req.user_id),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(400, "Invalid session_id for this user")
                session_encounter_id = row[0]
                if encounter_id and session_encounter_id and encounter_id != session_encounter_id:
                    raise HTTPException(400, "encounter_id does not match session_id")
                encounter_id = session_encounter_id or encounter_id

            if not session_id:
                if encounter_id:
                    cur.execute(
                        """
                        SELECT us.id::text
                        FROM user_sessions us
                        WHERE us.user_id = %s
                          AND us.encounter_id = %s
                          AND COALESCE(us.meta->>'source', '') = 'api_chat'
                        ORDER BY COALESCE(us.ended_at, us.started_at) DESC, us.started_at DESC
                        LIMIT 1;
                        """,
                        (req.user_id, encounter_id),
                    )
                    row = cur.fetchone()
                    if row:
                        session_id = row[0]

                if not session_id:
                    encounter_id = encounter_id or str(uuid.uuid4())
                    cur.execute(
                        """
                        INSERT INTO user_sessions (user_id, started_at, meta, encounter_id)
                        VALUES (%s, now(), %s::jsonb, %s)
                        RETURNING id::text;
                        """,
                        (req.user_id, _json.dumps({"source": "api_chat"}), encounter_id),
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
                assistant_summary = _coalesce_nonempty(chat_result.get("assistant_summary"), assistant_message)
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
                    "last_assistant_summary": assistant_summary,
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
        chat_result["encounter_id"] = encounter_id
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
    
