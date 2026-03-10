#!/usr/bin/env python3
import csv
import json
import math
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
RESULTS_ROOT = REPO_ROOT / "test-results"
DEFAULT_API_BASE_URL = os.getenv("TRACKSANTE_API_BASE_URL", "http://127.0.0.1:8080")
DEFAULT_DB_SERVICE = os.getenv("TRACKSANTE_DB_SERVICE", "db")
DEFAULT_DB_NAME = os.getenv("TRACKSANTE_DB_NAME", "postgres")
DEFAULT_DB_USER = os.getenv("TRACKSANTE_DB_USER", "postgres")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(dt: datetime | None = None) -> str:
    value = dt or utcnow()
    return value.isoformat()


def timestamp_label() -> str:
    return utcnow().strftime("%Y%m%dT%H%M%SZ")


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def ensure_results_dir(test_key: str, label: str | None = None) -> Path:
    run_label = label or timestamp_label()
    path = RESULTS_ROOT / test_key / run_label
    path.mkdir(parents=True, exist_ok=True)
    return path


def relative_to_repo(path: Path) -> str:
    return str(path.resolve().relative_to(REPO_ROOT.resolve()))


def write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_url(base_url: str, path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", path_or_url.lstrip("/"))


def parse_headers(raw_headers: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in raw_headers:
        name, sep, value = item.partition(":")
        if not sep:
            raise ValueError(f"Invalid header {item!r}; expected NAME:VALUE")
        headers[name.strip()] = value.strip()
    return headers


def http_request(
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    json_body: object | None = None,
    timeout_seconds: float = 10.0,
) -> dict:
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)

    data: bytes | None = None
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(
        url=url,
        data=data,
        headers=request_headers,
        method=method.upper(),
    )

    started = time.perf_counter()
    response_body = b""
    status_code = 0
    error_message = ""
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.getcode()
            response_body = response.read()
            ok = 200 <= status_code < 300
    except urllib.error.HTTPError as exc:
        status_code = exc.code
        response_body = exc.read()
        ok = False
        error_message = str(exc)
    except Exception as exc:  # pragma: no cover - exercised in real runs
        ok = False
        error_message = str(exc)
    duration_ms = (time.perf_counter() - started) * 1000.0

    parsed_body: object
    if response_body:
        try:
            parsed_body = json.loads(response_body.decode("utf-8"))
        except Exception:
            parsed_body = response_body.decode("utf-8", errors="replace")
    else:
        parsed_body = None

    return {
        "timestamp": isoformat(),
        "method": method.upper(),
        "url": url,
        "status_code": status_code,
        "ok": ok,
        "duration_ms": round(duration_ms, 3),
        "error": error_message,
        "body": parsed_body,
    }


def sql_literal(value: object | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def run_psql(sql: str) -> list[str]:
    command = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "exec",
        "-T",
        DEFAULT_DB_SERVICE,
        "psql",
        "-U",
        DEFAULT_DB_USER,
        "-d",
        DEFAULT_DB_NAME,
        "-v",
        "ON_ERROR_STOP=1",
        "-P",
        "footer=off",
        "-t",
        "-A",
        "-F",
        "\t",
        "-c",
        sql,
    ]
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "psql command failed")
    return [line for line in result.stdout.splitlines() if line.strip()]


def run_psql_scalar(sql: str) -> str:
    rows = run_psql(sql)
    return rows[0].strip() if rows else ""


def create_verification_user(label: str) -> str:
    auth_sub = f"verification-{label}-{uuid.uuid4()}"
    sql = f"""
    INSERT INTO users (auth_issuer, auth_sub, name)
    VALUES ('verification-script', {sql_literal(auth_sub)}, {sql_literal(f'Verification {label}')})
    RETURNING id::text;
    """
    return run_psql_scalar(sql)


def insert_verification_run(summary: dict) -> None:
    summary_json = json.dumps(summary, sort_keys=True)
    sql = f"""
    INSERT INTO verification_runs (
      test_key,
      scenario,
      status,
      target,
      started_at,
      finished_at,
      total_requests,
      successful_requests,
      failed_requests,
      success_rate,
      p50_ms,
      p95_ms,
      max_concurrency,
      sustainable_concurrency,
      records_expected,
      records_verified,
      exact_match_rate,
      downtime_events,
      artifacts_path,
      summary
    ) VALUES (
      {sql_literal(summary.get('test_key'))},
      {sql_literal(summary.get('scenario'))},
      {sql_literal(summary.get('status', 'ok'))},
      {sql_literal(summary.get('target'))},
      {sql_literal(summary.get('started_at'))}::timestamptz,
      {sql_literal(summary.get('finished_at'))}::timestamptz,
      {sql_literal(summary.get('total_requests'))},
      {sql_literal(summary.get('successful_requests'))},
      {sql_literal(summary.get('failed_requests'))},
      {sql_literal(summary.get('success_rate'))},
      {sql_literal(summary.get('p50_ms'))},
      {sql_literal(summary.get('p95_ms'))},
      {sql_literal(summary.get('max_concurrency'))},
      {sql_literal(summary.get('sustainable_concurrency'))},
      {sql_literal(summary.get('records_expected'))},
      {sql_literal(summary.get('records_verified'))},
      {sql_literal(summary.get('exact_match_rate'))},
      {sql_literal(summary.get('downtime_events'))},
      {sql_literal(summary.get('artifacts_path'))},
      {sql_literal(summary_json)}::jsonb
    );
    """
    run_psql(sql)
