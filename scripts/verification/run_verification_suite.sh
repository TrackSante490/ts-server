#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  run_verification_suite.sh <mode> [options]

Modes:
  quick         Run short response, ingest, and integrity checks.
  performance   Run response-time and concurrency-capacity checks.
  full          Run response, ingest, integrity, and concurrency checks.
  uptime-short  Run a short uptime probe to confirm the monitor works.
  uptime-week   Run the one-week uptime monitor in the foreground.

Options:
  --base-url URL        Override TRACKSANTE_API_BASE_URL (default: http://127.0.0.1:8080)
  --health-path PATH    Health endpoint for uptime/latency checks (default: /api/health)
  --label-prefix TEXT   Prefix applied to generated run labels
  --skip-db-record      Do not insert verification summary rows into Postgres
  -h, --help            Show this help text

Examples:
  ./scripts/verification/run_verification_suite.sh quick
  ./scripts/verification/run_verification_suite.sh performance --base-url https://api.tracksante.com
  ./scripts/verification/run_verification_suite.sh uptime-week
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MODE="${1:-}"
if [[ -z "${MODE}" ]]; then
  usage
  exit 1
fi
shift

BASE_URL="${TRACKSANTE_API_BASE_URL:-http://127.0.0.1:8080}"
HEALTH_PATH="/api/health"
LABEL_PREFIX=""
SKIP_DB_RECORD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="$2"
      shift 2
      ;;
    --health-path)
      HEALTH_PATH="$2"
      shift 2
      ;;
    --label-prefix)
      LABEL_PREFIX="$2"
      shift 2
      ;;
    --skip-db-record)
      SKIP_DB_RECORD=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

timestamp() {
  date -u +"%Y%m%dT%H%M%SZ"
}

make_label() {
  local suffix="$1"
  local stamp
  stamp="$(timestamp)"
  if [[ -n "${LABEL_PREFIX}" ]]; then
    printf '%s-%s-%s' "${LABEL_PREFIX}" "${suffix}" "${stamp}"
  else
    printf '%s-%s' "${suffix}" "${stamp}"
  fi
}

run_python() {
  local script_path="$1"
  shift
  echo "==> python3 ${script_path} $*"
  python3 "${script_path}" "$@"
}

run_quick() {
  run_python "${SCRIPT_DIR}/http_load_test.py" \
    --base-url "${BASE_URL}" \
    --path "${HEALTH_PATH}" \
    --requests 60 \
    --concurrency 6 \
    --label "$(make_label response)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"

  run_python "${SCRIPT_DIR}/ingest_verifier.py" \
    --base-url "${BASE_URL}" \
    --packets 15 \
    --concurrency 4 \
    --label "$(make_label ingest)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"

  run_python "${SCRIPT_DIR}/data_integrity_check.py" \
    --base-url "${BASE_URL}" \
    --packets 10 \
    --wait-seconds 5 \
    --label "$(make_label integrity)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"
}

run_performance() {
  run_python "${SCRIPT_DIR}/http_load_test.py" \
    --base-url "${BASE_URL}" \
    --path "${HEALTH_PATH}" \
    --requests 200 \
    --concurrency 20 \
    --label "$(make_label latency)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"

  run_python "${SCRIPT_DIR}/http_load_test.py" \
    --base-url "${BASE_URL}" \
    --path "${HEALTH_PATH}" \
    --ladder 5,10,20,40,80 \
    --requests-per-step 50 \
    --max-p95-ms 2000 \
    --max-error-rate 0.01 \
    --label "$(make_label concurrency)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"
}

run_full() {
  run_python "${SCRIPT_DIR}/http_load_test.py" \
    --base-url "${BASE_URL}" \
    --path "${HEALTH_PATH}" \
    --requests 200 \
    --concurrency 20 \
    --label "$(make_label response)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"

  run_python "${SCRIPT_DIR}/ingest_verifier.py" \
    --base-url "${BASE_URL}" \
    --packets 25 \
    --concurrency 5 \
    --label "$(make_label ingest)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"

  run_python "${SCRIPT_DIR}/data_integrity_check.py" \
    --base-url "${BASE_URL}" \
    --packets 15 \
    --wait-seconds 10 \
    --label "$(make_label integrity)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"

  run_python "${SCRIPT_DIR}/http_load_test.py" \
    --base-url "${BASE_URL}" \
    --path "${HEALTH_PATH}" \
    --ladder 5,10,20,40,80 \
    --requests-per-step 50 \
    --max-p95-ms 2000 \
    --max-error-rate 0.01 \
    --label "$(make_label concurrency)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"
}

run_uptime_short() {
  run_python "${SCRIPT_DIR}/uptime_probe.py" \
    --base-url "${BASE_URL}" \
    --endpoint "${HEALTH_PATH}" \
    --interval-seconds 60 \
    --checks 10 \
    --label "$(make_label uptime-short)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"
}

run_uptime_week() {
  run_python "${SCRIPT_DIR}/uptime_probe.py" \
    --base-url "${BASE_URL}" \
    --endpoint "${HEALTH_PATH}" \
    --interval-seconds 60 \
    --checks 10080 \
    --label "$(make_label uptime-week)" \
    "${EXTRA_SCRIPT_FLAGS[@]}"
}

maybe_skip_db=()
EXTRA_SCRIPT_FLAGS=()
if [[ "${SKIP_DB_RECORD}" -eq 1 ]]; then
  EXTRA_SCRIPT_FLAGS+=(--skip-db-record)
fi

case "${MODE}" in
  quick)
    run_quick
    ;;
  performance)
    run_performance
    ;;
  full)
    run_full
    ;;
  uptime-short)
    run_uptime_short
    ;;
  uptime-week)
    run_uptime_week
    ;;
  *)
    echo "Unknown mode: ${MODE}" >&2
    usage
    exit 1
    ;;
esac

echo
echo "Verification run completed."
echo "Open Grafana and check: TrackSante Verification Overview"
echo "Artifacts are under: ${ROOT_DIR}/test-results"
