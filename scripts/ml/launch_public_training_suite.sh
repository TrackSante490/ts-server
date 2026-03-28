#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: launch_public_training_suite.sh [--run-id ID] [--runs-dir DIR] [--follow] [--] [suite args...]

Starts a detached GPU-backed sequential public-model training suite in Docker so it survives SSH disconnects.
When sweeps are enabled, the suite finishes one base run for each enabled dataset first, then runs sweep variants in the same dataset order.

Examples:
  scripts/ml/launch_public_training_suite.sh --follow
  scripts/ml/launch_public_training_suite.sh -- --quality-profile max
  scripts/ml/launch_public_training_suite.sh -- --quality-profile balanced --enable-sweeps --sweep-profiles balanced,max --sweep-extra-seeds 1337
  scripts/ml/launch_public_training_suite.sh --run-id overnight-a -- --skip-occupancy
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNS_DIR=""
RUN_ID=""
FOLLOW_LOGS=0
SUITE_ARGS=()

while (($#)); do
  case "$1" in
    --run-id)
      RUN_ID="${2:?missing value for --run-id}"
      shift 2
      ;;
    --runs-dir)
      RUNS_DIR="${2:?missing value for --runs-dir}"
      shift 2
      ;;
    --follow)
      FOLLOW_LOGS=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      SUITE_ARGS+=("$@")
      break
      ;;
    *)
      SUITE_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${RUNS_DIR}" ]]; then
  RUNS_DIR="${REPO_ROOT}/ml_runs"
fi
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="public-suite-$(date -u +%Y%m%dT%H%M%SZ)"
fi

case "${RUN_ID}" in
  *[!A-Za-z0-9._-]*)
    echo "run-id may only contain letters, numbers, dot, underscore, and hyphen" >&2
    exit 1
    ;;
esac

RUN_DIR="${RUNS_DIR}/${RUN_ID}"
SUITE_LOG="${RUN_DIR}/suite.log"
PUBLISH_DIR="${REPO_ROOT}/api/models"
if [[ -e "${RUN_DIR}" && -n "$(find "${RUN_DIR}" -mindepth 1 -maxdepth 1 2>/dev/null)" ]]; then
  echo "run directory already exists and is not empty: ${RUN_DIR}" >&2
  exit 1
fi
mkdir -p "${RUN_DIR}"

CONTAINER_NAME="capstone-sensor-trainer-suite-${RUN_ID}"
COMPOSE_CMD=(docker compose -f "${REPO_ROOT}/docker-compose.yml" --profile ml)

"${COMPOSE_CMD[@]}" build sensor-trainer >/dev/null

RUN_CMD=(
  "${COMPOSE_CMD[@]}"
  run
  -d
  --rm
  --name "${CONTAINER_NAME}"
  --no-deps
  --entrypoint bash
  sensor-trainer
  /workspace/scripts/ml/run_public_training_suite.sh
  --run-root "/workspace/ml_runs/${RUN_ID}"
  --publish-dir "/workspace/api/models"
)
RUN_CMD+=("${SUITE_ARGS[@]}")

CONTAINER_ID="$("${RUN_CMD[@]}")"

cat > "${RUN_DIR}/job.json" <<EOF
{
  "run_id": "${RUN_ID}",
  "container_name": "${CONTAINER_NAME}",
  "container_id": "${CONTAINER_ID}",
  "suite_log": "${SUITE_LOG}",
  "publish_dir": "${PUBLISH_DIR}",
  "started_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Started public training suite: ${RUN_ID}"
echo "Container: ${CONTAINER_NAME}"
echo "Run directory: ${RUN_DIR}"
echo "Suite log: ${SUITE_LOG}"
echo "Publish dir: ${PUBLISH_DIR}"
echo "Metadata: ${RUN_DIR}/job.json"
echo "Follow persisted suite log: tail -f '${SUITE_LOG}'"
echo "Follow container logs: docker logs -f '${CONTAINER_NAME}'"
echo "Stop the suite: docker stop '${CONTAINER_NAME}'"

if [[ "${FOLLOW_LOGS}" -eq 1 ]]; then
  exec docker logs -f "${CONTAINER_NAME}"
fi
