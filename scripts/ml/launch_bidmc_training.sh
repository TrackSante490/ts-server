#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: launch_bidmc_training.sh [--run-id ID] [--runs-dir DIR] [--follow] [--] [trainer args...]

Starts a detached GPU-backed BIDMC training job in Docker so it survives SSH disconnects.

Examples:
  scripts/ml/launch_bidmc_training.sh --follow
  scripts/ml/launch_bidmc_training.sh -- --quality-profile max --epochs 400
  scripts/ml/launch_bidmc_training.sh --run-id my-run -- --dataset-dir /workspace/data/bidmc
EOF
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNS_DIR=""
RUN_ID=""
FOLLOW_LOGS=0
TRAINER_ARGS=()

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
      TRAINER_ARGS+=("$@")
      break
      ;;
    *)
      TRAINER_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${RUNS_DIR}" ]]; then
  RUNS_DIR="${REPO_ROOT}/ml_runs"
fi
if [[ -z "${RUN_ID}" ]]; then
  RUN_ID="bidmc-$(date -u +%Y%m%dT%H%M%SZ)"
fi

case "${RUN_ID}" in
  *[!A-Za-z0-9._-]*)
    echo "run-id may only contain letters, numbers, dot, underscore, and hyphen" >&2
    exit 1
    ;;
esac

RUN_DIR="${RUNS_DIR}/${RUN_ID}"
REPORT_DIR="${RUN_DIR}/reports"
DATASET_DIR="${RUN_DIR}/dataset"
OUTPUT_PATH="${RUN_DIR}/bidmc_public_model.pt"
LOG_FILE="${REPORT_DIR}/training.log"
INPUT_SOURCE="${DATASET_DIR}"
if [[ -e "${RUN_DIR}" && -n "$(find "${RUN_DIR}" -mindepth 1 -maxdepth 1 2>/dev/null)" ]]; then
  echo "run directory already exists and is not empty: ${RUN_DIR}" >&2
  exit 1
fi
mkdir -p "${REPORT_DIR}"

for arg in "${TRAINER_ARGS[@]}"; do
  case "${arg}" in
    --output|--output=*|--report-dir|--report-dir=*|--log-file|--log-file=*)
      echo "launch_bidmc_training.sh manages --output, --report-dir, and --log-file automatically." >&2
      exit 1
      ;;
  esac
done

HAS_DATASET_ARG=0
for arg in "${TRAINER_ARGS[@]}"; do
  case "${arg}" in
    --dataset-dir|--dataset-dir=*|--download-dir|--download-dir=*)
      HAS_DATASET_ARG=1
      ;;
  esac
done

for ((i = 0; i < ${#TRAINER_ARGS[@]}; i++)); do
  case "${TRAINER_ARGS[$i]}" in
    --dataset-dir)
      INPUT_SOURCE="${TRAINER_ARGS[$((i + 1))]:-}"
      ;;
    --dataset-dir=*)
      INPUT_SOURCE="${TRAINER_ARGS[$i]#--dataset-dir=}"
      ;;
    --download-dir)
      INPUT_SOURCE="${TRAINER_ARGS[$((i + 1))]:-}"
      ;;
    --download-dir=*)
      INPUT_SOURCE="${TRAINER_ARGS[$i]#--download-dir=}"
      ;;
  esac
done

CONTAINER_NAME="capstone-sensor-trainer-${RUN_ID}"
COMPOSE_CMD=(docker compose -f "${REPO_ROOT}/docker-compose.yml" --profile ml)

"${COMPOSE_CMD[@]}" build sensor-trainer >/dev/null

RUN_CMD=(
  "${COMPOSE_CMD[@]}"
  run
  -d
  --rm
  --name "${CONTAINER_NAME}"
  --no-deps
  sensor-trainer
  --quality-profile max
  --device cuda
  --output "/workspace/ml_runs/${RUN_ID}/bidmc_public_model.pt"
  --report-dir "/workspace/ml_runs/${RUN_ID}/reports"
  --log-file "/workspace/ml_runs/${RUN_ID}/reports/training.log"
)
if [[ "${HAS_DATASET_ARG}" -eq 0 ]]; then
  RUN_CMD+=(--download-dir "/workspace/ml_runs/${RUN_ID}/dataset")
fi
RUN_CMD+=("${TRAINER_ARGS[@]}")

CONTAINER_ID="$("${RUN_CMD[@]}")"

cat > "${RUN_DIR}/job.json" <<EOF
{
  "run_id": "${RUN_ID}",
  "container_name": "${CONTAINER_NAME}",
  "container_id": "${CONTAINER_ID}",
  "artifact_path": "${OUTPUT_PATH}",
  "report_dir": "${REPORT_DIR}",
  "log_file": "${LOG_FILE}",
  "input_source": "${INPUT_SOURCE}",
  "started_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "Started training run: ${RUN_ID}"
echo "Container: ${CONTAINER_NAME}"
echo "Artifact: ${OUTPUT_PATH}"
echo "Reports: ${REPORT_DIR}"
echo "Log file: ${LOG_FILE}"
echo "Metadata: ${RUN_DIR}/job.json"
echo "Follow persisted log: tail -f '${LOG_FILE}'"
echo "Follow container logs: docker logs -f '${CONTAINER_NAME}'"
echo "Stop the run: docker stop '${CONTAINER_NAME}'"

if [[ "${FOLLOW_LOGS}" -eq 1 ]]; then
  exec docker logs -f "${CONTAINER_NAME}"
fi
