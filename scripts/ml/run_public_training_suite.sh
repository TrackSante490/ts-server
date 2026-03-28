#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_public_training_suite.sh --run-root DIR [options]

Runs the public sensor model trainers sequentially.

Options:
  --run-root DIR              Root directory for suite outputs (required)
  --publish-dir DIR           Directory to copy final .pt models into
  --device DEVICE             auto, cpu, or cuda (default: cuda)
  --quality-profile PROFILE   fast, balanced, or max for all trainers (default: max)
  --bidmc-profile PROFILE     Override quality profile for BIDMC
  --ppg-profile PROFILE       Override quality profile for PPG-DaLiA
  --occupancy-profile PROFILE Override quality profile for Occupancy Detection
  --seed INT                  Random seed (default: 42)
  --enable-sweeps             After the base pass, run additional profile/seed variants and keep the best artifact
  --sweep-profiles LIST       Comma-separated profiles for sweep candidates (default: balanced,max)
  --sweep-extra-seeds LIST    Comma-separated extra seeds for sweep candidates (default: 1337)
  --skip-bidmc                Skip BIDMC
  --skip-ppg-dalia            Skip PPG-DaLiA
  --skip-occupancy            Skip Occupancy Detection
  --bidmc-dataset-dir DIR     Use an existing BIDMC dataset
  --ppg-dalia-dataset-dir DIR Use an existing PPG-DaLiA dataset
  --occupancy-dataset-dir DIR Use an existing Occupancy Detection dataset
EOF
}

RUN_ROOT=""
PUBLISH_DIR="/workspace/api/models"
DEVICE="cuda"
QUALITY_PROFILE="max"
BIDMC_PROFILE=""
PPG_PROFILE=""
OCCUPANCY_PROFILE=""
SEED="42"
ENABLE_SWEEPS=0
SWEEP_PROFILES="balanced,max"
SWEEP_EXTRA_SEEDS="1337"
SKIP_BIDMC=0
SKIP_PPG_DALIA=0
SKIP_OCCUPANCY=0
BIDMC_DATASET_DIR=""
PPG_DALIA_DATASET_DIR=""
OCCUPANCY_DATASET_DIR=""

while (($#)); do
  case "$1" in
    --run-root)
      RUN_ROOT="${2:?missing value for --run-root}"
      shift 2
      ;;
    --publish-dir)
      PUBLISH_DIR="${2:?missing value for --publish-dir}"
      shift 2
      ;;
    --device)
      DEVICE="${2:?missing value for --device}"
      shift 2
      ;;
    --quality-profile)
      QUALITY_PROFILE="${2:?missing value for --quality-profile}"
      shift 2
      ;;
    --bidmc-profile)
      BIDMC_PROFILE="${2:?missing value for --bidmc-profile}"
      shift 2
      ;;
    --ppg-profile)
      PPG_PROFILE="${2:?missing value for --ppg-profile}"
      shift 2
      ;;
    --occupancy-profile)
      OCCUPANCY_PROFILE="${2:?missing value for --occupancy-profile}"
      shift 2
      ;;
    --seed)
      SEED="${2:?missing value for --seed}"
      shift 2
      ;;
    --enable-sweeps)
      ENABLE_SWEEPS=1
      shift
      ;;
    --sweep-profiles)
      SWEEP_PROFILES="${2:?missing value for --sweep-profiles}"
      shift 2
      ;;
    --sweep-extra-seeds)
      SWEEP_EXTRA_SEEDS="${2:?missing value for --sweep-extra-seeds}"
      shift 2
      ;;
    --skip-bidmc)
      SKIP_BIDMC=1
      shift
      ;;
    --skip-ppg-dalia)
      SKIP_PPG_DALIA=1
      shift
      ;;
    --skip-occupancy)
      SKIP_OCCUPANCY=1
      shift
      ;;
    --bidmc-dataset-dir)
      BIDMC_DATASET_DIR="${2:?missing value for --bidmc-dataset-dir}"
      shift 2
      ;;
    --ppg-dalia-dataset-dir)
      PPG_DALIA_DATASET_DIR="${2:?missing value for --ppg-dalia-dataset-dir}"
      shift 2
      ;;
    --occupancy-dataset-dir)
      OCCUPANCY_DATASET_DIR="${2:?missing value for --occupancy-dataset-dir}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${RUN_ROOT}" ]]; then
  echo "--run-root is required" >&2
  exit 1
fi

mkdir -p "${RUN_ROOT}" "${PUBLISH_DIR}"
SUITE_LOG="${RUN_ROOT}/suite.log"

log() {
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "${ts} $*" | tee -a "${SUITE_LOG}"
}

run_trainer() {
  local name="$1"
  shift
  log "starting ${name}"
  "$@" 2>&1 | tee -a "${SUITE_LOG}"
  log "completed ${name}"
}

publish_model() {
  local artifact_path="$1"
  local target_name="$2"
  if [[ -f "${artifact_path}" ]]; then
    cp "${artifact_path}" "${PUBLISH_DIR}/${target_name}"
    log "published $(basename "${artifact_path}") to ${PUBLISH_DIR}/${target_name}"
  fi
}

choose_best_artifact() {
  local summary_path="$1"
  shift
  python /workspace/scripts/ml/evaluate_public_artifacts.py \
    --summary-out "${summary_path}" \
    "$@"
}

run_base_phase() {
  local dataset_name="$1"
  local base_profile="$2"
  local dataset_dir="$3"
  local trainer_script="$4"
  local artifact_name="$5"
  local run_subdir="$6"
  shift 6

  local dataset_root="${RUN_ROOT}/${run_subdir}"
  local report_dir="${dataset_root}/reports"
  local base_artifact="${dataset_root}/${artifact_name}"
  local selection_summary="${dataset_root}/selection_summary.json"
  mkdir -p "${report_dir}"

  local base_args=(
    python "${trainer_script}"
    --quality-profile "${base_profile}"
    --device "${DEVICE}"
    --seed "${SEED}"
    --output "${base_artifact}"
    --report-dir "${report_dir}"
    --log-file "${report_dir}/training.log"
  )
  if [[ -n "${dataset_dir}" ]]; then
    base_args+=(--dataset-dir "${dataset_dir}")
  else
    base_args+=(--download-dir "${dataset_root}/dataset")
  fi

  run_trainer "${dataset_name}" "${base_args[@]}"
  publish_model "${base_artifact}" "${artifact_name}"
}

run_sweep_phase() {
  local dataset_name="$1"
  local base_profile="$2"
  local dataset_dir="$3"
  local trainer_script="$4"
  local artifact_name="$5"
  local run_subdir="$6"
  shift 6

  local dataset_root="${RUN_ROOT}/${run_subdir}"
  local base_artifact="${dataset_root}/${artifact_name}"
  local candidate_paths=("${base_artifact}")
  local selection_summary="${dataset_root}/selection_summary.json"

  if [[ ! -f "${base_artifact}" ]]; then
    log "skipping sweeps for ${dataset_name}; base artifact missing at ${base_artifact}"
    return
  fi

  IFS=',' read -r -a sweep_profiles <<< "${SWEEP_PROFILES}"
  IFS=',' read -r -a sweep_extra_seeds <<< "${SWEEP_EXTRA_SEEDS}"
  local sweep_seeds=("${SEED}")
  local seen_seed=0
  for extra_seed in "${sweep_extra_seeds[@]}"; do
    extra_seed="${extra_seed//[[:space:]]/}"
    [[ -z "${extra_seed}" ]] && continue
    seen_seed=0
    for existing_seed in "${sweep_seeds[@]}"; do
      if [[ "${existing_seed}" == "${extra_seed}" ]]; then
        seen_seed=1
        break
      fi
    done
    if [[ "${seen_seed}" -eq 0 ]]; then
      sweep_seeds+=("${extra_seed}")
    fi
  done

  for sweep_profile in "${sweep_profiles[@]}"; do
    sweep_profile="${sweep_profile//[[:space:]]/}"
    [[ -z "${sweep_profile}" ]] && continue
    for sweep_seed in "${sweep_seeds[@]}"; do
      [[ -z "${sweep_seed}" ]] && continue
      if [[ "${sweep_profile}" == "${base_profile}" && "${sweep_seed}" == "${SEED}" ]]; then
        continue
      fi
      local sweep_id="${sweep_profile}-seed${sweep_seed}"
      local sweep_root="${dataset_root}/sweeps/${sweep_id}"
      local sweep_report_dir="${sweep_root}/reports"
      local sweep_artifact="${sweep_root}/${artifact_name}"
      mkdir -p "${sweep_report_dir}"
      local sweep_args=(
        python "${trainer_script}"
        --quality-profile "${sweep_profile}"
        --device "${DEVICE}"
        --seed "${sweep_seed}"
        --output "${sweep_artifact}"
        --report-dir "${sweep_report_dir}"
        --log-file "${sweep_report_dir}/training.log"
      )
      if [[ -n "${dataset_dir}" ]]; then
        sweep_args+=(--dataset-dir "${dataset_dir}")
      else
        sweep_args+=(--download-dir "${dataset_root}/dataset")
      fi
      run_trainer "${dataset_name}:sweep:${sweep_id}" "${sweep_args[@]}"
      candidate_paths+=("${sweep_artifact}")
    done
  done

  local best_artifact
  best_artifact="$(choose_best_artifact "${selection_summary}" "${candidate_paths[@]}")"
  if [[ -n "${best_artifact}" && -f "${best_artifact}" ]]; then
    publish_model "${best_artifact}" "${artifact_name}"
    log "selected best ${dataset_name} artifact: ${best_artifact}"
  fi
}

: > "${SUITE_LOG}"
log "suite run root=${RUN_ROOT} publish_dir=${PUBLISH_DIR} device=${DEVICE}"
if [[ "${ENABLE_SWEEPS}" -eq 1 ]]; then
  log "sweeps enabled profiles=${SWEEP_PROFILES} extra_seeds=${SWEEP_EXTRA_SEEDS}"
fi

if [[ -z "${BIDMC_PROFILE}" ]]; then
  BIDMC_PROFILE="${QUALITY_PROFILE}"
fi
if [[ -z "${PPG_PROFILE}" ]]; then
  PPG_PROFILE="${QUALITY_PROFILE}"
fi
if [[ -z "${OCCUPANCY_PROFILE}" ]]; then
  OCCUPANCY_PROFILE="${QUALITY_PROFILE}"
fi

if [[ "${SKIP_BIDMC}" -eq 0 ]]; then
  run_base_phase \
    "bidmc" \
    "${BIDMC_PROFILE}" \
    "${BIDMC_DATASET_DIR}" \
    "/workspace/scripts/ml/train_bidmc_public_model.py" \
    "bidmc_public_model.pt" \
    "bidmc"
fi

if [[ "${SKIP_PPG_DALIA}" -eq 0 ]]; then
  run_base_phase \
    "ppg_dalia" \
    "${PPG_PROFILE}" \
    "${PPG_DALIA_DATASET_DIR}" \
    "/workspace/scripts/ml/train_ppg_dalia_public_model.py" \
    "ppg_dalia_public_model.pt" \
    "ppg_dalia"
fi

if [[ "${SKIP_OCCUPANCY}" -eq 0 ]]; then
  run_base_phase \
    "occupancy" \
    "${OCCUPANCY_PROFILE}" \
    "${OCCUPANCY_DATASET_DIR}" \
    "/workspace/scripts/ml/train_occupancy_public_model.py" \
    "occupancy_public_model.pt" \
    "occupancy"
fi

if [[ "${ENABLE_SWEEPS}" -eq 1 ]]; then
  if [[ "${SKIP_BIDMC}" -eq 0 ]]; then
    run_sweep_phase \
      "bidmc" \
      "${BIDMC_PROFILE}" \
      "${BIDMC_DATASET_DIR}" \
      "/workspace/scripts/ml/train_bidmc_public_model.py" \
      "bidmc_public_model.pt" \
      "bidmc"
  fi

  if [[ "${SKIP_PPG_DALIA}" -eq 0 ]]; then
    run_sweep_phase \
      "ppg_dalia" \
      "${PPG_PROFILE}" \
      "${PPG_DALIA_DATASET_DIR}" \
      "/workspace/scripts/ml/train_ppg_dalia_public_model.py" \
      "ppg_dalia_public_model.pt" \
      "ppg_dalia"
  fi

  if [[ "${SKIP_OCCUPANCY}" -eq 0 ]]; then
    run_sweep_phase \
      "occupancy" \
      "${OCCUPANCY_PROFILE}" \
      "${OCCUPANCY_DATASET_DIR}" \
      "/workspace/scripts/ml/train_occupancy_public_model.py" \
      "occupancy_public_model.pt" \
      "occupancy"
  fi
fi

log "suite complete"
