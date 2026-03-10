#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEMD_DIR="${SCRIPT_DIR}/systemd"

ENABLE_TIMER=0
START_NOW=0

usage() {
  cat <<'EOF'
Usage:
  install_uptime_systemd.sh [--enable-timer] [--start-now]

Installs:
  /etc/systemd/system/tracksante-uptime-week.service
  /etc/systemd/system/tracksante-uptime-week.timer

Options:
  --enable-timer   Enable the weekly timer immediately
  --start-now      Start the one-week service immediately
  -h, --help       Show this help text
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --enable-timer)
      ENABLE_TIMER=1
      shift
      ;;
    --start-now)
      START_NOW=1
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

SERVICE_TEMPLATE="${SYSTEMD_DIR}/tracksante-uptime-week.service.template"
TIMER_FILE="${SYSTEMD_DIR}/tracksante-uptime-week.timer"
TMP_FILE="$(mktemp)"

cleanup() {
  rm -f "${TMP_FILE}"
}
trap cleanup EXIT

USER_NAME="${SUDO_USER:-$(id -un)}"
GROUP_NAME="$(id -gn "${USER_NAME}")"

sed \
  -e "s/__USER__/${USER_NAME}/g" \
  -e "s/__GROUP__/${GROUP_NAME}/g" \
  "${SERVICE_TEMPLATE}" > "${TMP_FILE}"

sudo install -m 0644 "${TMP_FILE}" /etc/systemd/system/tracksante-uptime-week.service
sudo install -m 0644 "${TIMER_FILE}" /etc/systemd/system/tracksante-uptime-week.timer
sudo systemctl daemon-reload

if [[ "${ENABLE_TIMER}" -eq 1 ]]; then
  sudo systemctl enable --now tracksante-uptime-week.timer
fi

if [[ "${START_NOW}" -eq 1 ]]; then
  sudo systemctl start tracksante-uptime-week.service
fi

echo "Installed systemd units."
echo "Manual one-week run: sudo systemctl start tracksante-uptime-week.service"
echo "Watch logs:           sudo journalctl -u tracksante-uptime-week.service -f"
echo "Stop early:           sudo systemctl stop tracksante-uptime-week.service"
echo "Enable weekly timer:  sudo systemctl enable --now tracksante-uptime-week.timer"
