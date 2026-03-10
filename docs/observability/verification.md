# TrackSante Verification Scripts

These scripts generate formal test evidence and write the latest summary rows into `verification_runs`, which powers the `TrackSante Verification Overview` Grafana dashboard.

## Outputs

- Raw artifacts: `test-results/<test_key>/<timestamp>/`
- Dashboard summary table: `verification_runs`
- Grafana dashboard: `TrackSante Verification Overview`

## Scripts

- `scripts/verification/uptime_probe.py`
  - Probes a health endpoint repeatedly and records success rate, downtime events, and response-time percentiles.
- `scripts/verification/http_load_test.py`
  - Measures p50/p95 response times under fixed load or finds sustainable concurrency using a concurrency ladder.
- `scripts/verification/ingest_verifier.py`
  - Sends controlled sensor batches and verifies stored row counts.
- `scripts/verification/data_integrity_check.py`
  - Sends known sensor payloads and verifies retrieved records match exactly.
- `scripts/verification/run_verification_suite.sh`
  - Runs the common test bundles with one command.
- `scripts/verification/install_uptime_systemd.sh`
  - Installs a long-running one-week uptime service and an optional weekly timer.

## Recommended Usage

- `quick`
  - Use before demos or after a normal backend change.
  - Runs a short response-time test, ingest check, and integrity check.
- `performance`
  - Use when you want latency and concurrency numbers.
  - Runs a fixed-load response-time test and a concurrency ladder.
- `full`
  - Use before milestone review or final verification.
  - Runs response-time, ingest, integrity, and concurrency tests.
- `uptime-short`
  - Use once to confirm the uptime monitor is writing results correctly.
- `uptime-week`
  - Use for the formal one-week uptime run.

## One-Command Suite Runner

```bash
./scripts/verification/run_verification_suite.sh quick
./scripts/verification/run_verification_suite.sh performance
./scripts/verification/run_verification_suite.sh full
./scripts/verification/run_verification_suite.sh uptime-short
```

Optional flags:

```bash
./scripts/verification/run_verification_suite.sh quick \
  --base-url https://api.tracksante.com \
  --health-path /api/health
```

## One-Week Uptime Monitor

Foreground run:

```bash
./scripts/verification/run_verification_suite.sh uptime-week
```

Systemd install:

```bash
./scripts/verification/install_uptime_systemd.sh
```

Install and enable the weekly timer immediately:

```bash
./scripts/verification/install_uptime_systemd.sh --enable-timer
```

Install and start the one-week run immediately:

```bash
./scripts/verification/install_uptime_systemd.sh --start-now
```

After installation:

```bash
sudo systemctl start tracksante-uptime-week.service
sudo journalctl -u tracksante-uptime-week.service -f
sudo systemctl stop tracksante-uptime-week.service
```

The service template is in `scripts/verification/systemd/`. If your deployment user is not `rishit`, the install script fills in the current user automatically.

## Example Commands

```bash
python3 /srv/capstone/scripts/verification/uptime_probe.py \
  --endpoint /api/health \
  --interval-seconds 60 \
  --checks 10080
```

```bash
python3 /srv/capstone/scripts/verification/http_load_test.py \
  --path /api/health \
  --requests 200 \
  --concurrency 20
```

```bash
python3 /srv/capstone/scripts/verification/http_load_test.py \
  --path /api/health \
  --ladder 5,10,20,40,80 \
  --requests-per-step 50 \
  --max-p95-ms 2000 \
  --max-error-rate 0.01
```

```bash
python3 /srv/capstone/scripts/verification/ingest_verifier.py \
  --packets 50 \
  --concurrency 10
```

```bash
python3 /srv/capstone/scripts/verification/data_integrity_check.py \
  --packets 20 \
  --wait-seconds 30
```

## Verification Pattern

For each test:

1. Run the script and keep the generated `summary.json`.
2. Open Grafana and confirm the new row appears in `TrackSante Verification Overview`.
3. Open the corresponding artifact folder in `test-results/`.
4. Use the artifact files as primary evidence and Grafana as the quick visual summary.

## What To Do In Practice

Normal day-to-day workflow:

1. Run `./scripts/verification/run_verification_suite.sh quick`
2. Open Grafana and check `TrackSante Verification Overview`
3. If someone asks for proof, open the latest artifact folder shown in the Grafana table

For performance verification:

1. Run `./scripts/verification/run_verification_suite.sh performance`
2. Use Grafana for the summary numbers
3. Use `test-results/response_time/.../summary.json` and `test-results/concurrent_capacity/.../summary.json` as the formal evidence

For final validation:

1. Start the one-week uptime monitor
2. Run `./scripts/verification/run_verification_suite.sh full`
3. At the end, use the Grafana dashboard plus the raw files in `test-results/`
