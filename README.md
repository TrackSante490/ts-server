# TrackSanté Local Server Backend

This repo brings up the **local server stack** with Docker Compose. It includes the API, databases, storage, auth, and supporting services. The `cappie_run` runtime is *mounted* for the API to import, but its logic lives in a different repo and is intentionally not documented here.

If you're new: think of this as a self‑contained backend lab. You run one command and get a working API, database, file storage, authentication, and local AI model runtime.

## What Runs Here (In Depth)

Each service is listed below with its role, how it connects to other services, and what it stores or exposes.

| Service | What it does | Key connections and behavior |
| --- | --- | --- |
| `api` | The FastAPI backend that exposes the REST API used by the web app and other clients. It also serves static files under `/assets`. | Connects to Postgres using `DATABASE_URL`, MinIO using `S3_*` vars, and Ollama using `OLLAMA_BASE_URL`. It mounts `./api` as code, `./auth/assets` for static assets, and the `cappie_run` directory for imports (logic documented elsewhere). |
| `migrate` | One‑shot container that applies database migrations at startup. | Uses `DATABASE_URL` and runs `api/db/migrate.sh` with SQL migrations from `api/db/migrations`. It must complete before the API starts. |
| `db` | Primary PostgreSQL database for application metadata, telemetry, and any structured storage used by the API. | Persists data to `pgdata/`. Exposed on port `5432` for local tools. |
| `minio` | Local S3‑compatible object storage (images, files, and large blobs). | Exposes an S3 API on `9000` and a web console on `9001`. Persists to `minio-data/`. Uses credentials from `S3_ACCESS_KEY` and `S3_SECRET_KEY`. |
| `mc-init` | Bootstraps MinIO by creating the bucket defined by `S3_BUCKET_IMAGES`. | Waits until MinIO is ready, then creates the bucket if it doesn’t exist. Runs once and exits. |
| `ollama` | Local LLM runtime used for chat and embeddings. | Runs inside the Docker network (no public port). The API calls it at `http://ollama:11434`. GPU reservation is enabled for NVIDIA devices. Model state is persisted in the `ollama-data` volume. |
| `ollama-init` | Warm‑starts the model runtime by pulling models on startup. | Pulls `OLLAMA_CHAT_MODEL` and `OLLAMA_EMBED_MODEL` defined in `.env`. Runs once after Ollama is reachable. |
| `adminer` | Simple database UI for Postgres. | Accessible at `http://localhost:8081`. Useful for quick schema checks and manual queries. |
| `prometheus` | Scrapes the API `/metrics` endpoint and stores time-series metrics locally. | Powers infrastructure and RAG/LLM monitoring dashboards in Grafana. |
| `grafana` | Self-hosted dashboard UI for infrastructure, product telemetry, and RAG observability. | Provisioned from `observability/grafana/` with Prometheus + Postgres datasources. |
| `authentik-postgresql` | Dedicated Postgres instance for Authentik. | Persists to `authentik-pgdata/`. Separate from the main `db` to keep auth data isolated. |
| `authentik-redis` | Redis backend for Authentik background tasks and caching. | Internal‑only dependency for Authentik server/worker. |
| `authentik-server` | Authentik web UI and API for identity, SSO, and user management. | Runs on `9002` (HTTP) and `9443` (optional HTTPS). Stores media in `authentik-media/` and uses custom templates/branding mounted from this repo. |
| `authentik-worker` | Background job processor for Authentik (emails, tasks, workflows). | Shares config with the Authentik server and connects to Authentik Postgres + Redis. |
| `tracksante-site` | Static marketing site hosted by Nginx. | Serves files from `/srv/tracksante-site` inside the container on port `8088`. |
| `tracksante-app` | Static web application hosted by Nginx. | Serves files from `/srv/tracksante-app` on port `8089`, with a custom Nginx config at `tracksante-app.conf`. |

## How It Fits Together

1. `docker compose up -d` starts everything.
2. `db` starts, then `migrate` applies migrations.
3. `minio` starts, then `mc-init` creates the bucket.
4. `ollama` starts, then `ollama-init` pulls the chat + embed models.
5. `api` connects to Postgres, MinIO, and Ollama using `.env` values.
6. Authentik provides identity management and SSO-style flows.

## API Endpoints (Detailed)

Below is a practical, human‑readable map of the API routes in `api/app.py` and what each one does.

### Auth and Session Tokens

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `GET` | `/auth/register` | Redirects the user to the Authentik enrollment flow. | Requires `AUTH_ENROLLMENT_FLOW_SLUG`. |
| `GET` | `/auth/login` | Starts OIDC login for web or mobile. | Web flow uses cookies for PKCE; mobile must pass `state` + `code_challenge`. |
| `POST` | `/auth/mobile/exchange` | Exchanges an auth code + PKCE verifier for TrackSanté access/refresh tokens. | Mobile‑only bootstrap flow; returns app auth state including `role`, `portal`, the default redirect target, and a TrackSanté refresh token for secure device storage. |
| `GET` | `/auth/oidc/callback` | Completes OIDC login, creates/links user, and sets refresh cookie. | Redirects to the app/site after login, using the doctor portal redirect when the resolved role is `doctor` and no explicit `next` is set. |
| `POST` | `/auth/logout` | Clears the refresh cookie. | Stateless; returns `{ok: true}`. |
| `POST` | `/auth/refresh` | Issues a new access token from a TrackSanté refresh token. | Web uses the httpOnly refresh cookie; native mobile can instead send `{refresh_token}` in JSON. Returns `{access_token, user_id, role, portal, is_doctor, default_redirect_url, default_redirect_path}`. |
| `GET` | `/auth/me` | Returns the current app auth context for the bearer token. | Includes `user_id`, `role`, `portal`, `is_doctor`, `default_redirect_url`, and `default_redirect_path`. |

Doctor-role resolution is driven by Authentik claims returned from OIDC userinfo. The backend looks at `AUTH_ROLE_CLAIM_KEYS` (default: `role,roles,group,groups,ak_groups`) and treats any matching label from `AUTH_DOCTOR_GROUPS` (default: `doctor,doctors`) as a doctor login. The authorize request scopes are configurable via `AUTH_OIDC_SCOPES`, and the web callback uses `AUTH_SUCCESS_REDIRECT_DOCTOR` or `AUTH_SUCCESS_REDIRECT_PATIENT` when no explicit `next` parameter is present.

### Health and Debug

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `GET` | `/api/health` | Returns `{ok: true, time: ...}` for basic health checks. | Useful for uptime checks. |
| `GET` | `/callback` | Echoes query params. | Useful for debugging auth redirects. |

### Users and Profiles

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `POST` | `/api/users` | Creates a new user record. | Returns `{user_id}`. |
| `GET` | `/api/profile` | Returns the authenticated user's profile. | Requires access token. |
| `PATCH` | `/api/profile` | Updates the authenticated user's profile fields. | Merges into JSON profile; requires access token. |

### Devices, Sessions, and Sensors

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `POST` | `/api/devices/register` | Registers or updates a device and its capabilities. | Upserts on `device_external_id`. |
| `POST` | `/api/sessions/start` | Starts a new user session for a device. | Accepts optional `encounter_id`; returns `{session_id, device_id, encounter_id}`. |
| `POST` | `/api/sessions/end` | Ends a session (idempotent). | Validates `ended_at` vs `started_at`. |
| `POST` | `/api/sensors/events` | Ingests one sensor event or a batched list of timestamped readings for one sensor kind. | Optional `encounter_id` keeps chat, session, and sensor history joined; `measurement_run_id` groups user-triggered measurement flows; de‑dupes on `(device_id, kind, seq)` when `seq` is present. |
| `GET` | `/api/sensors/last` | Gets last `n` sensor events by device and kind. | Query params: `device_external_id`, `kind`, `n`, optional `session_id`, optional `measurement_run_id`. |
| `GET` | `/api/measurements/history` | Returns one explicit history row per measurement capture (`measurement_run_id` + canonical `measurement_id`). | Requires access token; supports `user_id` and `limit` query params. Background telemetry is returned separately with `source=background_telemetry` when present. Rows include latest run-level inference when available (`analysis`, plus compatibility mirrors `analysis_result` and `sensor_analysis`). |
| `GET` | `/api/measurements/trends` | Returns chart-ready grouped vital series. | Requires access token; supports optional `user_id` query param. |
| `GET` | `/api/measurements/analysis` | Returns persisted hybrid sensor analysis results for prior measurement runs. | Requires access token; supports optional `measurement_run_id`, optional `user_id`, and `limit`. |

Example batched sensor payload:

```json
{
  "device_external_id": "watch-123",
  "session_id": "2b4d8ff5-2b0b-4e7e-8f8f-8f52f8d51fd1",
  "measurement_run_id": "4f6f5ff0-d01f-4536-a8b3-a59e9e8de9d3",
  "kind": "hr",
  "readings": [
    {
      "ts": "2026-03-13T15:00:00Z",
      "seq": 101,
      "data": { "value": 78 }
    },
    {
      "ts": "2026-03-13T15:00:03Z",
      "seq": 102,
      "data": { "value": 80 }
    }
  ]
}
```

Send heart rate and SpO2 as separate requests by `kind` such as `hr` and `spo2`. Reuse the same `measurement_run_id` across all user-triggered uploads in one measurement flow, and omit `measurement_run_id` for background ambient uploads.

`POST /api/sensors/events` now also returns a best-effort `analysis` object. The API combines rules with an adaptive baseline model once prior measurement runs exist. When `measurement_run_id` is present, it persists the latest run-level result and exposes it again via `/api/measurements/analysis`.

The sensor ingest path also normalizes mixed hardware payloads into standard analysis fields. A single device packet can include fields like `sht_Tc_x100`, `sht_RH_x100`, `ens_iaq`, `ens_eCO2_ppm`, `mlx_Ta_mC`, `mlx_To_mC`, `hr_bpm`, and `spo2_x10`; the API will derive normalized values such as `ambient_temperature_c`, `humidity_percent`, `air_quality_index`, `co2_ppm`, `skin_temperature_c`, `heart_rate_bpm`, and `spo2_percent` before storage and inference. This lets the current model score `heart_rate` and `spo2` immediately, while keeping `eCO2`, humidity, and ambient temperature as context signals.

The API now auto-loads every `.pt` artifact in `api/models/` by default, so multiple public models can contribute to the same sensor analysis. The current public-model stack is:

- `BIDMC` for `heart_rate`, `spo2`, and optional `respiratory_rate`
- `PPG-DaLiA` for `heart_rate` and wrist `temperature`
- `Occupancy Detection` as an environment-context model for ambient `temperature`, `humidity`, and `co2`

To train the BIDMC physiology model on its own, run:

```bash
python3 scripts/ml/train_bidmc_public_model.py --download-dir /tmp/bidmc
```

To train the PPG-DaLiA physiology model on its own, run:

```bash
python3 scripts/ml/train_ppg_dalia_public_model.py --download-dir /tmp/ppg_dalia
```

To train the ambient environment model on its own, run:

```bash
python3 scripts/ml/train_occupancy_public_model.py --download-dir /tmp/occupancy
```

Each trainer writes a `.pt` artifact plus training reports next to it by default, including `training.log`, `training_summary.json`, `subset_metrics.csv`, `loss_history.csv`, `reconstruction_errors.csv`, `loss_curves.png`, and `reconstruction_error_histograms.png`. The API loads all `.pt` artifacts automatically from `SENSOR_PUBLIC_MODEL_DIR` (default: `/app/models`) and adds both a best-match `model.public` result and a `model.public_models` list to sensor analyses when matching metrics are present.

To launch a detached GPU-backed BIDMC-only training job that survives SSH disconnects, run:

```bash
scripts/ml/launch_bidmc_training.sh --follow
```

That creates a timestamped run under `ml_runs/`, stores the persistent log at `ml_runs/<run_id>/reports/training.log`, and runs the trainer in a separate Docker container using the `sensor-trainer` service. The current BIDMC trainer covers `heart_rate`, `respiratory_rate`, and `spo2`, and the runtime automatically falls back to the best matching subset model such as `hr_spo2` when your device does not provide respiratory rate. It does not train on temperature, humidity, ambient eCO2, or air-quality context because those channels are not present in BIDMC.

To run the full overnight GPU suite sequentially, use:

```bash
scripts/ml/launch_public_training_suite.sh -- --quality-profile max --device cuda
```

That suite trains `BIDMC`, then `PPG-DaLiA`, then the `Occupancy Detection` environment-context model in one GPU container. Each model starts only after the previous one finishes, and the final `.pt` files are copied into `api/models/` so the API can pick them up automatically.

To guarantee one completed run for each dataset first and then do parameter sweeps, use:

```bash
scripts/ml/launch_public_training_suite.sh -- \
  --quality-profile balanced \
  --device cuda \
  --enable-sweeps \
  --sweep-profiles balanced,max \
  --sweep-extra-seeds 1337
```

That ordering is guaranteed:

- base `BIDMC`
- base `PPG-DaLiA`
- base `Occupancy Detection` environment-context model
- `BIDMC` sweeps
- `PPG-DaLiA` sweeps
- `Occupancy Detection` environment-context sweeps

Recommended epoch caps for an overnight run on one GPU:

- base pass: `balanced` profile, which is `160` epochs
- sweeps: `balanced` and `max`, which are `160` and `320` epochs respectively

Early stopping is enabled, so many sweep runs will finish before those caps, especially for the smaller environment-context model.

During training, you can:

- watch persisted logs with `tail -f ml_runs/<run_id>/reports/training.log`
- follow container stdout with `docker logs -f capstone-sensor-trainer-<run_id>`
- inspect graphs and metrics in `ml_runs/<run_id>/reports/`

For the suite runner, use:

- `tail -f ml_runs/<run_id>/suite.log`
- `docker logs -f capstone-sensor-trainer-suite-<run_id>`
- inspect per-model reports under `ml_runs/<run_id>/bidmc/reports/`, `ml_runs/<run_id>/ppg_dalia/reports/`, and `ml_runs/<run_id>/occupancy/reports/`
- inspect per-dataset sweep comparisons in `ml_runs/<run_id>/<dataset>/selection_summary.json`

Each run stores:

- model artifact: `ml_runs/<run_id>/bidmc_public_model.pt`
- persistent log: `ml_runs/<run_id>/reports/training.log`
- summary JSON: `ml_runs/<run_id>/reports/training_summary.json`
- CSV metrics: `subset_metrics.csv`, `loss_history.csv`, `reconstruction_errors.csv`
- PNG graphs: `loss_curves.png`, `reconstruction_error_histograms.png`

The current public datasets do not cover `air_quality_index` directly, so `AQI` remains a rule-based context feature for now.

### Files, Images, and Storage

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `POST` | `/api/images/upload` | Uploads an image for a device/session, stores in MinIO, and records metadata. | Multipart upload; requires `device_external_id` unless an access token is provided. |
| `POST` | `/api/images/analyze` | Runs non-diagnostic visual anomaly screening, stores overlay metadata/history, and returns labels + boxes. | Accepts either `image_id` or multipart `file`; `overlay_preview_base64` is transient response-only and object-storage keys are kept internal. |
| `GET` | `/api/images/history` | Returns persisted visual-screening history for the authenticated user. | Requires access token; supports `limit` and optional `user_id` (must match token subject). |
| `GET` | `/api/images/analysis` | Returns a persisted visual-screening result for a given `image_id`. | Requires access token; query params: `image_id`, optional `user_id` (must match token subject). |
| `DELETE` | `/api/images/{image_id}` | Deletes the authenticated user's own visual analysis and its linked source/overlay image records. | Requires access token; use the history item's `source_image_id` as the delete target. Passing the overlay image id also resolves to the same analysis group. Returns `{ok, analysis_id, source_image_id, overlay_image_id}`. |
| `GET` | `/api/images/list` | Lists images by device or session. | Query params: `device_external_id`, `session_id`, `limit`. |
| `POST` | `/api/files/upload` | Uploads a PDF medical history file for a user. | Requires access token; PDF only; size limited by `PROFILE_FILE_MAX_MB`. |

### Chat / RAG

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `POST` | `/api/chat` | Runs a chat turn using RAG, stores user/assistant messages, and updates session signals. | Creates or reuses a chat session; accepts optional `encounter_id` and returns both `session_id` and `encounter_id`. |

## Quick Start

1. Copy the env template:
   `cp .env.example .env`
2. Start the stack:
   `docker compose up -d`
3. Open the API docs:
   `https://api.tracksante.com/docs`

## Ports

| Service | Port |
| --- | --- |
| API | `8080` |
| Adminer | `8081` |
| Grafana (local bind / tunnel origin) | `3000` |
| Postgres | `5432` |
| MinIO S3 API | `9000` |
| MinIO Console | `9001` |
| Authentik UI | `9002` |
| Prometheus | `9090` |
| Authentik HTTPS (optional) | `9443` |
| TrackSanté Site | `8088` |
| TrackSanté App | `8089` |

## Observability

- API metrics are exposed at `/metrics` and include HTTP traffic, dependency health, sensor ingestion, and chat/RAG counters.
- Grafana is deployed at `https://grafana.tracksante.com` and served locally from port `3000`.
- Grafana is provisioned automatically with:
  - `TrackSante Daily Health Overview`
  - `TrackSante Infrastructure Overview`
  - `TrackSante Product Telemetry`
  - `TrackSante RAG and LLM Observability`
  - `TrackSante Verification Overview`
- Prometheus is available at `http://localhost:9090`.
- See `docs/observability/stack.md` for setup details and dashboard intent.

## Data That Persists Locally

These folders on your machine are mounted into containers so data survives restarts:

| Path | Purpose |
| --- | --- |
| `pgdata/` | Postgres data |
| `minio-data/` | MinIO object storage |
| `authentik-pgdata/` | Authentik Postgres data |
| `authentik-media/` | Authentik media uploads |
| `rag_state/` | RAG session state (mounted for API runtime) |
| `vectordb/` | Vector DB persistence (mounted for API runtime) |

## Environment Variables (Highlights)

The single source of truth is `.env`. The template is `.env.example`.

Common values you may care about:

| Variable | What it does |
| --- | --- |
| `DATABASE_URL` | API connection string to Postgres |
| `S3_ENDPOINT` | MinIO base URL |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | MinIO credentials |
| `S3_BUCKET_IMAGES` | Bucket name for images |
| `OLLAMA_BASE_URL` | Where the API reaches Ollama |
| `OLLAMA_CHAT_MODEL` / `OLLAMA_EMBED_MODEL` | Model names to pull and keep warm |

## Notes

- The API container is a plain Python image that installs requirements at startup.
- The Ollama service expects GPU access (NVIDIA). If you don't have a GPU, you can remove the GPU reservation block or point `OLLAMA_BASE_URL` to a remote host.
- `cappie_run` is mounted for imports only; it is documented in a separate repo.
