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
| `POST` | `/auth/mobile/exchange` | Exchanges an auth code + PKCE verifier for TrackSanté access/refresh tokens. | Mobile‑only flow; returns JWTs in JSON. |
| `GET` | `/auth/oidc/callback` | Completes OIDC login, creates/links user, and sets refresh cookie. | Redirects to the app/site after login. |
| `POST` | `/auth/logout` | Clears the refresh cookie. | Stateless; returns `{ok: true}`. |
| `POST` | `/auth/refresh` | Issues a new access token from a refresh cookie. | Returns `{access_token, user_id}`. |
| `GET` | `/auth/me` | Returns the current user ID from the access token. | Requires `Authorization: Bearer <access_token>`. |

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
| `POST` | `/api/sessions/start` | Starts a new user session for a device. | Returns `{session_id, device_id}`. |
| `POST` | `/api/sessions/end` | Ends a session (idempotent). | Validates `ended_at` vs `started_at`. |
| `POST` | `/api/sensors/events` | Ingests a sensor event payload. | De‑dupes on `(device_id, kind, seq)`. |
| `GET` | `/api/sensors/last` | Gets last `n` sensor events by device and kind. | Query params: `device_external_id`, `kind`, `n`. |

### Files, Images, and Storage

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `POST` | `/api/images/upload` | Uploads an image for a device/session, stores in MinIO, and records metadata. | Multipart upload; requires `device_external_id`. |
| `GET` | `/api/images/list` | Lists images by device or session. | Query params: `device_external_id`, `session_id`, `limit`. |
| `POST` | `/api/files/upload` | Uploads a PDF medical history file for a user. | Requires access token; PDF only; size limited by `PROFILE_FILE_MAX_MB`. |

### Chat / RAG

| Method | Path | What it does | Notes |
| --- | --- | --- | --- |
| `POST` | `/api/chat` | Runs a chat turn using RAG, stores user/assistant messages, and updates session signals. | Creates a session if missing; uses `rag_conv.rag_agent.chat_once`. |

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
| Postgres | `5432` |
| MinIO S3 API | `9000` |
| MinIO Console | `9001` |
| Authentik UI | `9002` |
| Authentik HTTPS (optional) | `9443` |
| TrackSanté Site | `8088` |
| TrackSanté App | `8089` |

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
