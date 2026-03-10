# TrackSante Observability Stack

This repo now includes a self-hosted observability stack with Prometheus and Grafana.

## What Gets Collected

- API-level Prometheus metrics from `/metrics`
  - request rate and latency
  - dependency reachability for Postgres, MinIO, Authentik, and Ollama
  - sensor event ingest outcomes
  - chat request volume and latency
  - RAG chat turn volume and error counts
- Product telemetry straight from Postgres
  - `user_sessions`
  - `sensor_events`
  - `session_messages`

## Services

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`

## Provisioned Dashboards

- `TrackSante Infrastructure Overview`
  - dependency availability and probe latency
  - API request rate and p95 latency
  - sensor ingestion rate
  - chat request rate
- `TrackSante Product Telemetry`
  - sessions started
  - average session duration
  - sensor events by kind
  - chat messages by role
  - recommended sensor trends
  - session outcome trends
- `TrackSante RAG and LLM Observability`
  - RAG turn rate
  - RAG p95 duration
  - RAG error rate
  - Ollama availability and probe latency
  - chat session growth by recommended sensor
- `TrackSante Verification Overview`
  - latest logged uptime test result
  - latest response-time p50 and p95 from scripted runs
  - latest ingest success and data-integrity results
  - latest sustainable concurrency result
  - recent verification artifact paths

## Startup

1. Ensure your `.env` contains Grafana settings if you want to override the defaults in `.env.example`.
2. Run `docker compose up -d`.
3. Open Grafana and log in with `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`.

## Notes

- The Postgres Grafana datasource uses the credentials provided through the Grafana-related env vars.
- Ollama GPU metrics are not exported directly by this stack. The implemented dashboard currently tracks Ollama availability and probe latency from the API.
- Formal verification scripts live in `scripts/verification/` and write raw artifacts to `test-results/`.
- See `docs/observability/verification.md` for commands and expected outputs.
