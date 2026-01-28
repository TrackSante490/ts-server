# TrackSanté Local Server Backend

Runs locally using Docker Compose:
- FastAPI (API)
- PostgreSQL (telemetry + metadata)
- MinIO (S3-compatible object storage)
- Adminer (DB UI)

## Quick Start
1. Copy env template:
   cp .env.example .env
2. Start:
   docker compose up -d
3. API:
   http://localhost:8080/docs

## Ports
- API: 8080
- Postgres: 5432
- MinIO S3: 9000
- MinIO Console: 9001
- Adminer: 8081
