CREATE TABLE IF NOT EXISTS telemetry (
  id BIGSERIAL PRIMARY KEY,
  device_id TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  seq BIGINT,
  metrics_json JSONB NOT NULL,
  UNIQUE (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_device_ts ON telemetry(device_id, ts_utc);
