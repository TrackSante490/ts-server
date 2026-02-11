-- Sensor events (your telemetry replacement)
CREATE TABLE IF NOT EXISTS sensor_events (
  id BIGSERIAL PRIMARY KEY,
  session_id UUID REFERENCES user_sessions(id) ON DELETE CASCADE,
  device_id UUID REFERENCES devices(id) ON DELETE SET NULL,
  ts TIMESTAMPTZ NOT NULL,
  kind TEXT NOT NULL,                    -- 'hr', 'spo2', 'temp', 'ppg_summary', etc.
  seq BIGINT,
  data JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS sensor_events_unique_seq ON sensor_events(device_id, kind, seq) WHERE seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sensor_events_session_ts ON sensor_events(session_id, ts);
CREATE INDEX IF NOT EXISTS idx_sensor_events_device_ts ON sensor_events(device_id, ts);
CREATE INDEX IF NOT EXISTS idx_sensor_events_kind_ts ON sensor_events(kind, ts);