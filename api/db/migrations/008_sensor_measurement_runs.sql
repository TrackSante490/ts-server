ALTER TABLE sensor_events
  ADD COLUMN IF NOT EXISTS measurement_run_id UUID;

CREATE INDEX IF NOT EXISTS idx_sensor_events_measurement_run_ts
  ON sensor_events(measurement_run_id, ts DESC)
  WHERE measurement_run_id IS NOT NULL;
