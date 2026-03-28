CREATE TABLE IF NOT EXISTS sensor_analysis (
  id BIGSERIAL PRIMARY KEY,
  measurement_run_id UUID NOT NULL,
  session_id UUID REFERENCES user_sessions(id) ON DELETE CASCADE,
  encounter_id UUID,
  device_id UUID REFERENCES devices(id) ON DELETE SET NULL,
  analyzer TEXT NOT NULL,
  scope TEXT NOT NULL,
  status TEXT NOT NULL,
  score DOUBLE PRECISION,
  confidence DOUBLE PRECISION,
  summary TEXT,
  event_count INT NOT NULL DEFAULT 0,
  findings JSONB NOT NULL DEFAULT '[]'::jsonb,
  latest_values JSONB NOT NULL DEFAULT '{}'::jsonb,
  features JSONB NOT NULL DEFAULT '{}'::jsonb,
  analyzed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (measurement_run_id, analyzer)
);

CREATE INDEX IF NOT EXISTS idx_sensor_analysis_session_time
  ON sensor_analysis(session_id, analyzed_at DESC)
  WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sensor_analysis_encounter_time
  ON sensor_analysis(encounter_id, analyzed_at DESC)
  WHERE encounter_id IS NOT NULL;
