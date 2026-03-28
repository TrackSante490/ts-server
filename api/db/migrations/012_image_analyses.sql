CREATE TABLE IF NOT EXISTS image_analyses (
  id BIGSERIAL PRIMARY KEY,
  source_image_id BIGINT NOT NULL REFERENCES images(id) ON DELETE CASCADE,
  overlay_image_id BIGINT REFERENCES images(id) ON DELETE SET NULL,
  session_id UUID REFERENCES user_sessions(id) ON DELETE SET NULL,
  encounter_id UUID,
  device_id UUID REFERENCES devices(id) ON DELETE SET NULL,
  measurement_run_id UUID,
  status TEXT NOT NULL,
  non_diagnostic BOOLEAN NOT NULL DEFAULT TRUE,
  summary TEXT NOT NULL DEFAULT '',
  findings JSONB NOT NULL DEFAULT '[]'::jsonb,
  analysis JSONB NOT NULL DEFAULT '{}'::jsonb,
  analyzed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_image_id)
);

CREATE INDEX IF NOT EXISTS idx_image_analyses_session_time
  ON image_analyses(session_id, analyzed_at DESC)
  WHERE session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_image_analyses_encounter_time
  ON image_analyses(encounter_id, analyzed_at DESC)
  WHERE encounter_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_image_analyses_measurement_run_time
  ON image_analyses(measurement_run_id, analyzed_at DESC)
  WHERE measurement_run_id IS NOT NULL;