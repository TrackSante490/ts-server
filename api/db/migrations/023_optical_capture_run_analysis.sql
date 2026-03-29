CREATE TABLE IF NOT EXISTS optical_capture_run_analysis (
  measurement_run_id UUID PRIMARY KEY REFERENCES optical_capture_runs(measurement_run_id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'processing',
  analysis_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  analyzed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT optical_capture_run_analysis_status_check CHECK (status IN ('processing', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_optical_capture_run_analysis_status_time
  ON optical_capture_run_analysis(status, analyzed_at DESC);