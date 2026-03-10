CREATE TABLE IF NOT EXISTS verification_runs (
  id BIGSERIAL PRIMARY KEY,
  test_key TEXT NOT NULL,
  scenario TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ok',
  target TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ NOT NULL,
  total_requests INTEGER,
  successful_requests INTEGER,
  failed_requests INTEGER,
  success_rate DOUBLE PRECISION,
  p50_ms DOUBLE PRECISION,
  p95_ms DOUBLE PRECISION,
  max_concurrency INTEGER,
  sustainable_concurrency INTEGER,
  records_expected INTEGER,
  records_verified INTEGER,
  exact_match_rate DOUBLE PRECISION,
  downtime_events INTEGER,
  artifacts_path TEXT,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT verification_runs_finished_after_started CHECK (finished_at >= started_at)
);

CREATE INDEX IF NOT EXISTS idx_verification_runs_test_finished
  ON verification_runs (test_key, finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_verification_runs_scenario_finished
  ON verification_runs (scenario, finished_at DESC);
