CREATE TABLE IF NOT EXISTS reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  model TEXT NOT NULL,
  payload_hash TEXT,
  status TEXT NOT NULL DEFAULT 'generated',
  report_md TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reports_user_created_at ON reports(user_id, created_at DESC);
