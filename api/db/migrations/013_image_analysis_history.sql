ALTER TABLE image_analyses
  ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES users(id) ON DELETE SET NULL;

ALTER TABLE image_analyses
  ADD COLUMN IF NOT EXISTS findings_json JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE image_analyses
  ADD COLUMN IF NOT EXISTS retention_policy TEXT NOT NULL DEFAULT 'standard';

ALTER TABLE image_analyses
  ADD COLUMN IF NOT EXISTS retention_expires_at TIMESTAMPTZ;

ALTER TABLE image_analyses
  ADD COLUMN IF NOT EXISTS retention_meta JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE image_analyses
SET findings_json = findings
WHERE findings_json = '[]'::jsonb
  AND findings IS NOT NULL;

UPDATE image_analyses ia
SET user_id = resolved.user_id
FROM (
  SELECT ia2.id,
         COALESCE(us.user_id, d.user_id) AS user_id
  FROM image_analyses ia2
  JOIN images i ON i.id = ia2.source_image_id
  LEFT JOIN user_sessions us ON us.id = COALESCE(ia2.session_id, i.session_id)
  LEFT JOIN devices d ON d.id = COALESCE(ia2.device_id, i.device_id)
) AS resolved
WHERE ia.id = resolved.id
  AND ia.user_id IS NULL
  AND resolved.user_id IS NOT NULL;

UPDATE image_analyses
SET retention_expires_at = COALESCE(
      retention_expires_at,
      analyzed_at + INTERVAL '365 days',
      created_at + INTERVAL '365 days'
    ),
    retention_meta = CASE
      WHEN retention_meta IS NULL OR retention_meta = '{}'::jsonb
      THEN jsonb_build_object('policy', retention_policy, 'days', 365)
      ELSE retention_meta
    END
WHERE retention_expires_at IS NULL
   OR retention_meta IS NULL
   OR retention_meta = '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_image_analyses_user_time
  ON image_analyses(user_id, analyzed_at DESC)
  WHERE user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_image_analyses_retention_expiry
  ON image_analyses(retention_expires_at)
  WHERE retention_expires_at IS NOT NULL;
