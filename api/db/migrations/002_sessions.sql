-- Device capabilities snapshot (overwrites each login)
CREATE TABLE IF NOT EXISTS device_capabilities (
  device_id UUID PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
  capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Sessions
CREATE TABLE IF NOT EXISTS user_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  device_id UUID REFERENCES devices(id) ON DELETE SET NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_started ON user_sessions(user_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_device_started ON user_sessions(device_id, started_at DESC);