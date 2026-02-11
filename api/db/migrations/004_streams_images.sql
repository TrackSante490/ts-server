-- Raw/high-frequency streams -> stored in MinIO, DB stores pointer
CREATE TABLE IF NOT EXISTS sensor_streams (
  id BIGSERIAL PRIMARY KEY,
  session_id UUID REFERENCES user_sessions(id) ON DELETE CASCADE,
  device_id UUID REFERENCES devices(id) ON DELETE SET NULL,

  kind TEXT NOT NULL,                      -- 'ppg_raw', 'accel_raw'
  sample_rate_hz DOUBLE PRECISION,

  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,

  bucket TEXT NOT NULL,
  key TEXT NOT NULL,

  meta JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- prevents duplicate pointers for the same bucket/key
  UNIQUE (bucket, key),

  -- sanity: if ended_at exists it must be >= started_at
  CONSTRAINT chk_stream_time CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- common query patterns
CREATE INDEX IF NOT EXISTS idx_streams_session_kind
  ON sensor_streams(session_id, kind);

CREATE INDEX IF NOT EXISTS idx_streams_session_started
  ON sensor_streams(session_id, started_at);

CREATE INDEX IF NOT EXISTS idx_streams_device_started
  ON sensor_streams(device_id, started_at);


-- Images -> stored in MinIO, DB stores pointer
CREATE TABLE IF NOT EXISTS images (
  id BIGSERIAL PRIMARY KEY,
  session_id UUID REFERENCES user_sessions(id) ON DELETE SET NULL,
  device_id UUID REFERENCES devices(id) ON DELETE SET NULL,

  ts TIMESTAMPTZ NOT NULL DEFAULT now(),

  bucket TEXT NOT NULL,
  key TEXT NOT NULL,

  sha256 TEXT NOT NULL,
  mime TEXT,
  width INT,
  height INT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- prevents duplicate pointers for the same bucket/key
  UNIQUE (bucket, key),

  -- basic sanity if dimensions exist
  CONSTRAINT chk_image_dims CHECK (
    (width IS NULL OR width > 0) AND (height IS NULL OR height > 0)
  )
);

CREATE INDEX IF NOT EXISTS idx_images_session_ts
  ON images(session_id, ts);

CREATE INDEX IF NOT EXISTS idx_images_device_ts
  ON images(device_id, ts);