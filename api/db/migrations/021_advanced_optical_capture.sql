CREATE TABLE IF NOT EXISTS optical_capture_runs (
  measurement_run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  encounter_id UUID,
  session_id UUID REFERENCES user_sessions(id) ON DELETE SET NULL,
  device_external_id TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'advanced_optical',
  status TEXT NOT NULL DEFAULT 'active',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  stopped_at TIMESTAMPTZ,
  sample_rate_hz DOUBLE PRECISION,
  requested_measurements JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_waveform_available BOOLEAN NOT NULL DEFAULT FALSE,
  total_chunks INTEGER NOT NULL DEFAULT 0,
  total_packets BIGINT NOT NULL DEFAULT 0,
  total_samples BIGINT NOT NULL DEFAULT 0,
  ts1_frame_count BIGINT NOT NULL DEFAULT 0,
  packet_loss_count BIGINT NOT NULL DEFAULT 0,
  checksum_failure_count BIGINT NOT NULL DEFAULT 0,
  malformed_packet_count BIGINT NOT NULL DEFAULT 0,
  latest_chunk_at TIMESTAMPTZ,
  CONSTRAINT optical_capture_runs_mode_check CHECK (mode IN ('advanced_optical')),
  CONSTRAINT optical_capture_runs_status_check CHECK (status IN ('active', 'stopped', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_optical_capture_runs_patient_started
  ON optical_capture_runs(patient_user_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_optical_capture_runs_encounter_started
  ON optical_capture_runs(encounter_id, started_at DESC)
  WHERE encounter_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_optical_capture_runs_session_started
  ON optical_capture_runs(session_id, started_at DESC)
  WHERE session_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS optical_capture_chunks (
  id BIGSERIAL PRIMARY KEY,
  measurement_run_id UUID NOT NULL REFERENCES optical_capture_runs(measurement_run_id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  chunk_started_at TIMESTAMPTZ,
  chunk_ended_at TIMESTAMPTZ,
  packet_count INTEGER NOT NULL DEFAULT 0,
  sample_count BIGINT NOT NULL DEFAULT 0,
  ts1_frame_count INTEGER NOT NULL DEFAULT 0,
  first_packet_id TEXT,
  last_packet_id TEXT,
  packet_loss_count_delta INTEGER NOT NULL DEFAULT 0,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT optical_capture_chunks_measurement_run_chunk_unique UNIQUE (measurement_run_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_optical_capture_chunks_run_started
  ON optical_capture_chunks(measurement_run_id, chunk_started_at ASC, chunk_index ASC);

CREATE INDEX IF NOT EXISTS idx_optical_capture_chunks_run_ended
  ON optical_capture_chunks(measurement_run_id, chunk_ended_at ASC, chunk_index ASC);

CREATE TABLE IF NOT EXISTS optical_capture_ts1_frames (
  id BIGSERIAL PRIMARY KEY,
  measurement_run_id UUID NOT NULL,
  chunk_index INTEGER NOT NULL,
  frame_index INTEGER NOT NULL,
  received_at TIMESTAMPTZ,
  timestamp_ms BIGINT,
  raw_line TEXT,
  checksum_ok BOOLEAN,
  heart_rate_bpm DOUBLE PRECISION,
  spo2_percent DOUBLE PRECISION,
  red_dc DOUBLE PRECISION,
  ir_dc DOUBLE PRECISION,
  red_ac DOUBLE PRECISION,
  ir_ac DOUBLE PRECISION,
  quality DOUBLE PRECISION,
  parsed_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT optical_capture_ts1_frames_chunk_fk
    FOREIGN KEY (measurement_run_id, chunk_index)
    REFERENCES optical_capture_chunks(measurement_run_id, chunk_index)
    ON DELETE CASCADE,
  CONSTRAINT optical_capture_ts1_frames_run_frame_unique
    UNIQUE (measurement_run_id, chunk_index, frame_index)
);

CREATE INDEX IF NOT EXISTS idx_optical_capture_ts1_frames_run_received
  ON optical_capture_ts1_frames(measurement_run_id, received_at ASC, chunk_index ASC, frame_index ASC);

CREATE INDEX IF NOT EXISTS idx_optical_capture_ts1_frames_run_timestamp_ms
  ON optical_capture_ts1_frames(measurement_run_id, timestamp_ms ASC, chunk_index ASC, frame_index ASC)
  WHERE timestamp_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_optical_capture_ts1_frames_run_checksum
  ON optical_capture_ts1_frames(measurement_run_id, checksum_ok, received_at DESC);
