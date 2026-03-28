ALTER TABLE patient_doctor_requests
  ADD COLUMN IF NOT EXISTS request_priority TEXT NOT NULL DEFAULT 'routine';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'patient_doctor_requests'::regclass
      AND conname = 'patient_doctor_requests_priority_check'
  ) THEN
    ALTER TABLE patient_doctor_requests DROP CONSTRAINT patient_doctor_requests_priority_check;
  END IF;
END $$;

ALTER TABLE patient_doctor_requests
  ADD CONSTRAINT patient_doctor_requests_priority_check
  CHECK (request_priority IN ('routine', 'urgent', 'critical'));

CREATE TABLE IF NOT EXISTS consult_attachments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  consult_case_id UUID NOT NULL REFERENCES consult_cases(id) ON DELETE CASCADE,
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  uploaded_by_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  uploaded_by_sender_type TEXT NOT NULL,
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  filename TEXT NOT NULL,
  content_type TEXT NOT NULL,
  size_bytes BIGINT NOT NULL,
  sha256 TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'file',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT consult_attachments_sender_type_check CHECK (uploaded_by_sender_type IN ('patient', 'doctor')),
  CONSTRAINT consult_attachments_kind_check CHECK (kind IN ('image', 'file'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_consult_attachments_bucket_key
  ON consult_attachments(bucket, object_key);

CREATE INDEX IF NOT EXISTS idx_consult_attachments_case_created
  ON consult_attachments(consult_case_id, created_at DESC);
