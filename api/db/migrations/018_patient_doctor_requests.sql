CREATE TABLE IF NOT EXISTS patient_doctor_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  request_reason TEXT,
  claimed_by_doctor_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
  claimed_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ,
  consult_case_id UUID REFERENCES consult_cases(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT patient_doctor_requests_status_check CHECK (status IN ('pending', 'claimed', 'cancelled'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_patient_doctor_requests_one_pending
  ON patient_doctor_requests(patient_user_id)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_patient_doctor_requests_status_created
  ON patient_doctor_requests(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_patient_doctor_requests_claimed_doctor
  ON patient_doctor_requests(claimed_by_doctor_user_id, claimed_at DESC)
  WHERE claimed_by_doctor_user_id IS NOT NULL;
