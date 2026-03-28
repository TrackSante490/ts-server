CREATE TABLE IF NOT EXISTS doctor_patient_assignments (
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (doctor_user_id, patient_user_id),
  CONSTRAINT doctor_patient_assignments_distinct_users CHECK (doctor_user_id <> patient_user_id)
);

CREATE INDEX IF NOT EXISTS idx_doctor_patient_assignments_patient
  ON doctor_patient_assignments(patient_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_doctor_patient_assignments_doctor
  ON doctor_patient_assignments(doctor_user_id, created_at DESC);
