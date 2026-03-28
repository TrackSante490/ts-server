CREATE TABLE IF NOT EXISTS consult_cases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  consult_status TEXT NOT NULL DEFAULT 'new',
  priority TEXT NOT NULL DEFAULT 'routine',
  escalation_status TEXT NOT NULL DEFAULT 'none',
  opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_at TIMESTAMPTZ,
  next_action_due_at TIMESTAMPTZ,
  next_follow_up_due_at TIMESTAMPTZ,
  last_action_summary TEXT,
  attention_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  attention_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_patient_activity_at TIMESTAMPTZ,
  last_clinician_activity_at TIMESTAMPTZ,
  last_critical_event_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revision BIGINT NOT NULL DEFAULT 1,
  CONSTRAINT consult_cases_distinct_users CHECK (doctor_user_id <> patient_user_id),
  CONSTRAINT consult_cases_status_check CHECK (consult_status IN ('new', 'in_review', 'awaiting_patient', 'follow_up_due', 'closed')),
  CONSTRAINT consult_cases_priority_check CHECK (priority IN ('routine', 'urgent', 'critical')),
  CONSTRAINT consult_cases_escalation_check CHECK (escalation_status IN ('none', 'watch', 'escalated'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_consult_cases_one_open_per_pair
  ON consult_cases(doctor_user_id, patient_user_id)
  WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_consult_cases_doctor_updated
  ON consult_cases(doctor_user_id, updated_at DESC, opened_at DESC);

CREATE INDEX IF NOT EXISTS idx_consult_cases_patient_updated
  ON consult_cases(patient_user_id, updated_at DESC, opened_at DESC);

CREATE TABLE IF NOT EXISTS consult_case_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  consult_case_id UUID NOT NULL REFERENCES consult_cases(id) ON DELETE CASCADE,
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  summary TEXT NOT NULL,
  encounter_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_consult_case_events_case_created
  ON consult_case_events(consult_case_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_consult_case_events_patient_created
  ON consult_case_events(patient_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS consult_case_messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  consult_case_id UUID NOT NULL REFERENCES consult_cases(id) ON DELETE CASCADE,
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  encounter_id UUID,
  sender_type TEXT NOT NULL,
  sender_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
  message_type TEXT NOT NULL DEFAULT 'message',
  body TEXT NOT NULL,
  requires_acknowledgement BOOLEAN NOT NULL DEFAULT FALSE,
  read_at TIMESTAMPTZ,
  attachments JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT consult_case_messages_sender_type_check CHECK (sender_type IN ('patient', 'doctor', 'system')),
  CONSTRAINT consult_case_messages_type_check CHECK (message_type IN ('message', 'instruction', 'system')),
  CONSTRAINT consult_case_messages_body_nonempty CHECK (btrim(body) <> '')
);

CREATE INDEX IF NOT EXISTS idx_consult_case_messages_case_created
  ON consult_case_messages(consult_case_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_consult_case_messages_encounter_created
  ON consult_case_messages(patient_user_id, encounter_id, created_at DESC)
  WHERE encounter_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_consult_case_messages_patient_unread
  ON consult_case_messages(patient_user_id, read_at, created_at DESC);

CREATE TABLE IF NOT EXISTS encounter_clinical_summaries (
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  encounter_id UUID NOT NULL,
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  reason_for_consult TEXT,
  patient_reported_symptoms JSONB NOT NULL DEFAULT '[]'::jsonb,
  clinician_assessment TEXT,
  disposition TEXT,
  follow_up_plan TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (patient_user_id, encounter_id)
);

CREATE INDEX IF NOT EXISTS idx_encounter_clinical_summaries_doctor
  ON encounter_clinical_summaries(doctor_user_id, updated_at DESC);

ALTER TABLE reports
  ADD COLUMN IF NOT EXISTS encounter_id UUID;

ALTER TABLE reports
  ADD COLUMN IF NOT EXISTS consult_case_id UUID REFERENCES consult_cases(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_reports_encounter_created
  ON reports(encounter_id, created_at DESC)
  WHERE encounter_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_reports_consult_case_created
  ON reports(consult_case_id, created_at DESC)
  WHERE consult_case_id IS NOT NULL;
