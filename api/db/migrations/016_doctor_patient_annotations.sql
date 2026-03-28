CREATE TABLE IF NOT EXISTS doctor_patient_notes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  encounter_id UUID,
  note_text TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT doctor_patient_notes_distinct_users CHECK (doctor_user_id <> patient_user_id)
);

CREATE INDEX IF NOT EXISTS idx_doctor_patient_notes_lookup
  ON doctor_patient_notes(doctor_user_id, patient_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_doctor_patient_notes_encounter
  ON doctor_patient_notes(patient_user_id, encounter_id, created_at DESC)
  WHERE encounter_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS doctor_encounter_tags (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  doctor_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  patient_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  encounter_id UUID NOT NULL,
  tag TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT doctor_encounter_tags_distinct_users CHECK (doctor_user_id <> patient_user_id),
  CONSTRAINT doctor_encounter_tags_tag_nonempty CHECK (btrim(tag) <> ''),
  CONSTRAINT doctor_encounter_tags_unique UNIQUE (doctor_user_id, patient_user_id, encounter_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_doctor_encounter_tags_lookup
  ON doctor_encounter_tags(doctor_user_id, patient_user_id, encounter_id, created_at DESC);
