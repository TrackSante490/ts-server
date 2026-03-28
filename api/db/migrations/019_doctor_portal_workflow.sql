ALTER TABLE consult_cases
  ADD COLUMN IF NOT EXISTS consult_status_reason TEXT,
  ADD COLUMN IF NOT EXISTS closed_reason TEXT,
  ADD COLUMN IF NOT EXISTS handoff_requested BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS handoff_target_clinician_id UUID REFERENCES users(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS reopened_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_clinician_action_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_clinician_action_type TEXT;

ALTER TABLE encounter_clinical_summaries
  ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 1;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'consult_cases'::regclass
      AND conname = 'consult_cases_escalation_check'
      AND pg_get_constraintdef(oid) <> 'CHECK ((escalation_status = ANY (ARRAY[''none''::text, ''watch''::text, ''escalated''::text, ''transferred''::text])))'
  ) THEN
    ALTER TABLE consult_cases DROP CONSTRAINT consult_cases_escalation_check;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'consult_cases'::regclass
      AND conname = 'consult_cases_escalation_check'
  ) THEN
    ALTER TABLE consult_cases
      ADD CONSTRAINT consult_cases_escalation_check
      CHECK (escalation_status IN ('none', 'watch', 'escalated', 'transferred'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_consult_cases_handoff_target
  ON consult_cases(handoff_target_clinician_id, updated_at DESC)
  WHERE handoff_target_clinician_id IS NOT NULL;
