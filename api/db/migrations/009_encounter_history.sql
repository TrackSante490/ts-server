ALTER TABLE user_sessions
  ADD COLUMN IF NOT EXISTS encounter_id UUID;

UPDATE user_sessions
SET encounter_id = id
WHERE encounter_id IS NULL;

ALTER TABLE user_sessions
  ALTER COLUMN encounter_id SET DEFAULT gen_random_uuid();

CREATE INDEX IF NOT EXISTS idx_sessions_user_encounter_started
  ON user_sessions(user_id, encounter_id, started_at DESC);

ALTER TABLE sensor_events
  ADD COLUMN IF NOT EXISTS encounter_id UUID;

UPDATE sensor_events se
SET encounter_id = us.encounter_id
FROM user_sessions us
WHERE se.session_id = us.id
  AND se.encounter_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_sensor_events_encounter_ts
  ON sensor_events(encounter_id, ts DESC)
  WHERE encounter_id IS NOT NULL;
