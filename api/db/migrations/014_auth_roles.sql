ALTER TABLE users
  ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'patient';

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS auth_groups JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;

UPDATE users
SET role = 'patient'
WHERE role IS NULL
   OR btrim(role) = ''
   OR role NOT IN ('patient', 'doctor');

UPDATE users
SET auth_groups = '[]'::jsonb
WHERE auth_groups IS NULL;

CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'users_role_check'
      AND conrelid = 'users'::regclass
  ) THEN
    ALTER TABLE users
      ADD CONSTRAINT users_role_check
      CHECK (role IN ('patient', 'doctor'));
  END IF;
END $$;
