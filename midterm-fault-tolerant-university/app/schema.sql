CREATE TABLE IF NOT EXISTS students (
  id    TEXT PRIMARY KEY,
  name  TEXT NOT NULL,
  paid  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS enrollments (
  id          BIGSERIAL PRIMARY KEY,
  student_id  TEXT NOT NULL REFERENCES students(id),
  course      TEXT NOT NULL,
  client_ref  TEXT,                -- always stored, used only to audit duplicates
  request_id  TEXT,                -- stored only in ft mode, unique
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS enrollments_request_id ON enrollments(request_id) WHERE request_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS payments (
  id          BIGSERIAL PRIMARY KEY,
  student_id  TEXT NOT NULL REFERENCES students(id),
  amount      INTEGER NOT NULL,
  client_ref  TEXT,
  idem_key    TEXT,
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS payments_idem_key ON payments(idem_key) WHERE idem_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS transcripts (
  id          BIGSERIAL PRIMARY KEY,
  batch_id    TEXT NOT NULL,
  student_id  TEXT NOT NULL,
  gpa         NUMERIC(3,2),
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS batch_checkpoints (
  batch_id    TEXT PRIMARY KEY,
  size        INTEGER NOT NULL,
  last_index  INTEGER NOT NULL DEFAULT 0,
  status      TEXT NOT NULL DEFAULT 'running',
  updated_at  TIMESTAMPTZ DEFAULT now()
);
INSERT INTO students (id, name)
SELECT 'S' || lpad(g::text, 4, '0'), 'Student ' || g FROM generate_series(1, 500) g
ON CONFLICT DO NOTHING;
