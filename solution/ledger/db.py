"""SQLite schema. See SUBMISSION.md for the reasoning behind each table."""
from __future__ import annotations

import sqlite3

from .pii import Crypto

DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- One row per file ever ingested. UNIQUE(kind, workflow_id, sha256) is what makes re-ingest a
-- no-op; the row is written in the same transaction as the file's data, so a file is either
-- fully ingested or not at all.
CREATE TABLE IF NOT EXISTS ingested_files (
  id          INTEGER PRIMARY KEY,
  kind        TEXT NOT NULL CHECK (kind IN ('leads', 'events')),
  workflow_id TEXT NOT NULL DEFAULT '',          -- '' for event files
  drop_name   TEXT NOT NULL,
  file_name   TEXT NOT NULL,
  batch_id    TEXT,                               -- file stem, lead files only
  sha256      TEXT NOT NULL,
  ingested_at TEXT NOT NULL,
  stats       TEXT NOT NULL DEFAULT '{}',         -- JSON counters for data_quality.json
  UNIQUE (kind, workflow_id, sha256)
);

-- Current merged state of each lead. Identity = (workflow_id, phone_hash).
-- PII lives only in pii_enc (encrypted) and phone_hash (keyed blind index).
CREATE TABLE IF NOT EXISTS leads (
  id           INTEGER PRIMARY KEY,
  workflow_id  TEXT NOT NULL,
  phone_hash   BLOB NOT NULL,
  phone_last4  TEXT NOT NULL,                    -- only for the masked display in reports
  batch_id     TEXT NOT NULL,                    -- batch of the FIRST valid row, never changes
  external_ref TEXT NOT NULL,                    -- value of the schema's "reference" field
  attrs        TEXT NOT NULL,                    -- JSON of the normalised non-PII fields
  pii_enc      BLOB NOT NULL,                    -- Fernet token of the JSON PII fields
  file_id      INTEGER NOT NULL REFERENCES ingested_files(id),
  UNIQUE (workflow_id, phone_hash)
);
CREATE INDEX IF NOT EXISTS leads_batch ON leads (workflow_id, batch_id);

-- Rejected lead rows. Deliberately contains no row content (no PII).
CREATE TABLE IF NOT EXISTS rejections (
  id          INTEGER PRIMARY KEY,
  file_id     INTEGER NOT NULL REFERENCES ingested_files(id),
  workflow_id TEXT NOT NULL,
  batch_id    TEXT NOT NULL,
  row_number  INTEGER NOT NULL,
  reason      TEXT NOT NULL
);

-- The interaction ledger: append-only, one row per distinct event_id. Events are stored as
-- received (even orphans); linking to leads is derived at report time, so an event that
-- arrives before its lead needs no repair job.
CREATE TABLE IF NOT EXISTS events (
  seq              INTEGER PRIMARY KEY AUTOINCREMENT,   -- ingest order
  event_id         TEXT NOT NULL UNIQUE,
  event_type       TEXT NOT NULL,
  workflow_id      TEXT,
  call_id          TEXT,
  occurred_at      TEXT NOT NULL,                       -- normalised, UTC ISO-8601
  occurred_us      INTEGER NOT NULL,                    -- same instant, epoch microseconds
  phone_hash       BLOB,                                -- NULL when absent or invalid
  disposition_code TEXT,                                -- disposition.* events only
  payload          TEXT NOT NULL,                       -- JSON as received (no phone)
  file_id          INTEGER NOT NULL REFERENCES ingested_files(id)
);
CREATE INDEX IF NOT EXISTS events_call ON events (call_id);
CREATE INDEX IF NOT EXISTS events_link ON events (workflow_id, phone_hash);

CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'interaction ledger is append-only'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'interaction ledger is append-only'); END;
"""


def connect(path: str, crypto: Crypto) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None)  # explicit BEGIN/COMMIT below
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(DDL)
    check = crypto.key_check()
    row = conn.execute("SELECT value FROM meta WHERE key='key_check'").fetchone()
    if row is None:
        conn.execute("INSERT INTO meta VALUES ('key_check', ?)", (check,))
    elif row[0] != check:
        raise SystemExit("PII key does not match this database (wrong LEDGER_PII_KEY / .key file?)")
    return conn
