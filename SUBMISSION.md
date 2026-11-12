# Submission

## Candidate
- **Name:** Galib
- **Assignment chosen:** A (Data): Interaction Ledger & Reporting Pipeline
- **Time spent (honest estimate):** TODO: fill in your own number

## How to run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
cd solution
python -m ledger ingest ../data/drop_1
python -m ledger ingest ../data/drop_2
python -m ledger report --out ../reports/
pytest                                  # 97 tests
```
`ingest` also accepts a folder of drops (`ingest ../data` processes `drop_1`, `drop_2`, ... in natural order).
Options: `--db PATH` (default `ledger.db`), `--schemas DIR`. PII key: env `LEDGER_PII_KEY` or an auto-created `<db>.key`.

## Design

**Principle: store facts, derive everything else.** Ingest only validates and records. Linking an event
to a lead, orphan status, effective dispositions, buckets and all report numbers are computed from the
stored facts every time `report` runs. This is what makes the pipeline order-independent: a lead that
arrives in drop_2 automatically "adopts" the events that were orphans after drop_1, with no repair step
and no state to get out of sync.

**Tables** (SQLite, `ledger/db.py`)
- `ingested_files`: one row per file, `UNIQUE(kind, workflow_id, sha256)`. Inserted in the *same
  transaction* as the file's rows, so a file is either fully ingested or absent. This is the idempotency
  mechanism: re-ingesting a known file is skipped. Also holds the per-file counters used by `data_quality.json`.
- `leads`: current merged state, `UNIQUE(workflow_id, phone_hash)` (the lead identity). `batch_id` is
  that of the first valid row and never changes. Non-PII fields are JSON in `attrs`; PII is encrypted in `pii_enc`.
- `rejections`: `(workflow, batch, row_number, reason)`. No row content, so no PII.
- `events`: the **append-only ledger**, one row per distinct `event_id`, stored as received (orphans
  included). `BEFORE UPDATE` / `BEFORE DELETE` triggers abort, so even a buggy query cannot rewrite history.
  A disposition override is just another row that outranks the original at read time.

**PII at rest.** Fields marked `"pii": true` (customer name, phone) are encrypted with Fernet. To match
events to leads by phone without a plaintext phone, the DB stores a *blind index*: `HMAC-SHA256(key,
canonical_phone)`. It is keyed because an unkeyed hash of a 10-digit mobile number is trivially brute-forced
(~4 x 10^9 candidates). Events keep only that hash, never the number. The last 4 digits are stored
in clear solely for the `+91******1234` display the report contract requires. A test greps the raw DB
file (incl. WAL) for names and every phone format and finds nothing. The key lives outside the DB.

**Pipeline.** `ingest`: per file, one transaction: register file -> parse rows/lines -> write -> commit
(crash = rollback, test included). `report`: two streaming passes over `events`: (1) map each `call_id` to
its lead via the events that carry a phone, (2) classify every event (orphan / unknown disposition /
good) and group good ones per call; then derive per-call facts (connected, telephony_failed, effective
disposition), per-lead bucket, and aggregate.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Orphan and unknown-disposition counts are computed at report time on the final state | Spec: "orphan counts reflect the final state". Also makes results independent of ingest batching | Flag at ingest and reconcile later: more state, more ways to be wrong |
| Event priority is malformed -> duplicate -> orphan -> unknown, so an unlinkable event with a bad code counts as orphan only | Literal reading of the processing order | - |
| A line that is valid JSON with an `event_id` but an unparseable/missing `occurred_at` is counted **malformed** | The event cannot be placed in time, so no report can use it. Not in the generated data | Keep it with NULL time and exclude from derivations |
| `event_id` must be a non-empty string, else malformed | A missing/empty id cannot be deduped | `str()` the value |
| Whitespace-only event lines are skipped and not counted in `events_total` | "non-empty line" | Count them |
| Ties on `occurred_at` (overrides, sets, calls) are broken by `event_id` / `call_id` | Deterministic output independent of file order | Ingest order |
| Duplicate `call.initiated` for one call: earliest wins | Can't be both; earliest is the dial | Latest |
| Unknown-code `disposition.*` events are ignored entirely (an earlier valid `set` becomes effective again) | Spec says "ignore it" | Treat as "no disposition" for the call |
| Overrides link **only** via `call_id` (phone ignored if present) | Spec wording; avoids two linking paths | Phone first, call_id fallback |
| If one `call_id` is seen with two different leads, the first wins | Not in the data; ids are unique per call | Reject both |
| Rounding: decimals round half-up (Decimal); `connectivity_rate` uses Python's `f"{x:.4f}"` | Matches the likely reference; only differs on exact ties | Half-up for the rate |
| Required non-phone field present but invalid -> reject as `invalid_<field>` | Spec only defines optional-field handling; no such field exists in the schemas | Treat as warning |
| `rejections_by_reason` lists every possible reason with 0 for unseen ones | The contract example shows zeros | Only observed reasons |
| `batch_summary` has a row per batch that created >=1 lead | Derived from leads | A zero row for all-rejected batches |
| If two columns match the same field, the left-most wins | Deterministic | Error out |
| File identity = SHA-256 of content (+kind, workflow) | Same content = same file even if copied/renamed. Different content with same name is treated as new data | Path/name only |
| Unreadable bytes in a CSV/JSONL are decoded with replacement characters instead of crashing | One bad byte should not stop a drop | Fail the file |

## Failure modes considered
- **Re-run / partial runs:** per-file transaction + content hash: nothing double counts; a crash mid-file leaves nothing (tested by injecting an exception).
- **Out-of-order / late data:** events before their lead, overrides before/after sets, duplicates across drops: all handled by deriving at report time (tested).
- **Bad input:** non-JSON or truncated lines, non-object JSON, missing `event_id`, BOM in CSV, rows shorter/longer than the header, blank rows, unknown columns, workflows without a schema (skipped with a log line).
- **Wrong key:** a key-check value in `meta` makes opening the DB with the wrong key fail loudly instead of silently producing orphans.
- **Not handled:** concurrent ingest from two processes (SQLite serialises writers via `BEGIN IMMEDIATE`, but I did not test it); key rotation (would need re-encrypting `pii_enc` and rebuilding the blind index); corrected re-delivery of a *changed* lead file is merged as new rows (last write wins) and counts as duplicates, not as a replacement; extremely large single files are read fully into memory.

## Testing
97 tests (`pytest`, ~4 s):
- **Unit** (`test_normalize.py`, `test_derive.py`): every phone format the generator emits plus invalid ones and the 91/0-prefix edge cases, headers, all four date formats and impossible dates, decimals/currency prefixes, the three timestamp forms and IST day roll-over, effective disposition (override beats later set, latest override, ties), bucket order.
- **End-to-end hand-computed fixture** (`test_pipeline.py`): a 5-lead dataset whose expected outputs I worked out on paper: rejections and row numbering with a blank row, merge/default semantics, renamed+re-ordered columns, duplicates in-drop and across drops, malformed lines, orphan override via `call_id`, an event that arrives a drop before its lead, naive/offset/UTC timestamps, IST date change. Also: re-ingest no-op, incremental == all-at-once, no plaintext PII in the DB file, append-only triggers, crash rollback.
- **Property tests on generator output** (two other seeds): incremental == all-at-once with *different PII keys*; reports are internally consistent (batch totals = lead_report grouped, call totals match across views, accounting identity `rows_total = rejected + leads + merged`); event counters match an independent naive recount of the raw files.
- I mutation-checked the tests: breaking override precedence, the naive-timestamp zone, and override linking each made tests fail.
- **Not tested:** no independent oracle for the full report on generated data. The first rows of my seed-42 output do match the examples printed in the spec (`0.5625`, `2026-10-14T17:55:24+05:30`), but I cannot diff against expected output.

## Results
Sandbox, `--seed 42 --scale large` (143,990 lead rows, 632,602 event lines): **ingest 18.8 s (drop_1) + 23.9 s (drop_2), report 6.2 s, total ~49 s**; peak RSS ~306 MB; DB ~229 MB. Small scale: <1 s end to end.
Seed 42 small: 2,057 lead rows -> 108 rejected, 57 merged, 1,892 leads; 8,745 event lines -> 8 malformed, 237 duplicates, 462 orphans, 18 unknown dispositions.
Surprises: ~5% of lead rows are rejected, and ~5% of events are orphans because rejected leads (missing name/ref) and invalid phones still generate calls. Duplicate delivery crosses drops often, so dedupe must be global (it is: `UNIQUE(event_id)`).

## Production note (100k leads/day)
Run `ingest` per delivered file from a queue/object-store trigger (it is idempotent, so at-least-once delivery is fine) and `report` on a schedule (e.g. every 15 min). Move to Postgres: partition `events` by `occurred_at`, index `(workflow_id, phone_hash)` and `call_id`, run linking/aggregation in SQL as materialised views refreshed incrementally. Store the key in a KMS and rotate via envelope encryption. Alert on rising malformed/orphan ratios, since those usually mean an upstream schema change. Backfills = re-ingest files (no-ops if seen) then re-run `report`, since nothing derived is stored.

## If I had another day
1. Do the aggregation in SQL (and compare against the Python path as a second oracle).
2. Truly incremental reporting: recompute only calls/leads touched by new files (needs a dirty-set because a new lead re-links old events).
3. Speed up event ingest with batched `executemany` (lead ingest must stay sequential because merge order matters).
4. Property-based tests (Hypothesis) for phone/date/decimal normalisers.
5. Key rotation tooling and a `verify` command that re-checks the ledger invariants.
