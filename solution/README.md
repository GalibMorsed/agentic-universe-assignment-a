# Assignment A: Interaction Ledger & Reporting Pipeline

Python 3.11+, SQLite (stdlib) and `cryptography` (PII encryption). Details and design reasoning are in
[`../SUBMISSION.md`](../SUBMISSION.md).

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt                       # from the repo root
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

cd solution                                            # the package lives here
python -m ledger ingest ../data/drop_1
python -m ledger ingest ../data/drop_2                 # or: python -m ledger ingest ../data  (all drops, in order)
python -m ledger report --out ../reports/
```

* `--db PATH` (before the sub-command) chooses the SQLite file, default `./ledger.db`.
* The PII key is read from `LEDGER_PII_KEY` (urlsafe-base64, 32 bytes) or, if unset, generated once
  into `<db>.key` (mode 0600). Keep it with the database; never commit either.
* Re-running `ingest` on an already-ingested file prints `already ingested, skipped` and changes nothing.

## Test

```bash
pytest            # from the repo root or from solution/ (97 tests, ~4 s)
```

## Layout

| File | Responsibility |
|---|---|
| `ledger/normalize.py` | header, phone, date, decimal, enum, timestamp rules (pure functions) |
| `ledger/catalog.py` | loads workflow schemas + `dispositions.csv` |
| `ledger/pii.py` | Fernet encryption + keyed-HMAC blind index for phones |
| `ledger/db.py` | SQLite schema, append-only triggers |
| `ledger/ingest.py` | lead CSV + event JSONL ingestion, one transaction per file |
| `ledger/derive.py` | effective disposition and lead bucket (pure functions) |
| `ledger/report.py` | links events to leads, derives facts, writes the five files |
| `tests/` | unit tests, hand-computed end-to-end fixture, generator-based property tests |
