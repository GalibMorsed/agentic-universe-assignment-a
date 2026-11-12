"""Property-style checks on data from the official generator (different seeds, small scale)."""
import csv
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from ledger.catalog import load_catalog
from ledger.db import connect
from ledger.ingest import ingest_path
from ledger.pii import Crypto
from ledger.report import write_reports

REPO = Path(__file__).resolve().parents[2]
CATALOG = load_catalog(REPO / "assignment-a-data" / "schemas")
GEN = REPO / "assignment-a-data" / "tools" / "generate_data.py"
BUCKETS = ["yet_to_reach_out", "in_progress_telephony_failed", "in_progress_dialed", "completed"]


@pytest.fixture(scope="module", params=[1, 2024])
def generated(request, tmp_path_factory):
    root = tmp_path_factory.mktemp(f"seed{request.param}")
    subprocess.run([sys.executable, str(GEN), "--seed", str(request.param), "--scale", "small",
                    "--out", str(root / "data")], check=True, capture_output=True)
    return root


def build(root, name, per_drop):
    crypto = Crypto((name * 8).encode())       # a different key per DB: outputs must not depend on it
    conn = connect(str(root / f"{name}.db"), crypto)
    data = root / "data"
    for target in ([data / "drop_1", data / "drop_2"] if per_drop else [data]):
        ingest_path(conn, crypto, CATALOG, target, log=lambda m: None)
    write_reports(conn, CATALOG, root / name)
    return root / name


def rows(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_incremental_equals_all_at_once_and_is_key_independent(generated):
    inc = build(generated, "inc", per_drop=True)
    allatonce = build(generated, "all", per_drop=False)
    for f in inc.iterdir():
        assert f.read_bytes() == (allatonce / f.name).read_bytes(), f.name


def test_reports_are_internally_consistent(generated):
    out = build(generated, "chk", per_drop=True)
    leads = rows(out / "lead_report.csv")
    batches = rows(out / "batch_summary.csv")
    dq = json.loads((out / "data_quality.json").read_text())
    # every lead is in exactly one bucket, and batch_summary is just lead_report grouped
    by_batch = Counter((r["workflow_id"], r["batch_id"], r["bucket"]) for r in leads)
    for b in batches:
        assert int(b["total_leads"]) == sum(int(b[k]) for k in BUCKETS)
        for k in BUCKETS:
            assert int(b[k]) == by_batch[(b["workflow_id"], b["batch_id"], k)]
    assert sum(int(b["total_leads"]) for b in batches) == len(leads)
    # lead accounting: total rows = rejected + new leads + merged duplicates
    assert dq["lead_rows_total"] == dq["lead_rows_rejected"] + len(leads) + dq["duplicate_lead_rows_merged"]
    assert dq["lead_rows_rejected"] == sum(dq["rejections_by_reason"].values()) == len(rows(out / "rejections.csv"))
    # per-call counts add up across both views
    daily = rows(out / "daily_overview.csv")
    assert sum(int(r["total_calls"]) for r in daily) == sum(int(r["total_calls"]) for r in leads)
    assert sum(int(r["connected_calls"]) for r in daily) == sum(int(r["connected_calls"]) for r in leads)
    for r in leads:
        assert r["phone_masked"].startswith("+91******") and len(r["phone_masked"]) == 13
        assert (r["bucket"] == "yet_to_reach_out") == (r["total_calls"] == "0")
        assert int(r["connected_calls"]) <= int(r["total_calls"])
    for r in daily:
        assert r["connectivity_rate"] == f"{int(r['connected_calls']) / int(r['total_calls']):.4f}"


def test_event_counters_match_a_naive_recount_of_the_raw_files(generated):
    """Independent of the pipeline's parsing code: plain json + a set."""
    out = build(generated, "raw", per_drop=True)
    dq = json.loads((out / "data_quality.json").read_text())
    total = malformed = 0
    seen = set()
    dups = 0
    for d in ("drop_1", "drop_2"):
        for f in sorted((generated / "data" / d / "events").glob("*.jsonl")):
            for line in f.read_text(encoding="utf-8").split("\n"):
                if not line.strip():
                    continue
                total += 1
                try:
                    eid = json.loads(line)["event_id"]
                except (ValueError, KeyError):
                    malformed += 1
                    continue
                if eid in seen:
                    dups += 1
                seen.add(eid)
    assert (dq["events_total"], dq["malformed_events"], dq["duplicate_events_dropped"]) == (total, malformed, dups)
    assert dq["orphan_events"] + dq["unknown_disposition_events"] < len(seen)
