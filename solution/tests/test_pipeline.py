"""End-to-end tests on a tiny hand-written dataset whose correct output was worked out by hand."""
import csv
import json
import shutil
from pathlib import Path

import pytest

from ledger import ingest as ingest_mod
from ledger.catalog import load_catalog
from ledger.db import connect
from ledger.ingest import ingest_path
from ledger.pii import Crypto
from ledger.report import write_reports

REPO = Path(__file__).resolve().parents[2]
CATALOG = load_catalog(REPO / "assignment-a-data" / "schemas")

LEADS_BATCH_001 = """customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language
Asha Rao,98765 43210,POL-1,2026-12-01,"₹1,200",HI
,9876500001,POL-2,,,
Ravi K,12345,POL-3,,,
Meera S,+91 98765 43211,POL-4,31/02/2026,abc,Hindi
,,,,,
Neel D,098765 43212,POL-5,01-Dec-2026,Rs. 500,
Pia T,91-98765-43213,POL-6,,,
Asha Rao,9876543210,POL-1,,INR 1500.5,
"""
# renamed + re-ordered columns, an unknown extra column, arrives in the second drop
LEADS_BATCH_002 = """Language,Insured Name,Extra_Col,Mobile_No,POLICY NO
TA,Late Lead,x,98765 43299,POL-9
"""


def ev(n, etype, call, ts, phone=None, payload=None, wf="motor_renewal"):
    e = {"event_id": f"evt_{n:03d}", "event_type": etype, "workflow_id": wf, "call_id": call,
         "occurred_at": ts, "payload": payload or {}}
    if phone is not None:
        e["phone"] = phone
    return json.dumps(e)


DROP1_EVENTS = [
    # Asha: c1 callback_requested; c2 renewed but a supervisor overrides it to not_interested
    ev(1, "call.initiated", "c1", "2026-10-01T10:00:00Z", "9876543210"),
    ev(2, "call.connected", "c1", "2026-10-01T15:31:00+05:30", "9876543210"),
    ev(3, "call.ended", "c1", "2026-10-01T15:36:00+05:30", "9876543210", {"duration_sec": 300}),
    ev(4, "disposition.set", "c1", "2026-10-01T15:40:00+05:30", "9876543210", {"disposition_code": "callback_requested"}),
    ev(5, "call.initiated", "c2", "2026-10-02 09:00:00", "+91 98765 43210"),
    ev(6, "call.connected", "c2", "2026-10-02 09:00:20", "+91 98765 43210"),
    ev(7, "disposition.set", "c2", "2026-10-02 09:05:00", "+91 98765 43210", {"disposition_code": "renewed"}),
    ev(8, "disposition.override", "c2", "2026-10-02T10:00:00+05:30", None,
       {"disposition_code": "not_interested", "overridden_by": "supervisor_1", "reason": "qa_review"}),
    ev(1, "call.initiated", "c1", "2026-10-01T10:00:00Z", "9876543210"),          # duplicate of evt_001
    "{not json at all",                                                          # malformed
    json.dumps({"event_type": "call.initiated", "call_id": "cx"}),               # no event_id -> malformed
    # Meera: one call, telephony failed; 23:00Z is already the next day in IST
    ev(9, "call.initiated", "c3", "2026-10-01T23:00:00Z", "+91 98765 43211"),
    ev(10, "call.telephony_failed", "c3", "2026-10-01T23:00:05Z", "+91 98765 43211", {"reason": "sip_503"}),
    # Late Lead's calls arrive a drop BEFORE the lead itself -> orphans until drop_2
    ev(11, "call.initiated", "c4", "2026-10-03T05:00:00Z", "98765 43299"),
    ev(12, "disposition.set", "c4", "2026-10-03T05:09:00Z", "98765 43299", {"disposition_code": "BOGUS"}),
    ev(13, "call.connected", "c4", "2026-10-03T05:00:10Z", "98765 43299"),
    # real orphans: never-uploaded number, its override, and an invalid phone
    ev(14, "call.initiated", "c5", "2026-10-03T06:00:00Z", "9000000001"),
    ev(15, "disposition.override", "c5", "2026-10-03T07:00:00Z", None, {"disposition_code": "dnd"}),
    ev(16, "call.initiated", "c6", "2026-10-03T06:00:00Z", "123"),
    # Neel: renewed
    ev(18, "call.initiated", "c7", "2026-10-01T04:30:00Z", "098765 43212"),
    ev(19, "call.connected", "c7", "2026-10-01T04:30:30Z", "098765 43212"),
    ev(20, "disposition.set", "c7", "2026-10-01T04:35:00Z", "098765 43212", {"disposition_code": "renewed"}),
]
DROP2_EVENTS = [
    # unknown code for a linked lead -> counted + ignored, original callback_requested stays effective for c1
    ev(17, "disposition.set", "c1", "2026-10-05T10:00:00Z", "9876543210", {"disposition_code": "call_back"}),
    ev(11, "call.initiated", "c4", "2026-10-03T05:00:00Z", "98765 43299"),         # duplicate across drops
]


def make_drops(root: Path):
    (root / "drop_1/leads/motor_renewal").mkdir(parents=True)
    (root / "drop_1/events").mkdir(parents=True)
    (root / "drop_2/leads/motor_renewal").mkdir(parents=True)
    (root / "drop_2/events").mkdir(parents=True)
    (root / "drop_1/leads/motor_renewal/batch_001.csv").write_text(LEADS_BATCH_001, encoding="utf-8")
    (root / "drop_2/leads/motor_renewal/batch_002.csv").write_text(LEADS_BATCH_002, encoding="utf-8")
    (root / "drop_1/events/events_0001.jsonl").write_text("\n".join(DROP1_EVENTS) + "\n", encoding="utf-8")
    (root / "drop_2/events/events_1001.jsonl").write_text("\n".join(DROP2_EVENTS) + "\n", encoding="utf-8")
    return root


def open_db(tmp: Path, name="t.db"):
    crypto = Crypto(b"unit-test-master-key-0123456789")
    return connect(str(tmp / name), crypto), crypto


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture()
def dataset(tmp_path):
    return make_drops(tmp_path / "data")


def run_all(tmp_path, dataset, out="out", db="t.db"):
    conn, crypto = open_db(tmp_path, db)
    ingest_path(conn, crypto, CATALOG, dataset / "drop_1", log=lambda m: None)
    ingest_path(conn, crypto, CATALOG, dataset / "drop_2", log=lambda m: None)
    write_reports(conn, CATALOG, tmp_path / out)
    return conn, tmp_path / out


def test_lead_ingestion_rules(tmp_path, dataset):
    _, out = run_all(tmp_path, dataset)
    # rejections: row numbers skip the blank row; first failing field wins
    assert read_csv(out / "rejections.csv") == [
        {"workflow_id": "motor_renewal", "batch_id": "batch_001", "row_number": "2",
         "reason": "missing_required:customer_name"},
        {"workflow_id": "motor_renewal", "batch_id": "batch_001", "row_number": "3",
         "reason": "invalid_phone"},
    ]
    dq = json.loads((out / "data_quality.json").read_text())
    assert dq["lead_rows_total"] == 7 + 1          # batch_001 has 7 non-blank rows, batch_002 has 1
    assert dq["lead_rows_rejected"] == 2
    assert dq["duplicate_lead_rows_merged"] == 1   # second Asha row
    assert dq["field_warnings"] == 3               # Meera: bad date, bad amount, bad language
    assert dq["rejections_by_reason"]["invalid_phone"] == 1
    assert dq["rejections_by_reason"]["missing_required:customer_name"] == 1
    assert dq["rejections_by_reason"]["missing_required:policy_number"] == 0


def test_merge_last_non_null_wins_and_defaults_apply(tmp_path, dataset):
    conn, _ = run_all(tmp_path, dataset)
    crypto = Crypto(b"unit-test-master-key-0123456789")
    attrs = {r[0]: json.loads(r[1]) for r in conn.execute("SELECT external_ref, attrs FROM leads")}
    assert attrs["POL-1"]["premium_amount"] == "1500.50"       # overwritten by the later duplicate
    assert attrs["POL-1"]["preferred_language"] == "hi"        # later row was null -> kept
    assert attrs["POL-1"]["policy_expiry"] == "2026-12-01"
    assert attrs["POL-4"]["preferred_language"] == "en"        # 'Hindi' invalid -> null -> default
    assert attrs["POL-4"]["policy_expiry"] is None and attrs["POL-4"]["premium_amount"] is None
    assert attrs["POL-5"]["premium_amount"] == "500.00" and attrs["POL-5"]["policy_expiry"] == "2026-12-01"
    assert attrs["POL-9"]["preferred_language"] == "ta"        # renamed/re-ordered columns still map
    names = {crypto.decrypt(b)["customer_name"] for (b,) in conn.execute("SELECT pii_enc FROM leads")}
    assert names == {"Asha Rao", "Meera S", "Neel D", "Pia T", "Late Lead"}


def test_batch_summary_and_lead_report(tmp_path, dataset):
    _, out = run_all(tmp_path, dataset)
    assert read_csv(out / "batch_summary.csv") == [
        {"workflow_id": "motor_renewal", "batch_id": "batch_001", "total_leads": "4",
         "yet_to_reach_out": "1", "in_progress_telephony_failed": "1", "in_progress_dialed": "0", "completed": "2"},
        {"workflow_id": "motor_renewal", "batch_id": "batch_002", "total_leads": "1",
         "yet_to_reach_out": "0", "in_progress_telephony_failed": "0", "in_progress_dialed": "1", "completed": "0"},
    ]
    rows = {r["external_ref"]: r for r in read_csv(out / "lead_report.csv")}
    assert list(rows) == ["POL-1", "POL-4", "POL-5", "POL-6", "POL-9"]            # sorted by external_ref
    assert rows["POL-1"] == {
        "workflow_id": "motor_renewal", "external_ref": "POL-1", "batch_id": "batch_001",
        "phone_masked": "+91******3210", "bucket": "completed", "total_calls": "2", "connected_calls": "2",
        "last_call_at": "2026-10-02T09:00:00+05:30",       # naive input = IST
        "current_disposition": "not_interested",            # override beat the AI's 'renewed'
        "ever_success": "false"}                            # ... so no success anywhere
    assert rows["POL-4"]["bucket"] == "in_progress_telephony_failed"
    assert rows["POL-4"]["last_call_at"] == "2026-10-02T04:30:00+05:30"   # 23:00Z is next day in IST
    assert rows["POL-4"]["connected_calls"] == "0" and rows["POL-4"]["current_disposition"] == ""
    assert (rows["POL-5"]["bucket"], rows["POL-5"]["ever_success"], rows["POL-5"]["current_disposition"]) == \
        ("completed", "true", "renewed")
    assert rows["POL-6"]["bucket"] == "yet_to_reach_out" and rows["POL-6"]["last_call_at"] == ""
    # Late Lead: events arrived a drop earlier; the BOGUS disposition is ignored, so just 'dialed'
    assert (rows["POL-9"]["bucket"], rows["POL-9"]["total_calls"], rows["POL-9"]["connected_calls"],
            rows["POL-9"]["current_disposition"]) == ("in_progress_dialed", "1", "1", "")


def test_daily_overview(tmp_path, dataset):
    _, out = run_all(tmp_path, dataset)
    assert read_csv(out / "daily_overview.csv") == [
        {"workflow_id": "motor_renewal", "date": "2026-10-01", "total_calls": "2", "connected_calls": "2",
         "connectivity_rate": "1.0000", "success_leads": "1"},     # Asha c1 + Neel c7 (renewed)
        {"workflow_id": "motor_renewal", "date": "2026-10-02", "total_calls": "2", "connected_calls": "1",
         "connectivity_rate": "0.5000", "success_leads": "0"},     # Asha c2 (override -> not a success) + Meera c3
        {"workflow_id": "motor_renewal", "date": "2026-10-03", "total_calls": "1", "connected_calls": "1",
         "connectivity_rate": "1.0000", "success_leads": "0"},
    ]


def test_event_quality_counters_reflect_final_state(tmp_path, dataset):
    _, out = run_all(tmp_path, dataset)
    dq = json.loads((out / "data_quality.json").read_text())
    assert dq["events_total"] == 22 + 2             # non-empty lines in both drops, malformed + duplicates included
    assert dq["malformed_events"] == 2
    assert dq["duplicate_events_dropped"] == 2      # one in-drop (evt_001), one across drops (evt_011)
    assert dq["orphan_events"] == 3                 # evt_014, evt_015 (override via call_id), evt_016
    assert dq["unknown_disposition_events"] == 2    # BOGUS (evt_012, linked once its lead arrived) + call_back


def test_orphans_become_linked_when_the_lead_arrives_later(tmp_path, dataset):
    conn, crypto = open_db(tmp_path)
    ingest_path(conn, crypto, CATALOG, dataset / "drop_1", log=lambda m: None)
    write_reports(conn, CATALOG, tmp_path / "after1")
    dq1 = json.loads((tmp_path / "after1/data_quality.json").read_text())
    assert dq1["orphan_events"] == 3 + 3            # + Late Lead's evt_011..013 (no lead yet)
    assert dq1["unknown_disposition_events"] == 0   # BOGUS is only 'orphan' at this point
    ingest_path(conn, crypto, CATALOG, dataset / "drop_2", log=lambda m: None)
    write_reports(conn, CATALOG, tmp_path / "after2")
    dq2 = json.loads((tmp_path / "after2/data_quality.json").read_text())
    assert dq2["orphan_events"] == 3 and dq2["unknown_disposition_events"] == 2


def test_reingest_is_a_noop(tmp_path, dataset):
    conn, out = run_all(tmp_path, dataset)
    before = {p.name: p.read_bytes() for p in out.iterdir()}
    counts = [conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in ("leads", "events", "rejections", "ingested_files")]
    crypto = Crypto(b"unit-test-master-key-0123456789")
    for _ in range(2):
        ingest_path(conn, crypto, CATALOG, dataset, log=lambda m: None)       # whole folder, twice
    write_reports(conn, CATALOG, tmp_path / "out2")
    assert {p.name: p.read_bytes() for p in (tmp_path / "out2").iterdir()} == before
    assert counts == [conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                      for t in ("leads", "events", "rejections", "ingested_files")]


def test_incremental_equals_all_at_once(tmp_path, dataset):
    _, out_inc = run_all(tmp_path, dataset, out="inc", db="inc.db")
    conn, crypto = open_db(tmp_path, "all.db")
    ingest_path(conn, crypto, CATALOG, dataset, log=lambda m: None)          # both drops in one go
    write_reports(conn, CATALOG, tmp_path / "all")
    for f in out_inc.iterdir():
        assert f.read_bytes() == (tmp_path / "all" / f.name).read_bytes(), f.name


def test_no_plaintext_pii_in_the_database_file(tmp_path, dataset):
    conn, _ = run_all(tmp_path, dataset)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    raw = b"".join(p.read_bytes() for p in tmp_path.glob("t.db*"))
    for secret in [b"Asha", b"Rao", b"Meera", b"Late Lead", b"9876543210", b"98765 43210",
                   b"98765-43210", b"9876543299", b"+919876543210"]:
        assert secret not in raw, secret


def test_ledger_is_append_only(tmp_path, dataset):
    conn, _ = run_all(tmp_path, dataset)
    with pytest.raises(Exception, match="append-only"):
        conn.execute("UPDATE events SET event_type='x'")
    with pytest.raises(Exception, match="append-only"):
        conn.execute("DELETE FROM events")


def test_override_never_edits_the_original_event(tmp_path, dataset):
    conn, _ = run_all(tmp_path, dataset)
    codes = dict(conn.execute("SELECT event_id, disposition_code FROM events WHERE event_id IN ('evt_007','evt_008')"))
    assert codes == {"evt_007": "renewed", "evt_008": "not_interested"}


def test_a_crash_mid_file_leaves_nothing_behind(tmp_path, dataset, monkeypatch):
    conn, crypto = open_db(tmp_path)
    real = ingest_mod.parse_event_line
    calls = {"n": 0}

    def exploding(line, c):
        calls["n"] += 1
        if calls["n"] == 5:
            raise RuntimeError("simulated crash")
        return real(line, c)

    monkeypatch.setattr(ingest_mod, "parse_event_line", exploding)
    with pytest.raises(RuntimeError):
        ingest_path(conn, crypto, CATALOG, dataset / "drop_1", log=lambda m: None)
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM ingested_files WHERE kind='events'").fetchone()[0] == 0
    monkeypatch.setattr(ingest_mod, "parse_event_line", real)        # restart: everything succeeds
    ingest_path(conn, crypto, CATALOG, dataset / "drop_1", log=lambda m: None)
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 22 - 1 - 2   # 22 lines - 1 duplicate - 2 malformed
