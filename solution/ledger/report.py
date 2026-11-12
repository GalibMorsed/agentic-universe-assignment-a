"""Builds the five report files from the ledger.

Everything is recomputed from the current ledger + leads on every run. That is what makes the
output independent of ingest order/batching: linking (event -> lead), orphan status and
effective dispositions are *derived*, never stored, so a lead that arrives later automatically
"adopts" its earlier events.
"""
from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from .catalog import Catalog
from .derive import BUCKETS, Call, effective_disposition, lead_bucket
from .normalize import ist_date, ist_iso

OVERRIDE = "disposition.override"


@dataclass
class _CallAgg:
    lead_id: int
    initiated_us: int | None = None
    connected: bool = False
    failed: bool = False

    def __post_init__(self):
        self.sets: list = []
        self.overrides: list = []


def compute(conn: sqlite3.Connection, catalog: Catalog) -> dict:
    # ---- leads
    leads: dict[int, tuple] = {}            # id -> (workflow, last4, batch, ref)
    by_key: dict[tuple[str, bytes], int] = {}
    for lid, wf, ph, last4, batch, ref in conn.execute(
            "SELECT id, workflow_id, phone_hash, phone_last4, batch_id, external_ref FROM leads"):
        leads[lid] = (wf, last4, batch, ref)
        by_key[(wf, ph)] = lid

    # ---- pass 1: which lead does each call belong to? (overrides have no phone, only call_id)
    call_lead: dict[str, int] = {}
    for call_id, wf, ph in conn.execute(
            "SELECT call_id, workflow_id, phone_hash FROM events"
            " WHERE event_type != ? AND phone_hash IS NOT NULL AND call_id IS NOT NULL", (OVERRIDE,)):
        lid = by_key.get((wf, ph))
        if lid is not None:
            call_lead.setdefault(call_id, lid)

    # ---- pass 2: classify every ledger event, group the good ones by call
    orphans = unknown = 0
    calls: dict[str, _CallAgg] = {}
    for eid, etype, call_id, us, ph, wf, code in conn.execute(
            "SELECT event_id, event_type, call_id, occurred_us, phone_hash, workflow_id,"
            " disposition_code FROM events ORDER BY seq"):
        if etype == OVERRIDE:
            lid = call_lead.get(call_id) if call_id is not None else None
        else:
            lid = by_key.get((wf, ph)) if ph is not None else None
        if lid is None:
            orphans += 1                                   # not linkable to any valid lead
            continue
        if etype.startswith("disposition.") and catalog.disposition(leads[lid][0], code) is None:
            unknown += 1                                   # linked, but code not in the catalogue
            continue
        if call_id is None:
            continue
        agg = calls.get(call_id)
        if agg is None:
            agg = calls[call_id] = _CallAgg(lead_id=lid)
        if etype == "call.initiated":
            agg.initiated_us = us if agg.initiated_us is None else min(agg.initiated_us, us)
        elif etype == "call.connected":
            agg.connected = True
        elif etype == "call.telephony_failed":
            agg.failed = True
        elif etype == "disposition.set":
            agg.sets.append((us, eid, code))
        elif etype == OVERRIDE:
            agg.overrides.append((us, eid, code))

    # ---- derived facts per lead
    lead_calls: dict[int, list[Call]] = defaultdict(list)
    for call_id, a in calls.items():
        if a.initiated_us is None:                         # counted only if call.initiated present
            continue
        lead_calls[a.lead_id].append(Call(
            call_id, a.initiated_us, a.connected, a.failed,
            effective_disposition(a.sets, a.overrides)))

    batch_rows: dict[tuple[str, str], Counter] = defaultdict(Counter)
    lead_rows = []
    daily: dict[tuple[str, object], list] = defaultdict(lambda: [0, 0, set()])
    for lid, (wf, last4, batch, ref) in leads.items():
        cl = sorted(lead_calls.get(lid, []), key=lambda c: (c.initiated_us, c.call_id))

        def flags(code):
            return catalog.disposition(wf, code)

        bucket = lead_bucket(cl, lambda code: flags(code)[0])
        b = batch_rows[(wf, batch)]
        b["total_leads"] += 1
        b[bucket] += 1
        with_disp = [c for c in cl if c.disposition is not None]
        lead_rows.append((wf, ref, lid, [
            wf, ref, batch, "+91******" + last4, bucket, len(cl), sum(c.connected for c in cl),
            ist_iso(cl[-1].initiated_us) if cl else "",
            with_disp[-1].disposition if with_disp else "",
            str(any(flags(c.disposition)[1] for c in with_disp)).lower()]))
        for c in cl:
            d = daily[(wf, ist_date(c.initiated_us))]
            d[0] += 1
            d[1] += c.connected
            if c.disposition is not None and flags(c.disposition)[1]:
                d[2].add(lid)

    # ---- data quality
    fstats = [(k, json.loads(s)) for k, s in conn.execute("SELECT kind, stats FROM ingested_files")]
    def total(kind, key):
        return sum(s.get(key, 0) for k, s in fstats if k == kind)
    reasons = {r: 0 for r in catalog.rejection_reasons()}
    for reason, n in conn.execute("SELECT reason, COUNT(*) FROM rejections GROUP BY reason"):
        reasons[reason] = n
    quality = {
        "lead_rows_total": total("leads", "rows_total"),
        "lead_rows_rejected": total("leads", "rows_rejected"),
        "rejections_by_reason": dict(sorted(reasons.items())),
        "duplicate_lead_rows_merged": total("leads", "duplicates_merged"),
        "field_warnings": total("leads", "field_warnings"),
        "events_total": total("events", "events_total"),
        "malformed_events": total("events", "malformed"),
        "duplicate_events_dropped": total("events", "duplicates_dropped"),
        "orphan_events": orphans,
        "unknown_disposition_events": unknown,
    }
    return {"batches": batch_rows, "leads": lead_rows, "daily": daily, "quality": quality}


def write_reports(conn: sqlite3.Connection, catalog: Catalog, out_dir: Path) -> None:
    data = compute(conn, catalog)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    def write_csv(name, header, rows):
        with open(out / name, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(header)
            w.writerows(rows)

    write_csv("batch_summary.csv",
              ["workflow_id", "batch_id", "total_leads", *BUCKETS],
              [[wf, batch, c["total_leads"], *(c[b] for b in BUCKETS)]
               for (wf, batch), c in sorted(data["batches"].items())])

    write_csv("daily_overview.csv",
              ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
              [[wf, d.isoformat(), tot, con, f"{con / tot:.4f}", len(succ)]
               for (wf, d), (tot, con, succ) in sorted(data["daily"].items())])

    write_csv("lead_report.csv",
              ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket", "total_calls",
               "connected_calls", "last_call_at", "current_disposition", "ever_success"],
              [r[3] for r in sorted(data["leads"], key=lambda r: (r[0], r[1], r[2]))])

    write_csv("rejections.csv", ["workflow_id", "batch_id", "row_number", "reason"],
              conn.execute("SELECT workflow_id, batch_id, row_number, reason FROM rejections"
                           " ORDER BY workflow_id, batch_id, row_number, id"))

    (out / "data_quality.json").write_text(json.dumps(data["quality"], indent=2) + "\n", encoding="utf-8")
