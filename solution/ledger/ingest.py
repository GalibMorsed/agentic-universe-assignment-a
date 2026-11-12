"""Ingestion of lead CSVs and event JSONL files.

Idempotency: every file is identified by (kind, workflow, sha256). A file that is already
in `ingested_files` is skipped. A file's rows and its `ingested_files` row are written in ONE
transaction, so a crash leaves either nothing or the whole file.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from .catalog import Catalog, Workflow
from .normalize import normalize_header, normalize_phone, parse_timestamp, parse_value, to_epoch_us
from .pii import Crypto


def _natural(p: Path):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p.name)]


def _register_file(conn, kind, workflow_id, drop, name, batch_id, digest) -> Optional[int]:
    """Insert the ingested_files row; None if this exact file was already ingested."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO ingested_files (kind, workflow_id, drop_name, file_name, batch_id,"
        " sha256, ingested_at) VALUES (?,?,?,?,?,?,?)",
        (kind, workflow_id, drop, name, batch_id, digest,
         datetime.now(timezone.utc).isoformat(timespec="seconds")))
    return cur.lastrowid if cur.rowcount else None


# ------------------------------------------------------------------- leads
def parse_lead_row(wf: Workflow, colmap: dict[str, int], row: list[str]):
    """Validate one non-blank row. Returns (values, warnings, reject_reason).

    Walks the schema in order; the first failure is the single rejection reason. Warnings are
    only meaningful when the row is not rejected (the caller discards them otherwise).
    """
    values: dict[str, Optional[str]] = {}
    warnings = 0
    for f in wf.fields:
        idx = colmap.get(f.name)
        raw = row[idx].strip() if idx is not None and idx < len(row) else ""
        if raw == "":
            if f.required:
                return None, 0, f"missing_required:{f.name}"
            values[f.name] = None
            continue
        ok, val = parse_value(f, raw)
        if ok:
            values[f.name] = val
        elif f.type == "phone":
            return None, 0, "invalid_phone"
        elif f.required:
            return None, 0, f"invalid_{f.name}"
        else:
            values[f.name] = None
            warnings += 1
    return values, warnings, None


def ingest_lead_file(conn: sqlite3.Connection, crypto: Crypto, wf: Workflow, path: Path,
                     drop: str) -> Optional[dict]:
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    batch_id = path.stem
    conn.execute("BEGIN IMMEDIATE")
    try:
        file_id = _register_file(conn, "leads", wf.workflow_id, drop, path.name, batch_id, digest)
        if file_id is None:
            conn.execute("ROLLBACK")
            return None
        reader = csv.reader(io.StringIO(data.decode("utf-8-sig", errors="replace"), newline=""))
        header = next(reader, None) or []
        colmap: dict[str, int] = {}
        for idx, h in enumerate(header):
            norm = normalize_header(h)
            for f in wf.fields:
                if norm in f.aliases and f.name not in colmap:
                    colmap[f.name] = idx
                    break
        pii_names = [f.name for f in wf.fields if f.pii]
        ref_name = wf.reference_field.name
        phone_name = wf.phone_field.name
        stats = {"rows_total": 0, "rows_rejected": 0, "duplicates_merged": 0, "field_warnings": 0}
        rejections = []
        for row in reader:
            if all(c.strip() == "" for c in row):       # blank row: skipped, not counted
                continue
            stats["rows_total"] += 1
            values, warns, reason = parse_lead_row(wf, colmap, row)
            if reason:
                stats["rows_rejected"] += 1
                rejections.append((file_id, wf.workflow_id, batch_id, stats["rows_total"], reason))
                continue
            stats["field_warnings"] += warns
            phone = values[phone_name]
            phash = crypto.phone_hash(phone)
            existing = conn.execute(
                "SELECT id, attrs, pii_enc FROM leads WHERE workflow_id=? AND phone_hash=?",
                (wf.workflow_id, phash)).fetchone()
            if existing is None:
                for f in wf.fields:                      # defaults for still-null fields
                    if values[f.name] is None and f.default is not None:
                        values[f.name] = f.default
                conn.execute(
                    "INSERT INTO leads (workflow_id, phone_hash, phone_last4, batch_id,"
                    " external_ref, attrs, pii_enc, file_id) VALUES (?,?,?,?,?,?,?,?)",
                    (wf.workflow_id, phash, phone[-4:], batch_id, values[ref_name],
                     json.dumps({k: v for k, v in values.items() if k not in pii_names}, sort_keys=True),
                     crypto.encrypt({k: values[k] for k in pii_names}), file_id))
            else:                                        # duplicate: last non-null write wins
                stats["duplicates_merged"] += 1
                lead_id, attrs_json, pii_enc = existing
                attrs = json.loads(attrs_json)
                pii = crypto.decrypt(pii_enc)
                for k, v in values.items():
                    if v is None:
                        continue
                    (pii if k in pii_names else attrs)[k] = v
                conn.execute(
                    "UPDATE leads SET attrs=?, pii_enc=?, external_ref=? WHERE id=?",
                    (json.dumps(attrs, sort_keys=True), crypto.encrypt(pii), attrs[ref_name], lead_id))
        conn.executemany("INSERT INTO rejections (file_id, workflow_id, batch_id, row_number, reason)"
                         " VALUES (?,?,?,?,?)", rejections)
        conn.execute("UPDATE ingested_files SET stats=? WHERE id=?", (json.dumps(stats), file_id))
        conn.execute("COMMIT")
        return stats
    except BaseException:
        conn.execute("ROLLBACK")
        raise


# ------------------------------------------------------------------- events
def parse_event_line(line: str, crypto: Crypto):
    """-> dict ready for INSERT, or None when the line is malformed."""
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    event_id = obj.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        return None
    ts = parse_timestamp(obj.get("occurred_at"))
    if ts is None:                       # decision: an event we cannot place in time is malformed
        return None
    etype = obj.get("event_type") if isinstance(obj.get("event_type"), str) else ""
    payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
    phone = obj.get("phone")
    canonical = normalize_phone(str(phone)) if phone not in (None, "") else None
    code = None
    if etype.startswith("disposition."):
        c = payload.get("disposition_code")
        code = c if isinstance(c, str) else None
    wf, call = obj.get("workflow_id"), obj.get("call_id")
    return {
        "event_id": event_id.strip(), "event_type": etype,
        "workflow_id": wf if isinstance(wf, str) else None,
        "call_id": call if isinstance(call, str) else None,
        "occurred_at": ts.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        "occurred_us": to_epoch_us(ts),
        "phone_hash": crypto.phone_hash(canonical) if canonical else None,   # never the number
        "disposition_code": code,
        "payload": json.dumps(payload, ensure_ascii=False, sort_keys=True),
    }


def ingest_event_file(conn: sqlite3.Connection, crypto: Crypto, path: Path, drop: str) -> Optional[dict]:
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    conn.execute("BEGIN IMMEDIATE")
    try:
        file_id = _register_file(conn, "events", "", drop, path.name, None, digest)
        if file_id is None:
            conn.execute("ROLLBACK")
            return None
        stats = {"events_total": 0, "malformed": 0, "duplicates_dropped": 0}
        sql = ("INSERT INTO events (event_id, event_type, workflow_id, call_id, occurred_at, occurred_us,"
               " phone_hash, disposition_code, payload, file_id) VALUES"
               " (:event_id,:event_type,:workflow_id,:call_id,:occurred_at,:occurred_us,"
               ":phone_hash,:disposition_code,:payload,:file_id) ON CONFLICT(event_id) DO NOTHING")
        for line in data.decode("utf-8", errors="replace").split("\n"):
            line = line.strip()
            if not line:
                continue
            stats["events_total"] += 1
            ev = parse_event_line(line, crypto)
            if ev is None:
                stats["malformed"] += 1
                continue
            ev["file_id"] = file_id
            if conn.execute(sql, ev).rowcount == 0:      # event_id already seen, in any drop
                stats["duplicates_dropped"] += 1
        conn.execute("UPDATE ingested_files SET stats=? WHERE id=?", (json.dumps(stats), file_id))
        conn.execute("COMMIT")
        return stats
    except BaseException:
        conn.execute("ROLLBACK")
        raise


# -------------------------------------------------------------------- drops
def ingest_drop(conn, crypto, catalog: Catalog, drop_dir: Path,
                log: Callable[[str], None] = print) -> None:
    """Ingest one drop directory: leads (workflow dirs A-Z, files by name) then events."""
    drop = drop_dir.name
    for wf_dir in sorted(p for p in (drop_dir / "leads").glob("*") if p.is_dir()):
        wf = catalog.workflows.get(wf_dir.name)
        if wf is None:
            log(f"  ! {drop}/leads/{wf_dir.name}: no schema for this workflow, skipped")
            continue
        for f in sorted(wf_dir.glob("*.csv"), key=_natural):
            s = ingest_lead_file(conn, crypto, wf, f, drop)
            log(f"  {drop}/leads/{wf.workflow_id}/{f.name}: " + ("already ingested, skipped" if s is None else
                f"{s['rows_total']} rows, {s['rows_rejected']} rejected, {s['duplicates_merged']} merged"))
    for f in sorted((drop_dir / "events").glob("*.jsonl"), key=_natural):
        s = ingest_event_file(conn, crypto, f, drop)
        log(f"  {drop}/events/{f.name}: " + ("already ingested, skipped" if s is None else
            f"{s['events_total']} lines, {s['malformed']} malformed, {s['duplicates_dropped']} duplicates"))


def ingest_path(conn, crypto, catalog: Catalog, path: Path, log: Callable[[str], None] = print) -> None:
    """`path` is a drop (has leads/ or events/) or a directory of drops (drop_1, drop_2, ...)."""
    path = Path(path)
    if (path / "leads").is_dir() or (path / "events").is_dir():
        log(f"ingest {path}")
        ingest_drop(conn, crypto, catalog, path, log)
        return
    drops = sorted((p for p in path.glob("*") if p.is_dir()), key=_natural)
    if not drops:
        raise SystemExit(f"{path}: not a drop (no leads/ or events/ folder) and has no sub-folders")
    for d in drops:
        log(f"ingest {d}")
        ingest_drop(conn, crypto, catalog, d, log)
