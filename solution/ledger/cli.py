"""Command line: `python -m ledger ingest <drop>` and `python -m ledger report --out <dir>`."""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from .catalog import load_catalog
from .db import connect
from .ingest import ingest_path
from .pii import load_crypto
from .report import write_reports

DEFAULT_SCHEMAS = Path(__file__).resolve().parents[2] / "assignment-a-data" / "schemas"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ledger", description="Interaction ledger & reporting pipeline")
    ap.add_argument("--db", default="ledger.db", help="SQLite database file (default: ledger.db)")
    ap.add_argument("--schemas", default=str(DEFAULT_SCHEMAS), help="folder with workflow schemas + dispositions.csv")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_in = sub.add_parser("ingest", help="ingest a drop (or a folder of drops); safe to re-run")
    p_in.add_argument("path")
    p_rep = sub.add_parser("report", help="write the five report files")
    p_rep.add_argument("--out", required=True)
    args = ap.parse_args(argv)

    catalog = load_catalog(Path(args.schemas))
    crypto = load_crypto(args.db)
    conn = connect(args.db, crypto)
    t0 = time.perf_counter()
    if args.cmd == "ingest":
        ingest_path(conn, crypto, catalog, Path(args.path))
    else:
        write_reports(conn, catalog, Path(args.out))
        print(f"reports written to {args.out}")
    print(f"done in {time.perf_counter() - t0:.2f}s")
    return 0
