#!/usr/bin/env python3
"""Operator-only ingestion of exact official IRS Form 990 XML filings."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from servers.form_990_facts.fact_store import Form990FactStore  # noqa: E402
from servers.form_990_facts.operator_ingest import (  # noqa: E402
    IngestRequest,
    default_cache_dir,
    default_database_path,
    ingest_official_requests,
)


def _manifest_requests(path: Path) -> list[IngestRequest]:
    with path.open(newline="", encoding="utf-8-sig") as source:
        rows = list(csv.DictReader(source))
    return [
        IngestRequest(
            ein=row["ein"],
            tax_year=int(row["tax_year"]),
            filing_year=int(row["filing_year"]),
            object_id=(row.get("object_id") or "").strip() or None,
        )
        for row in rows
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Operator-only cold ingestion of exact Form 990 filings from official IRS XML batches."
    )
    request = parser.add_mutually_exclusive_group(required=True)
    request.add_argument("--manifest", type=Path, help="CSV with ein,tax_year,filing_year,object_id.")
    request.add_argument("--ein", help="Exact 9-digit legal-filer EIN.")
    parser.add_argument("--tax-year", type=int, help="Required with --ein.")
    parser.add_argument("--filing-year", type=int, help="Required with --ein; IRS annual index year.")
    parser.add_argument("--object-id", help="IRS OBJECT_ID; required when the exact scope has amendments.")
    parser.add_argument("--database", type=Path, default=default_database_path())
    parser.add_argument("--cache-dir", type=Path, default=default_cache_dir())
    parser.add_argument("--keep-archives", action="store_true", help="Retain large IRS batch ZIPs after ingestion.")
    args = parser.parse_args()

    if args.manifest:
        requests = _manifest_requests(args.manifest)
    else:
        if args.tax_year is None or args.filing_year is None:
            parser.error("--ein requires --tax-year and --filing-year")
        requests = [
            IngestRequest(
                ein=args.ein,
                tax_year=args.tax_year,
                filing_year=args.filing_year,
                object_id=args.object_id,
            )
        ]

    receipts = ingest_official_requests(
        Form990FactStore(args.database),
        requests,
        cache_dir=args.cache_dir,
        keep_archives=args.keep_archives,
    )
    print(json.dumps({"database": str(args.database), "receipts": receipts}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
