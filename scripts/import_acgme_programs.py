#!/usr/bin/env python3
"""Normalize an ACGME Program Search export into the canonical workforce CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from servers.workforce_analytics import workforce_data  # noqa: E402


def _read_tabular_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)  # type: ignore[no-any-return]
    if suffix == ".tsv":
        return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize an ACGME Program Search export from "
            "https://acgmecloud.org/analytics/explore-public-data/program-search "
            "into the canonical workforce-analytics CSV."
        )
    )
    parser.add_argument("input_file", help="Path to the exported CSV, TSV, or XLSX file.")
    parser.add_argument(
        "--output",
        default=str(workforce_data._ACGME_CACHE_CSV),
        help="Destination CSV path. Defaults to the shared workforce cache path.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_file).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    if not input_path.exists():
        parser.error(f"Input file not found: {input_path}")

    df = _read_tabular_file(input_path)
    normalized = workforce_data.normalize_acgme_dataframe(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(output_path, index=False)

    print(f"Imported {len(normalized)} ACGME program rows to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
