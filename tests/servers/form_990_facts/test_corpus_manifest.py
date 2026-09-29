from __future__ import annotations

import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
CORPUS_MANIFEST = REPO_ROOT / "configs" / "irs990-prototype-filers.csv"

REQUIRED_EXACT_FILERS = {
    "741180155": "THE METHODIST HOSPITAL",
    "232829095": "THOMAS JEFFERSON UNIVERSITY HOSPITALS INC",
    "010238552": "MaineHealth",
    "362174823": "Rush University Medical Center",
}

OFFICIAL_2025_TAX_PERIOD_EINS = {
    "232829095",
    "362174823",
    "452106295",
    "470617373",
    "521622253",
}


def _rows() -> list[dict[str, str]]:
    with CORPUS_MANIFEST.open(newline="", encoding="utf-8-sig") as source:
        return list(csv.DictReader(source))


def test_curated_corpus_has_16_exact_filers_with_two_or_three_periods() -> None:
    rows = _rows()
    rows_by_ein: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        rows_by_ein.setdefault(row["ein"], []).append(row)

    assert len(rows_by_ein) == 16
    assert all(2 <= len(filings) <= 3 for filings in rows_by_ein.values())
    assert len({row["object_id"] for row in rows}) == len(rows)


def test_required_brand_examples_are_exact_legal_filers() -> None:
    rows = _rows()
    legal_names_by_ein = {
        ein: {row["legal_filer"].casefold() for row in rows if row["ein"] == ein} for ein in REQUIRED_EXACT_FILERS
    }

    assert legal_names_by_ein == {ein: {legal_name.casefold()} for ein, legal_name in REQUIRED_EXACT_FILERS.items()}


def test_tax_period_2025_rows_are_only_verified_2026_electronic_filings() -> None:
    rows = _rows()
    tax_period_2025 = [row for row in rows if row["tax_year"] == "2025"]

    assert {row["ein"] for row in tax_period_2025} == OFFICIAL_2025_TAX_PERIOD_EINS
    assert all(row["filing_year"] == "2026" for row in tax_period_2025)
    assert all(row["object_id"].startswith("2026") for row in tax_period_2025)
    assert all(row["xml_batch_id"].startswith("2026_TEOS_XML_") for row in tax_period_2025)
