"""Build the preregistered 100-system cohort from the frozen AHRQ-derived frame."""

from __future__ import annotations

import csv
import hashlib
import json
import random
from pathlib import Path

STANDARD_IDS = (
    "HSI00000008",
    "HSI00000019",
    "HSI00000073",
    "HSI00000170",
    "HSI00000218",
    "HSI00000263",
    "HSI00001021",
    "HSI00000323",
    "HSI00000350",
    "HSI00000388",
    "HSI00000475",
    "HSI00000491",
    "HSI00000516",
    "HSI00000531",
    "HSI00000714",
    "HSI00000770",
    "HSI00000773",
    "HSI00000780",
    "HSI00000784",
    "HSI00000776",
    "HSI00000852",
    "HSI00000958",
    "HSI00000968",
    "HSI00000972",
    "HSI00000977",
    "HSI00001050",
    "HSI00001072",
    "HSI00001078",
    "HSI00001243",
    "HSI00000767",
    "HSI00000717",
    "HSI00000660",
    "HSI00001175",
    "HSI00001177",
    "HSI00001186",
)

TRICKY_IDS = (
    "HSI00000536",
    "HSI00001085",
    "HSI00000048",
    "HSI00001126",
    "HSI00000820",
    "HSI00000055",
    "HSI00001106",
    "HSI00001168",
    "HSI00001165",
    "HSI00000711",
    "HSI00001066",
    "HSI00001148",
    "HSI00000249",
    "HSI00001075",
    "HSI00000591",
)

PILOT_IDS = frozenset({"HSI00000813", "HSI00000631", "HSI00001427", "HSI00000492"})
SEED = "hspr-financial-v2-confirmatory-2026-07-26"


def load_frame(path: Path) -> list[dict[str, object]]:
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 639:
        raise ValueError(f"expected frozen 639-system frame, got {len(rows)}")
    result: list[dict[str, object]] = []
    for row in rows:
        result.append(
            {
                "system_id": row["system_id"],
                "system_name": row["system_name"],
                "acute_beds": int(row["expected_system_bed_count"] or 0),
            }
        )
    return result


def _quartile_buckets(rows: list[dict[str, object]]) -> list[list[dict[str, object]]]:
    ordered = sorted(rows, key=lambda row: (int(row["acute_beds"]), str(row["system_id"])))
    return [ordered[len(ordered) * i // 4 : len(ordered) * (i + 1) // 4] for i in range(4)]


def build_cohort(frame_path: Path) -> dict[str, object]:
    rows = load_frame(frame_path)
    by_id = {str(row["system_id"]): row for row in rows}
    declared = set(STANDARD_IDS) | set(TRICKY_IDS)
    missing = declared - by_id.keys()
    if missing:
        raise ValueError(f"declared systems missing from frame: {sorted(missing)}")
    if len(declared) != 50 or declared & PILOT_IDS:
        raise ValueError("declared cohort must be 50 unique systems disjoint from pilot")
    eligible = [row for row in rows if row["system_id"] not in declared | PILOT_IDS]
    rng = random.Random(int(hashlib.sha256(SEED.encode()).hexdigest(), 16))
    allocations = (12, 12, 13, 13)
    sampled: list[dict[str, object]] = []
    for quartile, (bucket, count) in enumerate(zip(_quartile_buckets(eligible), allocations), 1):
        chosen = rng.sample(bucket, count)
        sampled.extend({**row, "size_stratum": f"acute_bed_q{quartile}"} for row in chosen)

    def labeled(ids: tuple[str, ...], label: str) -> list[dict[str, object]]:
        return [{**by_id[item], "selection_group": label} for item in ids]

    random_rows = [{**row, "selection_group": "random"} for row in sampled]
    output_rows = labeled(STANDARD_IDS, "standard") + random_rows + labeled(TRICKY_IDS, "tricky")
    if len(output_rows) != 100 or len({row["system_id"] for row in output_rows}) != 100:
        raise AssertionError("cohort is not 100 unique systems")
    return {
        "frame": {
            "name": "AHRQ Compendium of U.S. Health Systems, 2023 (September 2025 revision)",
            "source_url": "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
            "local_derivative": str(frame_path),
            "row_count": len(rows),
            "sha256": hashlib.sha256(frame_path.read_bytes()).hexdigest(),
            "access_date": "2026-07-26",
        },
        "seed": SEED,
        "method": "exclude pilot and declared selections; rank eligible systems by acute beds (missing reported bed counts conservatively encoded as 0); split into quartiles; sample 12/12/13/13 without replacement",
        "systems": output_rows,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    cohort = build_cohort(root / "qa/reports/health_system_metrics_reconciliation.csv")
    print(json.dumps(cohort, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
