"""Regenerate checked-in JSON Schemas for public transport contracts."""

from __future__ import annotations

import json
from pathlib import Path

from shared.contracts.public_evidence import PublicEvidenceBundle

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    target = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(PublicEvidenceBundle.model_json_schema(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
