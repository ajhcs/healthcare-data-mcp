from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from perimeter_registry import Registry  # noqa: E402


def main() -> None:
    run_root = Path(__file__).parent
    questions = json.loads((run_root / "questions.json").read_text())["questions"]
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source": "deterministic resolver over frozen Nebraska and Kaiser fixtures",
        "answers": [],
    }
    for item in questions:
        resolution = Registry.from_fixture(item["fixture"]).resolve(item["question"])
        payload["answers"].append(
            {
                "question_id": item["id"],
                "fixture": item["fixture"],
                "resolution": asdict(resolution),
            }
        )
    (run_root / "registry_resolver_outputs.json").write_text(
        json.dumps(payload, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
