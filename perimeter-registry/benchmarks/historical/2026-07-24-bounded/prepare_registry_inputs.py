from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from perimeter_registry import Registry


def main() -> None:
    root = Path(__file__).parent
    questions = json.loads((root / "questions.json").read_text())["questions"]
    registries: dict[str, Registry] = {}
    outputs: dict[str, object] = {}
    for item in questions:
        fixture = item["fixture"]
        registry = registries.setdefault(fixture, Registry.from_fixture(fixture))
        outputs[item["id"]] = asdict(registry.resolve(item["question"]))
    payload = {
        "version": 1,
        "generated_from": "questions.json plus deterministic local fixtures",
        "outputs": outputs,
    }
    (root / "registry_resolver_outputs.json").write_text(json.dumps(payload, indent=2) + "\n")


if __name__ == "__main__":
    main()
