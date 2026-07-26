"""Schedule and trace helpers for matched HSPR benchmark arms.

This module intentionally does not launch answer agents. An approved executor
must provide a real access-control boundary and runtime-native event timestamps.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import time
from pathlib import Path
from typing import Any

from .leakage import audit_registry_packet


def monotonic_event(kind: str, **details: Any) -> dict[str, Any]:
    return {"event": kind, "monotonic_ns": time.monotonic_ns(), **details}


def counterbalanced_schedule(
    question_ids: list[str], arm_ids: list[str], repetitions: int, seed: str
) -> list[dict[str, Any]]:
    rng = random.Random(int(hashlib.sha256(seed.encode()).hexdigest(), 16))
    schedule = []
    for repetition in range(1, repetitions + 1):
        questions, arms = list(question_ids), list(arm_ids)
        rng.shuffle(questions)
        if repetition % 2 == 0:
            questions.reverse()
            arms.reverse()
        for q_index, question_id in enumerate(questions):
            rotated = arms[q_index % len(arms) :] + arms[: q_index % len(arms)]
            schedule.extend(
                {"question_id": question_id, "arm_id": arm_id, "repetition": repetition, "order": order}
                for order, arm_id in enumerate(rotated)
            )
    return schedule


def observable_events(stdout: str) -> list[dict[str, Any]]:
    """Normalize runtime JSONL without inventing timestamps or source authority."""
    events = []
    for line in stdout.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = str(item.get("tool") or item.get("name") or item.get("type") or "runtime_event")
        lower = json.dumps(item, sort_keys=True).lower()
        if any(token in lower for token in ("search_query", '"open"', '"click"', "download", "web")):
            kind = "observable_web_tool"
        elif any(token in lower for token in ("read_file", "local_read", "shell_command")):
            kind = "observable_local_read"
        else:
            kind = "runtime_event"
        runtime_time = item.get("monotonic_ns") or item.get("timestamp") or item.get("created_at")
        events.append(
            {
                "event": kind,
                "runtime_name": name,
                "runtime_timestamp": runtime_time,
                "timing_available": runtime_time is not None,
                "payload": item,
            }
        )
    return events


def preflight(answer_visible_root: Path, registry_path: Path, sealed_gold: Path) -> dict[str, Any]:
    if not sealed_gold.is_file():
        raise ValueError("adjudicated sealed gold is missing")
    audit = audit_registry_packet(registry_path, sealed_gold)
    if not audit["passed"]:
        raise ValueError(f"registry leakage audit failed: {audit['findings']}")
    if (answer_visible_root / ".benchmark-sealed").exists():
        raise ValueError("answer-visible root contains sealed directory")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--arms", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    questions = json.loads(args.questions.read_text())["questions"]
    arms_doc = json.loads(args.arms.read_text())
    schedule = counterbalanced_schedule(
        [q["question_id"] for q in questions],
        [a["arm_id"] for a in arms_doc["arms"]],
        arms_doc["target_repetitions"],
        "hspr-v2-run-order",
    )
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "schedule.json").write_text(
        json.dumps({"schedule": schedule, "execute_requested": args.execute}, indent=2)
    )
    if args.execute:
        raise SystemExit(
            "execution unavailable: implement and approve an access-controlled answer-context launcher with runtime-native event timestamps"
        )


if __name__ == "__main__":
    main()
