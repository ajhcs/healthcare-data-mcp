"""Isolated, event-instrumented runner for matched HSPR benchmark arms."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import tempfile
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
        questions = list(question_ids)
        arms = list(arm_ids)
        rng.shuffle(questions)
        if repetition % 2 == 0:
            questions.reverse()
            arms.reverse()
        for q_index, question_id in enumerate(questions):
            rotated = arms[q_index % len(arms) :] + arms[: q_index % len(arms)]
            for order, arm_id in enumerate(rotated):
                schedule.append(
                    {"question_id": question_id, "arm_id": arm_id, "repetition": repetition, "order": order}
                )
    return schedule


def _observable_events(stdout: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    first_financial = False
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
        event = monotonic_event(kind, runtime_name=name, payload=item)
        events.append(event)
        if not first_financial and any(
            token in lower for token in ("10-k", "audited", "financial statement", "form 990")
        ):
            events.append(monotonic_event("first_authoritative_financial_evidence"))
            first_financial = True
    if first_financial:
        events.append(monotonic_event("last_authoritative_financial_evidence"))
    return events


def run_one(
    *,
    command: list[str],
    arm: dict[str, Any],
    question: dict[str, Any],
    registry_path: Path | None,
    response_schema: Path,
    output_path: Path,
) -> None:
    if ".benchmark-sealed" in str(output_path.resolve()):
        raise ValueError("answer traces may not be written under sealed root")
    events = [monotonic_event("run_start", arm_id=arm["arm_id"], question_id=question["question_id"])]
    with tempfile.TemporaryDirectory(prefix="hspr-answer-") as temp_name:
        work = Path(temp_name)
        (work / "question.json").write_text(json.dumps(question, indent=2), encoding="utf-8")
        shutil.copy2(response_schema, work / "response-schema.json")
        if arm["hspr_available"]:
            if registry_path is None:
                raise ValueError("HSPR arm requires audited registry packet")
            events.append(monotonic_event("hspr_lookup_start"))
            shutil.copy2(registry_path, work / "hspr-identity.json")
            events.append(monotonic_event("hspr_lookup_end"))
        prompt = (
            "Answer question.json using live authoritative financial sources. The response must validate against "
            "response-schema.json. Retrieve the financial result live even when hspr-identity.json exists; that file "
            "contains identity/perimeter context only. Do not infer unavailable telemetry."
        )
        env = {"PATH": os.environ.get("PATH", "")}
        started = time.monotonic_ns()
        completed = subprocess.run(command + [prompt], cwd=work, env=env, text=True, capture_output=True, check=False)
        events.extend(_observable_events(completed.stdout))
        events.append(monotonic_event("final_answer"))
        events.append(
            monotonic_event("run_end", exit_code=completed.returncode, duration_ns=time.monotonic_ns() - started)
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(
                {
                    "arm": arm,
                    "question_id": question["question_id"],
                    "events": events,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                    "telemetry_policy": "tokens and cost are null unless explicitly emitted by runtime",
                },
                indent=2,
            ),
            encoding="utf-8",
        )


def preflight(public_root: Path, registry_path: Path, sealed_gold: Path) -> dict[str, Any]:
    if not sealed_gold.is_file():
        raise ValueError("adjudicated sealed gold is missing")
    audit = audit_registry_packet(registry_path, sealed_gold)
    if not audit["passed"]:
        raise ValueError(f"registry leakage audit failed: {audit['findings']}")
    if (public_root / ".benchmark-sealed").exists():
        raise ValueError("answer-visible root contains sealed directory")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--arms", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--sealed-gold", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--agent-command", nargs="+", required=True)
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
    manifest = {"schedule": schedule, "execute_requested": args.execute}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "schedule.json").write_text(json.dumps(manifest, indent=2))
    if not args.execute:
        return
    if os.environ.get("HSPR_BENCHMARK_EXECUTION_APPROVED") != "cleanup-ci-green-and-pilot-adjudicated":
        raise SystemExit("execution gate is closed")
    preflight(Path.cwd(), args.registry, args.sealed_gold)
    raise SystemExit("schedule frozen; execute individual isolated trials through an approved orchestration context")


if __name__ == "__main__":
    main()
