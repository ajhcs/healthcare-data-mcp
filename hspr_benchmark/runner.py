"""Schedule and execute isolated, matched HSPR benchmark answer trials."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import stat
import subprocess
import time
from pathlib import Path
from typing import Any

from .container_launcher import PINNED_NODE_IMAGE, REPOSITORY_ROOT, read_untrusted_regular, write_new_text
from .leakage import audit_registry_packet
from .trial_executor import run_trial


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


def _locked(path: Path, relative: str) -> Path:
    expected = (REPOSITORY_ROOT / relative).resolve(strict=True)
    if path.absolute() != expected or path.resolve(strict=True) != expected or path.is_symlink():
        raise ValueError(f"official execution requires locked input: {relative}")
    return expected


def _write_or_verify(path: Path, document: dict[str, Any]) -> None:
    rendered = json.dumps(document, indent=2) + "\n"
    if path.exists():
        existing = read_untrusted_regular(path, max_bytes=8 * 1024 * 1024).decode("utf-8")
        if existing != rendered:
            raise ValueError(f"existing run control file does not match: {path}")
        return
    write_new_text(path, rendered)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"digest input is not a regular file: {path}")
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
    finally:
        os.close(descriptor)
    return digest.hexdigest()


def _sha256_tree(path: Path) -> str:
    digest = hashlib.sha256()
    files = sorted(item for item in path.rglob("*") if item.is_file())
    if not files or any(item.is_symlink() for item in files):
        raise ValueError(f"runtime tree must contain regular non-symlink files: {path}")
    for item in files:
        relative = item.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(bytes.fromhex(_sha256_file(item)))
    return digest.hexdigest()


def _repository_revision() -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    if status.stdout.strip():
        raise ValueError("official execution requires a clean tracked worktree")
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _validate_sealed_gold(sealed_gold: Path) -> dict[str, Any]:
    manifest_path = _locked(
        REPOSITORY_ROOT / "hspr-benchmark-v2/config/sealed-manifest.json",
        "hspr-benchmark-v2/config/sealed-manifest.json",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sealed_root = REPOSITORY_ROOT / str(manifest["sealed_root"])
    declared_gold = sealed_root / "adjudication/gold.json"
    if sealed_root.absolute() != sealed_root.resolve(strict=True) or declared_gold.is_symlink():
        raise ValueError("sealed root and adjudicated gold may not traverse symlinks")
    expected = declared_gold.resolve(strict=True)
    if sealed_gold.resolve(strict=True) != expected:
        raise ValueError("official execution requires the manifest-bound adjudicated gold")
    digest = _sha256_file(expected)
    if not digest.startswith(str(manifest["adjudicated_gold_sha256_prefix7"])):
        raise ValueError("adjudicated gold digest does not match the sealed manifest")
    document = json.loads(expected.read_text(encoding="utf-8"))
    if len(document.get("records", [])) != int(manifest["adjudicated_records"]):
        raise ValueError("adjudicated gold record count does not match the sealed manifest")
    return {
        "sealed_manifest_sha256": _sha256_file(manifest_path),
        "adjudicated_gold_sha256": digest,
        "adjudicated_records": int(manifest["adjudicated_records"]),
    }


def _build_input_manifest(
    *,
    questions_path: Path,
    arms_path: Path,
    registry_path: Path,
    response_schema: Path,
    runtime_source: Path,
    codex_package_dir: Path,
    sealed_attestation: dict[str, Any],
) -> dict[str, Any]:
    codex_package = json.loads((codex_package_dir / "package.json").read_text(encoding="utf-8"))
    return {
        "schema_version": 1,
        "git_revision": _repository_revision(),
        "node_image": PINNED_NODE_IMAGE,
        "codex_package_name": codex_package.get("name"),
        "codex_package_version": codex_package.get("version"),
        "codex_package_tree_sha256": _sha256_tree(codex_package_dir),
        "questions_sha256": _sha256_file(questions_path),
        "arms_sha256": _sha256_file(arms_path),
        "registry_sha256": _sha256_file(registry_path),
        "response_schema_sha256": _sha256_file(response_schema),
        "runtime_tree_sha256": _sha256_tree(runtime_source),
        **sealed_attestation,
    }


def _verify_completed_trial(
    trial_dir: Path,
    *,
    expected: dict[str, Any],
    input_manifest_sha256: str,
) -> dict[str, Any]:
    metadata_path = trial_dir / "trial-metadata.json"
    receipt_path = trial_dir / "completion-receipt.json"
    artifact_names = ("native-trace.json", "answer.json", "observable-summary.json", "trial-metadata.json")
    metadata = json.loads(read_untrusted_regular(metadata_path).decode("utf-8"))
    receipt = json.loads(read_untrusted_regular(receipt_path).decode("utf-8"))
    expected_identity = {**expected, "input_manifest_sha256": input_manifest_sha256}
    if any(metadata.get(key) != value for key, value in expected_identity.items()):
        raise ValueError(f"completed trial metadata identity mismatch: {trial_dir}")
    if receipt.get("identity") != expected_identity:
        raise ValueError(f"completed trial receipt identity mismatch: {trial_dir}")
    artifact_hashes = receipt.get("artifact_sha256")
    if not isinstance(artifact_hashes, dict) or set(artifact_hashes) != set(artifact_names):
        raise ValueError(f"completed trial receipt has an invalid artifact set: {trial_dir}")
    for name in artifact_names:
        path = trial_dir / name
        if _sha256_file(path) != artifact_hashes[name]:
            raise ValueError(f"completed trial artifact digest mismatch: {path}")
    return metadata


def execute_batch(
    *,
    questions_path: Path,
    arms_path: Path,
    registry_path: Path,
    sealed_gold: Path,
    response_schema: Path,
    runtime_source: Path,
    codex_package_dir: Path,
    credential_path: Path,
    output_root: Path,
    start: int,
    trial_count: int,
    attempt: int = 1,
) -> dict[str, Any]:
    """Run one deliberate schedule slice; completed trial directories are immutable."""
    if start < 1 or trial_count < 1 or attempt < 1:
        raise ValueError("start, trial_count, and attempt must be positive")
    questions_path = _locked(questions_path, "hspr-benchmark-v2/public/pilot_questions.json")
    arms_path = _locked(arms_path, "hspr-benchmark-v2/config/arms.json")
    registry_path = _locked(registry_path, "hspr-benchmark-v2/registry/pilot.identity.json")
    response_schema = _locked(response_schema, "hspr-benchmark-v2/config/response-schema.json")
    runtime_source = _locked(runtime_source, "hspr-benchmark-v2/runtime")
    sealed_attestation = _validate_sealed_gold(sealed_gold)
    audit = audit_registry_packet(registry_path, sealed_gold)
    if not audit["passed"]:
        raise ValueError(f"registry leakage audit failed: {audit['findings']}")
    questions = json.loads(questions_path.read_text(encoding="utf-8"))["questions"]
    arms_doc = json.loads(arms_path.read_text(encoding="utf-8"))
    arms = {str(arm["arm_id"]): arm for arm in arms_doc["arms"]}
    question_by_id = {str(question["question_id"]): question for question in questions}
    if len(question_by_id) != len(questions):
        raise ValueError("pilot question IDs must be unique")
    if not all(re.fullmatch(r"[a-z0-9-]+", question_id) for question_id in question_by_id):
        raise ValueError("pilot question IDs must be safe lowercase slugs")
    schedule = counterbalanced_schedule(
        list(question_by_id), list(arms), int(arms_doc["target_repetitions"]), "hspr-v2-run-order"
    )
    selected = schedule[start - 1 : start - 1 + trial_count]
    if len(selected) != trial_count:
        raise ValueError("requested batch extends beyond the frozen schedule")
    if ".benchmark-sealed" in output_root.absolute().parts:
        raise ValueError("answer results may not be written under the sealed root")
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if output_root.absolute() != output_root.resolve(strict=True) or output_root.is_symlink():
        raise ValueError("answer result root may not traverse symlinks")
    output_root.chmod(0o700)
    input_manifest = _build_input_manifest(
        questions_path=questions_path,
        arms_path=arms_path,
        registry_path=registry_path,
        response_schema=response_schema,
        runtime_source=runtime_source,
        codex_package_dir=codex_package_dir,
        sealed_attestation=sealed_attestation,
    )
    _write_or_verify(output_root / "input-manifest.json", input_manifest)
    _write_or_verify(
        output_root / "schedule.json",
        {
            "schema_version": 1,
            "seed": "hspr-v2-run-order",
            "questions": [str(question["question_id"]) for question in questions],
            "arms": list(arms),
            "target_repetitions": int(arms_doc["target_repetitions"]),
            "schedule": schedule,
        },
    )
    completed: list[dict[str, Any]] = []
    input_manifest_sha256 = _sha256_file(output_root / "input-manifest.json")
    for sequence, row in enumerate(selected, start=start):
        trial_base = f"trial-{sequence:03d}-{row['question_id']}-{row['arm_id']}-r{row['repetition']}"
        trial_id = f"{trial_base}-a{attempt}"
        trial_dir = output_root / trial_id
        staging_dir = output_root / f"in-progress-{trial_id}"
        identity = {
            "schema_version": 1,
            "trial_id": trial_id,
            "sequence": sequence,
            "question_id": row["question_id"],
            "system_id": question_by_id[str(row["question_id"])]["system_id"],
            "arm_id": row["arm_id"],
            "repetition": row["repetition"],
            "attempt": attempt,
            "schedule_order": row["order"],
        }
        completed_attempts = sorted(output_root.glob(f"{trial_base}-a*"))
        if trial_dir.exists():
            metadata = _verify_completed_trial(
                trial_dir,
                expected=identity,
                input_manifest_sha256=input_manifest_sha256,
            )
            completed.append(metadata)
            continue
        if completed_attempts:
            raise ValueError(f"schedule position already has a completed attempt: {completed_attempts[0]}")
        if staging_dir.exists():
            raise FileExistsError(
                f"partial trial is preserved at {staging_dir}; retry this position with a higher --attempt"
            )
        if attempt > 1:
            prior_staging = output_root / f"in-progress-{trial_base}-a{attempt - 1}"
            if not prior_staging.is_dir() or prior_staging.is_symlink():
                raise ValueError("a higher attempt requires the immediately prior partial attempt")
        credential = bytearray(read_untrusted_regular(credential_path, max_bytes=1024 * 1024))
        try:
            result = run_trial(
                question=question_by_id[str(row["question_id"])],
                arm=arms[str(row["arm_id"])],
                registry_packet=registry_path,
                sealed_gold=sealed_gold,
                response_schema=response_schema,
                runtime_source=runtime_source,
                codex_package_dir=codex_package_dir,
                output_dir=staging_dir,
                credential_json=bytes(credential),
                allow_live_credential=True,
            )
        finally:
            for index in range(len(credential)):
                credential[index] = 0
        metadata = {
            **identity,
            "duration_ns": result["trace"]["duration_ns"],
            "input_manifest_sha256": input_manifest_sha256,
        }
        write_new_text(staging_dir / "trial-metadata.json", json.dumps(metadata, indent=2) + "\n")
        artifact_names = ("native-trace.json", "answer.json", "observable-summary.json", "trial-metadata.json")
        receipt = {
            "schema_version": 1,
            "identity": {**identity, "input_manifest_sha256": input_manifest_sha256},
            "artifact_sha256": {name: _sha256_file(staging_dir / name) for name in artifact_names},
        }
        write_new_text(staging_dir / "completion-receipt.json", json.dumps(receipt, indent=2) + "\n")
        os.rename(staging_dir, trial_dir)
        completed.append(metadata)
    return {"start": start, "trial_count": trial_count, "completed": completed}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--arms", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--sealed-gold", type=Path)
    parser.add_argument("--response-schema", type=Path)
    parser.add_argument("--runtime", type=Path)
    parser.add_argument("--codex-package", type=Path)
    parser.add_argument("--credential", type=Path)
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--trial-count", type=int, default=1)
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--allow-live-credential", action="store_true")
    args = parser.parse_args()
    questions = json.loads(args.questions.read_text())["questions"]
    arms_doc = json.loads(args.arms.read_text())
    schedule = counterbalanced_schedule(
        [q["question_id"] for q in questions],
        [a["arm_id"] for a in arms_doc["arms"]],
        arms_doc["target_repetitions"],
        "hspr-v2-run-order",
    )
    if not args.execute:
        args.output.mkdir(parents=True, exist_ok=True)
        _write_or_verify(args.output / "schedule.json", {"schedule": schedule, "execute_requested": False})
        return
    if not args.allow_live_credential:
        raise SystemExit("official execution requires --allow-live-credential")
    required = {
        "registry": args.registry,
        "sealed_gold": args.sealed_gold,
        "response_schema": args.response_schema,
        "runtime": args.runtime,
        "codex_package": args.codex_package,
        "credential": args.credential,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise SystemExit(f"official execution is missing required arguments: {', '.join(missing)}")
    result = execute_batch(
        questions_path=args.questions,
        arms_path=args.arms,
        registry_path=args.registry,
        sealed_gold=args.sealed_gold,
        response_schema=args.response_schema,
        runtime_source=args.runtime,
        codex_package_dir=args.codex_package,
        credential_path=args.credential,
        output_root=args.output,
        start=args.start,
        trial_count=args.trial_count,
        attempt=args.attempt,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
