"""Audit public Git history for material bearing on a protected active cohort."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ANSWER_BEARING_MARKERS = (
    "gold",
    "score",
    "result",
    "evaluated",
    "resolver_output",
    "research",
    "runs/",
    "historical/",
    "pilot_questions",
    "pilot.identity",
)
IDENTITY_DATA_ROOTS = ("src/perimeter_registry/data/", "perimeter-registry/src/perimeter_registry/data/")
SAMPLING_FRAME_PATHS = {"qa/reports/health_system_metrics_reconciliation.csv"}


def _git(repo: Path, *arguments: str, text: bool = True) -> str | bytes:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=text,
    )
    return completed.stdout


def _classification(path: str) -> str:
    lower = path.casefold()
    if path in SAMPLING_FRAME_PATHS:
        return "declared_sampling_frame"
    if lower.startswith(IDENTITY_DATA_ROOTS):
        return "answer_bearing"
    if "benchmark" in lower and any(marker in lower for marker in ANSWER_BEARING_MARKERS):
        return "answer_bearing"
    return "other_review_required"


def audit_public_history(
    repo: Path,
    questions_path: Path,
    public_refs: list[str],
    identity_path: Path | None = None,
) -> dict[str, Any]:
    document = json.loads(questions_path.read_text(encoding="utf-8"))
    systems = {str(question["system_id"]): str(question["system"]) for question in document["questions"]}
    terms = {system_id: {system_id, system_name} for system_id, system_name in systems.items()}
    if identity_path is not None:
        identity = json.loads(identity_path.read_text(encoding="utf-8"))
        for system in identity.get("systems", []):
            system_id = str(system.get("system_id", ""))
            if system_id not in terms:
                raise ValueError("identity packet contains a system outside the active questions")
            candidates = [system.get("canonical_name"), *system.get("aliases", [])]
            candidates.extend(entity.get("name") for entity in system.get("legal_entities", []))
            candidates.extend(item.get("identifier") for item in system.get("identifiers", []))
            terms[system_id].update(str(value) for value in candidates if value)
    commits: set[str] = set()
    ref_shas: dict[str, str] = {}
    for ref in public_refs:
        ref_shas[ref] = str(_git(repo, "rev-parse", "--verify", ref)).strip()
        commits.update(str(_git(repo, "rev-list", ref)).splitlines())
    blobs: dict[str, set[str]] = {}
    for commit in sorted(commits):
        listing = str(_git(repo, "ls-tree", "-r", commit))
        for line in listing.splitlines():
            metadata, path = line.split("\t", 1)
            object_id = metadata.split()[2]
            blobs.setdefault(object_id, set()).add(path)
    hits: list[dict[str, Any]] = []
    for object_id, paths in blobs.items():
        content = bytes(_git(repo, "cat-file", "blob", object_id, text=False)).decode("utf-8", errors="replace")
        folded = content.casefold()
        for system_id, system_name in systems.items():
            matched_terms = sorted(term for term in terms[system_id] if term.casefold() in folded)
            if not matched_terms:
                continue
            for path in sorted(paths):
                hits.append(
                    {
                        "system_id": system_id,
                        "system": system_name,
                        "path": path,
                        "blob": object_id,
                        "matched_terms": matched_terms,
                        "classification": _classification(path),
                    }
                )
    blocking = [hit for hit in hits if hit["classification"] != "declared_sampling_frame"]
    return {
        "schema_version": 1,
        "audited_at_utc": datetime.now(UTC).isoformat(),
        "public_refs": public_refs,
        "public_ref_shas": ref_shas,
        "reachable_commit_count": len(commits),
        "unique_blob_count": len(blobs),
        "active_system_count": len(systems),
        "audited_identity_packet": identity_path is not None,
        "term_count": sum(len(values) for values in terms.values()),
        "hits": hits,
        "blocking_hits": blocking,
        "passed": not blocking,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--public-ref", action="append", required=True)
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit_public_history(args.repo, args.questions, args.public_ref, args.identity)
    descriptor = args.output.open("x", encoding="utf-8")
    try:
        json.dump(result, descriptor, indent=2)
        descriptor.write("\n")
    finally:
        descriptor.close()
    args.output.chmod(0o600)
    print(json.dumps({"passed": result["passed"], "blocking_hit_count": len(result["blocking_hits"])}))


if __name__ == "__main__":
    main()
