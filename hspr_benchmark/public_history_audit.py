"""Audit public Git history for material bearing on a protected active cohort."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pypdf import PdfReader

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
    "questions.json",
    "orders.json",
)
IDENTITY_DATA_ROOTS = ("src/perimeter_registry/data/", "perimeter-registry/src/perimeter_registry/data/")
SAFE_DECLARED_BLOBS = {
    "qa/reports/health_system_metrics_reconciliation.csv": {
        "b594814db5106ccda63a29e9f20126ec1f740f308446834a46547cec45cd879e"
    },
    "hspr-benchmark-v2/public/cohort.json": {
        "c68098d2a9cd15cadff4730c8049e852d7802d051716ee6b50703f608c180d0a",
        "e738b65c7fc92b947021f142753471fb200fd29a226f722bed8c53bd5068af5f",
    },
    "hspr_benchmark/cohort.py": {
        "2b2756702eaace17cfccd4764628465eda8378245011bd9ac2955bfae3b00496",
        "a7d7c28c43b8444a74eaf217e0b7bf0d004e9dea35449936f007b12f67498429",
    },
}
BLOCKING_CLASSIFICATIONS = {"answer_bearing", "identity_hit_review_required"}
AUDIT_POLICY = "strict-identity-hit-v4"
OPAQUE_EXTENSIONS = {
    ".7z",
    ".bz2",
    ".gif",
    ".gz",
    ".jpeg",
    ".jpg",
    ".parquet",
    ".pyc",
    ".png",
    ".rar",
    ".tar",
    ".webp",
}
ZIP_EXTENSIONS = {".docx", ".xlsx", ".zip"}
ZIP_TEXT_EXTENSIONS = {".csv", ".html", ".json", ".md", ".rels", ".txt", ".xml"}


def implementation_sha256() -> str:
    return _sha256(Path(__file__).read_bytes())


def _git(repo: Path, *arguments: str, text: bool = True) -> str | bytes:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=repo,
        check=True,
        capture_output=True,
        text=text,
    )
    return completed.stdout


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _classification(path: str, object_sha256: str) -> str:
    lower = path.casefold()
    if object_sha256 in SAFE_DECLARED_BLOBS.get(path, set()):
        if path == "qa/reports/health_system_metrics_reconciliation.csv":
            return "declared_sampling_frame"
        return "declared_selection_metadata"
    if path in SAFE_DECLARED_BLOBS:
        return "identity_hit_review_required"
    if lower.startswith(IDENTITY_DATA_ROOTS):
        return "answer_bearing"
    if "benchmark" in lower and any(marker in lower for marker in ANSWER_BEARING_MARKERS):
        return "answer_bearing"
    return "identity_hit_review_required"


def _normalized(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _term_matches(term: str, folded: str, normalized: str, digits_only: str) -> bool:
    raw = term.casefold()
    if raw in folded:
        return True
    variant = _normalized(term)
    if len(variant) >= 4 and variant in normalized:
        return True
    digits = re.sub(r"\D", "", term)
    return not re.search(r"[a-z]", term, re.IGNORECASE) and len(digits) >= 6 and digits in digits_only


def _relationship_terms(relationship: object) -> set[str]:
    if not isinstance(relationship, dict):
        return set()
    keys = {"subject", "object", "source", "target", "source_name", "target_name"}
    return {str(value) for key, value in relationship.items() if key in keys and isinstance(value, str) and value}


def _pdf_text(content: bytes) -> tuple[str, str]:
    try:
        pages = PdfReader(io.BytesIO(content)).pages
        extracted: list[str] = []
        for page in pages:
            page_text = page.extract_text() or ""
            resources = page.get("/Resources") or {}
            xobjects = resources.get("/XObject") if hasattr(resources, "get") else None
            if not page_text.strip() or xobjects:
                return "", "pdf_page_not_fully_text_inspectable"
            extracted.append(page_text)
        return "\n".join(extracted), "pdf_requires_manual_payload_review"
    except Exception:  # pypdf exposes format-specific exception subclasses inconsistently
        return "", "pdf_text_extraction_failed"


def _binary_kind(content: bytes) -> str | None:
    signatures = (
        (b"%PDF-", "pdf"),
        (b"PK\x03\x04", "zip"),
        (b"\x89PNG\r\n\x1a\n", "opaque"),
        (b"\xff\xd8\xff", "opaque"),
        (b"GIF8", "opaque"),
        (b"II*\x00", "opaque"),
        (b"MM\x00*", "opaque"),
        (b"BM", "opaque"),
        (b"\x1f\x8b", "opaque"),
        (b"7z\xbc\xaf'\x1c", "opaque"),
        (b"PAR1", "opaque"),
        (b"SQLite format 3\x00", "opaque"),
        (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", "opaque"),
    )
    for signature, kind in signatures:
        if content.startswith(signature):
            return kind
    if b"\x00" in content:
        return "unknown"
    try:
        content.decode("utf-8")
    except UnicodeDecodeError:
        return "unknown"
    return None


def _searchable_blob(content: bytes, paths: set[str]) -> tuple[str, list[dict[str, str]]]:
    """Extract searchable text or return fail-closed unsupported-artifact records."""
    text = content.decode("utf-8", errors="replace")
    issues: list[dict[str, str]] = []
    suffixes = {Path(path).suffix.casefold() for path in paths}
    kind = _binary_kind(content)
    if content.startswith(b"version https://git-lfs.github.com/spec/v1"):
        issues.extend({"path": path, "reason": "git_lfs_object_not_present"} for path in sorted(paths))
    if kind == "pdf" or ".pdf" in suffixes:
        extracted, reason = _pdf_text(content)
        if extracted:
            text += "\n" + extracted
        issues.extend(
            {"path": path, "reason": reason}
            for path in sorted(paths)
            if kind == "pdf" or Path(path).suffix.casefold() == ".pdf"
        )
    if kind == "zip" or suffixes & ZIP_EXTENSIONS:
        extracted_members: list[str] = []
        unsupported_members: list[str] = []
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                total = 0
                for member in archive.infolist():
                    if member.is_dir():
                        continue
                    if Path(member.filename).suffix.casefold() not in ZIP_TEXT_EXTENSIONS:
                        unsupported_members.append(member.filename)
                        continue
                    total += member.file_size
                    if member.file_size > 16 * 1024 * 1024 or total > 64 * 1024 * 1024:
                        raise ValueError("archive text exceeds audit extraction limit")
                    member_bytes = archive.read(member)
                    if member_bytes.startswith((b"\xff\xfe", b"\xfe\xff")):
                        extracted_members.append(member_bytes.decode("utf-16"))
                    else:
                        if _binary_kind(member_bytes) is not None:
                            unsupported_members.append(member.filename)
                            continue
                        extracted_members.append(member_bytes.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError, zipfile.BadZipFile):
            extracted_members = []
            unsupported_members = ["<archive extraction failed>"]
        if extracted_members and not unsupported_members:
            text += "\n" + "\n".join(extracted_members)
        else:
            reason = "archive_contains_unsupported_members" if unsupported_members else "archive_text_extraction_failed"
            issues.extend(
                {"path": path, "reason": reason}
                for path in sorted(paths)
                if kind == "zip" or Path(path).suffix.casefold() in ZIP_EXTENSIONS
            )
    for path in sorted(paths):
        suffix = Path(path).suffix.casefold()
        if suffix in OPAQUE_EXTENSIONS or kind == "opaque":
            issues.append({"path": path, "reason": "unsupported_opaque_artifact"})
        elif kind == "unknown":
            issues.append({"path": path, "reason": "unsupported_binary_artifact"})
    return text, issues


def audit_public_history(
    repo: Path,
    questions_path: Path,
    public_refs: list[str],
    identity_path: Path | None = None,
) -> dict[str, Any]:
    questions_bytes = questions_path.read_bytes()
    document = json.loads(questions_bytes)
    systems = {str(question["system_id"]): str(question["system"]) for question in document["questions"]}
    terms = {system_id: {system_id, system_name} for system_id, system_name in systems.items()}
    identity_bytes: bytes | None = None
    if identity_path is not None:
        identity_bytes = identity_path.read_bytes()
        identity = json.loads(identity_bytes)
        for system in identity.get("systems", []):
            system_id = str(system.get("system_id", ""))
            if system_id not in terms:
                raise ValueError("identity packet contains a system outside the active questions")
            candidates = [system.get("canonical_name"), *system.get("aliases", [])]
            candidates.extend(entity.get("name") for entity in system.get("legal_entities", []))
            candidates.extend(item.get("identifier") for item in system.get("identifiers", []))
            for relationship in system.get("relationships", []):
                candidates.extend(_relationship_terms(relationship))
            terms[system_id].update(str(value) for value in candidates if value)
    commits: set[str] = set()
    ref_shas: dict[str, str] = {}
    for ref in public_refs:
        ref_shas[ref] = str(_git(repo, "rev-parse", "--verify", ref)).strip()
        commits.update(str(_git(repo, "rev-list", ref_shas[ref])).splitlines())
    blobs: dict[str, set[str]] = {}
    for commit in sorted(commits):
        listing = str(_git(repo, "ls-tree", "-r", commit))
        for line in listing.splitlines():
            metadata, path = line.split("\t", 1)
            object_id = metadata.split()[2]
            blobs.setdefault(object_id, set()).add(path)
    hits: list[dict[str, Any]] = []
    uninspectable_artifacts: list[dict[str, str]] = []
    for object_id, paths in blobs.items():
        content_bytes = bytes(_git(repo, "cat-file", "blob", object_id, text=False))
        content, issues = _searchable_blob(content_bytes, paths)
        uninspectable_artifacts.extend({"blob": object_id, **issue} for issue in issues)
        folded = content.casefold()
        normalized = _normalized(content)
        digits_only = re.sub(r"\D", "", content)
        object_sha256 = _sha256(content_bytes)
        for system_id, system_name in systems.items():
            for path in sorted(paths):
                path_folded = path.casefold()
                path_normalized = _normalized(path)
                path_digits_only = re.sub(r"\D", "", path)
                matched_terms = sorted(
                    term
                    for term in terms[system_id]
                    if _term_matches(term, folded, normalized, digits_only)
                    or _term_matches(term, path_folded, path_normalized, path_digits_only)
                )
                if not matched_terms:
                    continue
                hits.append(
                    {
                        "system_id": system_id,
                        "system": system_name,
                        "path": path,
                        "blob": object_id,
                        "matched_terms": matched_terms,
                        "classification": _classification(path, object_sha256),
                    }
                )
    metadata_objects: dict[str, str] = {}
    for commit in commits:
        metadata_objects[f"git-commit-message/{commit}"] = commit
    for ref, object_id in ref_shas.items():
        if str(_git(repo, "cat-file", "-t", object_id)).strip() == "tag":
            metadata_objects[f"git-annotated-tag/{ref}"] = object_id
        metadata_objects[f"git-ref-name/{ref}"] = object_id
    for pseudo_path, object_id in metadata_objects.items():
        if pseudo_path.startswith("git-ref-name/"):
            content = pseudo_path.removeprefix("git-ref-name/")
        else:
            raw_metadata = str(_git(repo, "cat-file", "-p", object_id))
            content = raw_metadata.split("\n\n", 1)[-1]
        folded = content.casefold()
        normalized = _normalized(content)
        digits_only = re.sub(r"\D", "", content)
        for system_id, system_name in systems.items():
            matched_terms = sorted(
                term for term in terms[system_id] if _term_matches(term, folded, normalized, digits_only)
            )
            if matched_terms:
                hits.append(
                    {
                        "system_id": system_id,
                        "system": system_name,
                        "path": pseudo_path,
                        "blob": object_id,
                        "matched_terms": matched_terms,
                        "classification": "identity_hit_review_required",
                    }
                )
    blocking = [hit for hit in hits if hit["classification"] in BLOCKING_CLASSIFICATIONS]
    return {
        "schema_version": 4,
        "policy": AUDIT_POLICY,
        "audit_implementation_sha256": implementation_sha256(),
        "questions_sha256": _sha256(questions_bytes),
        "identity_sha256": _sha256(identity_bytes) if identity_bytes is not None else None,
        "audited_at_utc": datetime.now(UTC).isoformat(),
        "public_refs": public_refs,
        "public_ref_shas": ref_shas,
        "reachable_commit_count": len(commits),
        "unique_blob_count": len(blobs),
        "metadata_object_count": len(metadata_objects),
        "active_system_count": len(systems),
        "audited_identity_packet": identity_path is not None,
        "term_count": sum(len(values) for values in terms.values()),
        "hits": hits,
        "blocking_hits": blocking,
        "uninspectable_artifacts": uninspectable_artifacts,
        "passed": not blocking and not uninspectable_artifacts,
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
