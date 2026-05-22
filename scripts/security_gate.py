"""Repository security-readiness checks that do not require network access."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_BASELINE_PREFIXES = ("tests/",)
ALLOWED_BASELINE_TYPES = {"Secret Keyword", "Hex High Entropy String"}
REQUIRED_FILTER_SUBSTRINGS = (
    "^\\.git/",
    "^\\.venv/",
    "^build/",
    "^dist/",
    "^\\.pytest_cache/",
    "^\\.ruff_cache/",
    "^\\.secrets\\.baseline$",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run local security gate checks.")
    parser.add_argument(
        "--baseline",
        default=".secrets.baseline",
        help="detect-secrets baseline to validate; defaults to .secrets.baseline",
    )
    parser.add_argument("--json", action="store_true", help="Emit machine-readable check results.")
    args = parser.parse_args(argv)

    result = validate_detect_secrets_baseline(REPO_ROOT / args.baseline)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    elif result["status"] == "ok":
        print(
            "Secret baseline policy passed: "
            f"{result['finding_count']} suppressed test-fixture finding(s), "
            f"{result['scanned_file_count']} file(s)."
        )
    else:
        print("Secret baseline policy failed:", file=sys.stderr)
        for error in result["errors"]:
            print(f"- {error}", file=sys.stderr)
    return 0 if result["status"] == "ok" else 1


def validate_detect_secrets_baseline(path: Path) -> dict[str, Any]:
    errors: list[str] = []
    if not path.exists():
        return {
            "status": "error",
            "baseline": _display_path(path),
            "errors": [f"Baseline file not found: {_display_path(path)}"],
            "finding_count": 0,
            "scanned_file_count": 0,
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {
            "status": "error",
            "baseline": _display_path(path),
            "errors": [f"Baseline is not valid JSON: {exc}"],
            "finding_count": 0,
            "scanned_file_count": 0,
        }

    results = payload.get("results")
    if not isinstance(results, dict):
        errors.append("Baseline results must be an object keyed by repository-relative file path.")
        results = {}

    finding_count = 0
    for filename, findings in sorted(results.items()):
        finding_count += len(findings) if isinstance(findings, list) else 0
        if Path(filename).is_absolute() or ".." in Path(filename).parts:
            errors.append(f"Baseline path must be repository-relative and non-traversing: {filename}")
        if not filename.startswith(ALLOWED_BASELINE_PREFIXES):
            errors.append(
                f"Baseline may only suppress test fixture findings, but includes {filename!r}. "
                "Do not baseline findings in source, docs, configs, installers, or CI."
            )
        if not (REPO_ROOT / filename).exists():
            errors.append(f"Baseline references a missing file: {filename}")
        if not isinstance(findings, list):
            errors.append(f"Baseline findings for {filename} must be a list.")
            continue
        for index, finding in enumerate(findings):
            if not isinstance(finding, dict):
                errors.append(f"Baseline finding {filename}[{index}] must be an object.")
                continue
            finding_type = str(finding.get("type") or "")
            if finding_type not in ALLOWED_BASELINE_TYPES:
                errors.append(f"Unexpected baseline finding type in {filename}: {finding_type!r}")
            if finding.get("is_verified") is True:
                errors.append(f"Verified secret findings must not be baselined: {filename}[{index}]")
            if finding.get("filename") != filename:
                errors.append(f"Baseline finding filename mismatch: key={filename!r}, value={finding.get('filename')!r}")
            if not str(finding.get("hashed_secret") or "").strip():
                errors.append(f"Baseline finding is missing hashed_secret: {filename}[{index}]")

    filter_patterns = [
        pattern
        for entry in payload.get("filters_used", [])
        if isinstance(entry, dict)
        for pattern in _as_list(entry.get("pattern"))
    ]
    combined_patterns = "\n".join(str(pattern) for pattern in filter_patterns)
    for required in REQUIRED_FILTER_SUBSTRINGS:
        if required not in combined_patterns:
            errors.append(f"Baseline missing required exclude pattern fragment: {required}")

    return {
        "status": "error" if errors else "ok",
        "baseline": _display_path(path),
        "errors": errors,
        "finding_count": finding_count,
        "scanned_file_count": len(results),
        "allowed_prefixes": list(ALLOWED_BASELINE_PREFIXES),
        "allowed_types": sorted(ALLOWED_BASELINE_TYPES),
    }


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
