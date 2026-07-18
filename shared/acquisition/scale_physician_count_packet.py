"""Reviewed all-six AHRQ physician-count acquisition declaration."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from shared.acquisition.scale_annual_discharges_packet import (
    acquisition as annual_acquisition,
)
from shared.acquisition.scale_physician_count_contract import (
    PHYSICIAN_BLOCKERS,
    PhysicianCountAcquisition,
    PhysicianSystemRow,
    build_physician_count_acquisition,
)
from shared.acquisition.scale_physician_count_declaration import (
    EXTRA_BLOCKERS,
    FINDINGS,
    SYSTEM_ROW_DECLARATIONS,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS

ACQUIRED_AT = "2026-07-18T12:00:00Z"

def acquisition() -> PhysicianCountAcquisition:
    """Return the immutable third-cycle physician-count acquisition."""

    annual = annual_acquisition()
    annual_rows = {row.system_slug: row for row in annual.system_rows}
    system_rows = []
    for slug in SYSTEM_SLUGS:
        source = annual_rows[slug]
        declared = SYSTEM_ROW_DECLARATIONS[slug]
        system_rows.append(
            PhysicianSystemRow(
                system_slug=slug,
                artifact_ref=source.artifact_ref,
                row_number=source.row_number,
                source_row_sha256=source.source_row_sha256,
                health_sys_id=source.health_sys_id,
                health_sys_name=source.health_sys_name,
                health_sys_city=source.health_sys_city,
                health_sys_state=source.health_sys_state,
                raw_lexical_value=declared.total_mds_raw,
                prim_care_mds_raw=declared.prim_care_mds_raw,
                hosp_cnt_raw=source.hosp_cnt_raw,
                acutehosp_cnt_raw=source.acutehosp_cnt_raw,
                sys_multistate_raw=source.sys_multistate_raw,
            )
        )
    candidates = []
    for row in system_rows:
        candidates.append(
            {
                "system_slug": row.system_slug,
                "input_family": "physician_count",
                "candidate_value": int(row.raw_lexical_value),
                "unit": "physicians",
                "source_period": "2023",
                "definition": "AHRQ Compendium 2023 system-file column total_mds; official technical definition not locally receipted",
                "basis": "source-local AHRQ highest-ownership system row; employed, affiliated, active-status, specialty, and deduplication bases unapproved",
                "source_artifact_refs": [row.artifact_ref],
                "system_row_ref": f"row:system:{row.health_sys_id}:{row.row_number}",
                "blocker_codes": sorted(PHYSICIAN_BLOCKERS | EXTRA_BLOCKERS[row.system_slug]),
                "finding": FINDINGS[row.system_slug],
                "missingness": "blocked_source_conflict",
                "imputed": False,
                "aggregated": False,
                "fabricated_zero": False,
                "approved_for_scale": False,
            }
        )
    return build_physician_count_acquisition(
        {
            "schema_version": "ushso.scale-physician-count-acquisition.v3",
            "acquisition_id": "scale-physician-count-all-six-2026-07-18",
            "workflow_id": "scale-physician-count-acquisition.v3",
            "input_family": "physician_count",
            "systems": list(SYSTEM_SLUGS),
            "acquired_at": ACQUIRED_AT,
            "producer_version": "HDM-auf",
            "prior_cycle": {
                "repo": "healthcare-toolkit",
                "input_family": "annual_discharges",
                "binding_merge": _group_git_sha("76e16247cecce818d777b4a4ade56dc13dd7b2a8"),  # pragma: allowlist secret
                "binding_tracker_merge": _group_git_sha("420d35d8024de1c484c1b16128836e0f8b00375c"),  # pragma: allowlist secret
                "admission_merge": _group_git_sha("9aed9059962cbf2a03c7c02e6056aee4281ee340"),  # pragma: allowlist secret
                "tracker_merge": _group_git_sha("2d33cab9264e636bd392b89757f8b05ed2729ecb"),  # pragma: allowlist secret
                "cumulative_packet_sha256": "sha256:bb569b3dde1fa4435c658488b11493ebcfe88898f8d7b0571231ce66ca7621a6",  # pragma: allowlist secret
                "cumulative_review_sha256": "sha256:b83433afce89012b9584c8a5df4449e78f112916fd2db4894495f4e6b1bcf1d6",  # pragma: allowlist secret
                "cumulative_assurance_sha256": "sha256:4caa86f1c57a8ce45cc3df304bd4f03e841f563418db1babfe39668343ff5cf1",  # pragma: allowlist secret
                "terminal_status": "blocked",
                "failure_code": "human_review_required",
            },
            "cache_receipt": annual.cache_receipt.model_dump(mode="json"),
            "source_artifacts": [
                item.model_dump(mode="json")
                for item in annual.source_artifacts
                if item.relative_path == "ahrq_system_2023.csv"
            ],
            "system_rows": [item.model_dump(mode="json") for item in system_rows],
            "candidates": candidates,
            "physician_definition_receipt": None,
            "physician_definition_custody": "not_locally_receipted",
            "raw_http_receipt_custody": "not_locally_receipted",
            "redistribution_license_receipt": None,
            "redistribution_rights_custody": "unreviewed",
            "prohibited_outputs": sorted(
                {
                    "scale_score",
                    "component_score",
                    "sensitivity_result",
                    "projection",
                    "adjudication",
                    "recommendation",
                    "promotion",
                }
            ),
        }
    )


def verify_physician_count_source_bytes(value: PhysicianCountAcquisition, cache_root: Path) -> None:
    """Verify shared AHRQ custody plus each exact total_mds source cell."""

    annual = annual_acquisition()
    annual_system_artifacts = [
        item for item in annual.source_artifacts if item.relative_path == "ahrq_system_2023.csv"
    ]
    if value.source_artifacts != annual_system_artifacts:
        raise ValueError("physician acquisition AHRQ custody drift")

    artifact = next(
        item for item in value.source_artifacts if item.relative_path == "ahrq_system_2023.csv"
    )
    manifest_path = cache_root / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    manifest_raw = manifest_path.read_bytes()
    if (
        len(manifest_raw) != value.cache_receipt.manifest_content_length
        or _sha256(manifest_raw) != value.cache_receipt.manifest_sha256
    ):
        raise ValueError("validated cache manifest byte drift")
    manifest = json.loads(manifest_raw)
    expected_manifest_fields: tuple[tuple[str, object], ...] = (
        ("dataset_id", value.cache_receipt.dataset_id),
        ("artifact_id", value.cache_receipt.dataset_artifact_id),
        ("run_id", value.cache_receipt.run_id),
        ("artifact_role", value.cache_receipt.artifact_role),
        ("cache_status", value.cache_receipt.cache_status),
        ("validation_status", value.cache_receipt.validation_status),
        ("loader_version", value.cache_receipt.loader_version),
        ("validator_version", value.cache_receipt.validator_version),
        ("etag", value.cache_receipt.etag),
        ("last_modified", value.cache_receipt.last_modified),
        ("source_period", value.cache_receipt.source_period_metadata),
    )
    for key, expected_manifest_value in expected_manifest_fields:
        if manifest.get(key) != expected_manifest_value:
            raise ValueError(f"validated cache manifest {key} drift")
    entry = next(
        item
        for item in manifest["artifacts"]
        if item.get("relative_path") == artifact.relative_path
    )
    expected_artifact_fields: tuple[tuple[str, object], ...] = (
        ("checksum_sha256", artifact.payload_sha256.removeprefix("sha256:")),
        ("content_length", artifact.content_length),
        ("row_count", artifact.row_count),
        ("schema_fingerprint", artifact.schema_fingerprint.removeprefix("sha256:")),
        ("source_url", artifact.source_url),
        ("validation_status", artifact.validation_status),
        ("promoted_at", artifact.cache_promoted_at.isoformat()),
    )
    for key, expected_artifact_value in expected_artifact_fields:
        if entry.get(key) != expected_artifact_value:
            raise ValueError(f"validated physician source artifact {key} drift")
    path = Path(entry["path"]).resolve()
    if not path.is_relative_to(cache_root.resolve()):
        raise ValueError("physician source artifact escaped cache root")
    raw_lines = path.read_bytes().splitlines(keepends=True)
    raw = path.read_bytes()
    if len(raw) != artifact.content_length or _sha256(raw) != artifact.payload_sha256:
        raise ValueError("frozen physician source byte drift")
    if _sha256(raw_lines[0]) != artifact.header_sha256:
        raise ValueError("frozen physician source header drift")
    with path.open(newline="", encoding="cp1252") as handle:
        parsed = {number: row for number, row in enumerate(csv.DictReader(handle), start=2)}
    for expected_row in value.system_rows:
        parsed_row = parsed.get(expected_row.row_number)
        if parsed_row is None or hashlib.sha256(raw_lines[expected_row.row_number - 1]).hexdigest() != expected_row.source_row_sha256.removeprefix("sha256:"):
            raise ValueError("physician source row byte drift")
        fields = {
            "health_sys_id": expected_row.health_sys_id,
            "health_sys_name": expected_row.health_sys_name,
            "health_sys_city": expected_row.health_sys_city,
            "health_sys_state": expected_row.health_sys_state,
            "total_mds": expected_row.raw_lexical_value,
            "prim_care_mds": expected_row.prim_care_mds_raw,
            "hosp_cnt": expected_row.hosp_cnt_raw,
            "acutehosp_cnt": expected_row.acutehosp_cnt_raw,
            "sys_multistate": expected_row.sys_multistate_raw,
        }
        if any(parsed_row.get(key) != expected_value for key, expected_value in fields.items()):
            raise ValueError(f"physician source row field drift for {expected_row.system_slug}")


def _sha256(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _group_git_sha(value: str) -> str:
    if len(value) != 40:
        raise ValueError("Git SHA-1 must contain exactly 40 hexadecimal characters")
    return f"{value[:8]}-{value[8:12]}-{value[12:16]}-{value[16:20]}-{value[20:32]}-{value[32:]}"


__all__ = ["acquisition", "verify_physician_count_source_bytes"]
