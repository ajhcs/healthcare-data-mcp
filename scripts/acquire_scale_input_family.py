"""Rebuild a reviewed Scale input-family acquisition and evidence handoff."""

from __future__ import annotations

import argparse
from pathlib import Path

from shared.acquisition.scale_input_family import (
    build_public_evidence_input,
    require_clean_repository,
    require_outputs_outside_repository,
    require_repository_commit,
    repository_top_level,
    verify_source_bytes,
)
from shared.acquisition.scale_annual_discharges_packet import (
    acquisition as annual_discharges_acquisition,
    verify_annual_discharges_source_bytes,
)
from shared.acquisition.scale_operating_revenue_packet import acquisition as operating_revenue_acquisition
from shared.acquisition.scale_physician_count_evidence import (
    build_physician_count_public_evidence_input,
)
from shared.acquisition.scale_physician_count_packet import (
    acquisition as physician_count_acquisition,
    verify_physician_count_source_bytes,
)
from shared.acquisition.scale_service_line_count_evidence import (
    build_service_line_count_public_evidence_input,
)
from shared.acquisition.scale_service_line_count_packet import (
    acquisition as service_line_count_acquisition,
    verify_service_line_count_source_bytes,
)
from shared.acquisition.scale_safety_net_patient_mix_evidence import (
    build_safety_net_patient_mix_public_evidence_input,
)
from shared.acquisition.scale_safety_net_patient_mix_packet import (
    acquisition as safety_net_patient_mix_acquisition,
    verify_safety_net_patient_mix_source_bytes,
)
from shared.acquisition.scale_tabular_input_family import (
    build_tabular_public_evidence_input,
)
from shared.utils.cache import write_atomic_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--family",
        choices=(
            "operating_revenue_usd",
            "annual_discharges",
            "physician_count",
            "service_line_count",
            "safety_net_patient_mix_pct",
        ),
        required=True,
    )
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--cms-rbcs-report", type=Path)
    parser.add_argument("--cms-dsh-report", type=Path)
    parser.add_argument("--acquisition-output", type=Path, required=True)
    parser.add_argument("--evidence-output", type=Path, required=True)
    args = parser.parse_args()

    repo_root = repository_top_level(Path(__file__).resolve())
    require_clean_repository(repo_root)
    require_repository_commit(repo_root, args.source_commit)
    require_outputs_outside_repository(
        repo_root,
        [args.acquisition_output, args.evidence_output],
    )
    if args.family == "operating_revenue_usd":
        frozen = operating_revenue_acquisition()
        verify_source_bytes(frozen, args.cache_root)
        evidence = build_public_evidence_input(frozen, producer_commit=args.source_commit)
        frozen_payload = frozen.model_dump(mode="json")
    elif args.family == "annual_discharges":
        tabular_frozen = annual_discharges_acquisition()
        verify_annual_discharges_source_bytes(tabular_frozen, args.cache_root)
        evidence = build_tabular_public_evidence_input(
            tabular_frozen,
            producer_commit=args.source_commit,
        )
        frozen_payload = tabular_frozen.model_dump(mode="json")
    elif args.family == "physician_count":
        physician_frozen = physician_count_acquisition()
        verify_physician_count_source_bytes(physician_frozen, args.cache_root)
        evidence = build_physician_count_public_evidence_input(
            physician_frozen,
            producer_commit=args.source_commit,
        )
        frozen_payload = physician_frozen.model_dump(mode="json")
    elif args.family == "service_line_count":
        if args.cms_rbcs_report is None:
            parser.error("--cms-rbcs-report is required for service_line_count")
        service_line_frozen = service_line_count_acquisition()
        verify_service_line_count_source_bytes(
            service_line_frozen,
            args.cache_root,
            args.cms_rbcs_report,
        )
        evidence = build_service_line_count_public_evidence_input(
            service_line_frozen,
            producer_commit=args.source_commit,
        )
        frozen_payload = service_line_frozen.model_dump(mode="json")
    else:
        if args.cms_dsh_report is None:
            parser.error("--cms-dsh-report is required for safety_net_patient_mix_pct")
        safety_net_frozen = safety_net_patient_mix_acquisition()
        verify_safety_net_patient_mix_source_bytes(
            safety_net_frozen,
            args.cache_root,
            args.cms_dsh_report,
        )
        evidence = build_safety_net_patient_mix_public_evidence_input(
            safety_net_frozen,
            producer_commit=args.source_commit,
        )
        frozen_payload = safety_net_frozen.model_dump(mode="json")
    write_atomic_json(args.acquisition_output, frozen_payload)
    write_atomic_json(args.evidence_output, evidence.model_dump(mode="json"))


if __name__ == "__main__":
    main()
