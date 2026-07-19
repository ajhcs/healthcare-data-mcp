"""Rebuild a reviewed Scale input-family acquisition and evidence handoff."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

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
from shared.acquisition.scale_emergency_department_count_evidence import (
    build_emergency_department_count_public_evidence_input,
)
from shared.acquisition.scale_emergency_department_count_packet import (
    acquisition as emergency_department_count_acquisition,
    verify_emergency_department_count_source_bytes,
)
from shared.acquisition.scale_essential_service_designation_count_evidence import (
    build_essential_service_designation_count_public_evidence_input,
)
from shared.acquisition.scale_essential_service_designation_count_packet import (
    acquisition as essential_service_designation_count_acquisition,
    verify_essential_service_designation_count_source_bytes,
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
            "emergency_department_count",
            "essential_service_designation_count",
        ),
        required=True,
    )
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--cms-rbcs-report", type=Path)
    parser.add_argument("--cms-dsh-report", type=Path)
    parser.add_argument("--ahrq-linkage", type=Path)
    parser.add_argument("--cms-hgi", type=Path)
    parser.add_argument("--cms-hgi-metadata", type=Path)
    parser.add_argument("--cms-hospital-dictionary", type=Path)
    parser.add_argument("--ecfr-ed-definition", type=Path)
    parser.add_argument("--cms-psf-zip", type=Path)
    parser.add_argument("--cms-provider-type-manual", type=Path)
    parser.add_argument("--cms-psf-release-page", type=Path)
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
    elif args.family == "safety_net_patient_mix_pct":
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
    elif args.family == "emergency_department_count":
        custody = (
            args.ahrq_linkage,
            args.cms_hgi,
            args.cms_hgi_metadata,
            args.cms_hospital_dictionary,
            args.ecfr_ed_definition,
        )
        if any(path is None for path in custody):
            parser.error(
                "--ahrq-linkage, --cms-hgi, --cms-hgi-metadata, "
                "--cms-hospital-dictionary, and --ecfr-ed-definition are required "
                "for emergency_department_count"
            )
        emergency_frozen = emergency_department_count_acquisition()
        exact_custody = tuple(cast(Path, path) for path in custody)
        verify_emergency_department_count_source_bytes(
            emergency_frozen,
            args.cache_root,
            *exact_custody,
        )
        evidence = build_emergency_department_count_public_evidence_input(
            emergency_frozen,
            producer_commit=args.source_commit,
        )
        frozen_payload = emergency_frozen.model_dump(mode="json")
    else:
        custody = (
            args.ahrq_linkage, args.cms_psf_zip,
            args.cms_provider_type_manual, args.cms_psf_release_page,
        )
        if any(path is None for path in custody):
            parser.error(
                "--ahrq-linkage, --cms-psf-zip, --cms-provider-type-manual, "
                "and --cms-psf-release-page are required for "
                "essential_service_designation_count"
            )
        designation_frozen = essential_service_designation_count_acquisition()
        exact_custody = tuple(cast(Path, path) for path in custody)
        verify_essential_service_designation_count_source_bytes(
            designation_frozen, args.cache_root, *exact_custody,
        )
        evidence = build_essential_service_designation_count_public_evidence_input(
            designation_frozen, producer_commit=args.source_commit,
        )
        frozen_payload = designation_frozen.model_dump(mode="json")
    write_atomic_json(args.acquisition_output, frozen_payload)
    write_atomic_json(args.evidence_output, evidence.model_dump(mode="json"))


if __name__ == "__main__":
    main()
