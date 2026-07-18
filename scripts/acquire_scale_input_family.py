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
from shared.acquisition.scale_tabular_input_family import (
    build_tabular_public_evidence_input,
)
from shared.utils.cache import write_atomic_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--family",
        choices=("operating_revenue_usd", "annual_discharges"),
        required=True,
    )
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
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
    else:
        tabular_frozen = annual_discharges_acquisition()
        verify_annual_discharges_source_bytes(tabular_frozen, args.cache_root)
        evidence = build_tabular_public_evidence_input(
            tabular_frozen,
            producer_commit=args.source_commit,
        )
        frozen_payload = tabular_frozen.model_dump(mode="json")
    write_atomic_json(args.acquisition_output, frozen_payload)
    write_atomic_json(args.evidence_output, evidence.model_dump(mode="json"))


if __name__ == "__main__":
    main()
