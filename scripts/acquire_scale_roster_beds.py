"""Acquire or rebuild the frozen all-six Scale roster/bed handoff."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from shared.acquisition.scale_roster_beds import (
    acquire,
    build_bundle_input,
    load_frozen,
    load_spec,
    verify_frozen_bytes,
    write_bundle_input,
    write_frozen_acquisition,
)
from shared.acquisition.scale_roster_bed_packet import acquisition_spec
from shared.utils.cache import write_atomic_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path)
    parser.add_argument("--write-spec", type=Path)
    parser.add_argument("--frozen", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path)
    parser.add_argument("--cache-run-id")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    spec = load_spec(args.spec) if args.spec is not None else acquisition_spec()
    if args.write_spec is not None:
        write_atomic_json(args.write_spec, spec.model_dump(mode="json"))
    if args.offline:
        frozen = load_frozen(args.frozen)
        if args.cache_root is not None:
            frozen = verify_frozen_bytes(spec, frozen, cache_root=args.cache_root)
    else:
        if args.cache_root is None or not args.cache_run_id:
            parser.error("live acquisition requires --cache-root and --cache-run-id")
        frozen = asyncio.run(acquire(spec, cache_root=args.cache_root, cache_run_id=args.cache_run_id))
        write_frozen_acquisition(args.frozen, frozen)
    write_bundle_input(args.output, build_bundle_input(spec, frozen))


if __name__ == "__main__":
    main()
