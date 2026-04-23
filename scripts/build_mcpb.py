"""Build a local Claude Desktop MCPB package for healthcare-data-mcp.

The script stages files under build/mcpb and writes a .mcpb zip archive. It
does not modify project source files or global client configuration.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "desktop-extension" / "manifest.json"
DEFAULT_OUTPUT = REPO_ROOT / "dist" / "healthcare-data-mcp.mcpb"
DEFAULT_STAGE = REPO_ROOT / "build" / "mcpb" / "healthcare-data-mcp"
LAUNCHER_SOURCE = REPO_ROOT / "desktop-extension" / "server" / "launcher.py"

SERVER_NAMES = {
    "claims-analytics",
    "cms-facility",
    "discovery",
    "drive-time",
    "financial-intelligence",
    "gateway",
    "geo-demographics",
    "health-system-profiler",
    "hospital-quality",
    "physician-referral-network",
    "price-transparency",
    "public-records",
    "service-area",
    "web-intelligence",
    "workforce-analytics",
}

EXCLUDED_PARTS = {"__pycache__", ".pytest_cache"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Claude Desktop .mcpb package.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Source manifest.json path.")
    parser.add_argument("--stage-dir", type=Path, default=DEFAULT_STAGE, help="Temporary staging directory.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output .mcpb path.")
    parser.add_argument(
        "--server-name",
        default="cms-facility",
        choices=sorted(SERVER_NAMES),
        help="Default healthcare-data-mcp server to write into the staged manifest.",
    )
    parser.add_argument(
        "--skip-dependency-install",
        action="store_true",
        help="Create a manifest/launcher skeleton without vendoring Python dependencies.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output file.")
    return parser.parse_args()


def require_within_repo(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(REPO_ROOT):
        raise SystemExit(f"{label} must stay inside the repository: {resolved}")
    return resolved


def load_manifest(path: Path, server_name: str) -> dict[str, object]:
    with path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)

    required = {"manifest_version", "name", "version", "description", "author", "server"}
    missing = sorted(required - set(manifest))
    if missing:
        raise SystemExit(f"manifest missing required fields: {', '.join(missing)}")

    user_config = manifest.setdefault("user_config", {})
    if not isinstance(user_config, dict):
        raise SystemExit("manifest user_config must be an object")

    server_config = user_config.setdefault("server_name", {})
    if not isinstance(server_config, dict):
        raise SystemExit("manifest user_config.server_name must be an object")
    server_config["default"] = server_name

    return manifest


def reset_stage(stage_dir: Path) -> None:
    stage_dir = require_within_repo(stage_dir, "stage-dir")
    build_root = (REPO_ROOT / "build").resolve()
    if not stage_dir.is_relative_to(build_root):
        raise SystemExit(f"stage-dir must stay under {build_root}")

    if stage_dir.exists():
        shutil.rmtree(stage_dir)

    (stage_dir / "server").mkdir(parents=True)


def install_dependencies(stage_dir: Path) -> None:
    target = stage_dir / "server" / "lib"
    target.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--target",
        str(target),
        str(REPO_ROOT),
    ]
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def copy_runtime_files(stage_dir: Path, manifest: dict[str, object]) -> None:
    shutil.copy2(LAUNCHER_SOURCE, stage_dir / "server" / "launcher.py")
    with (stage_dir / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")


def should_zip(path: Path) -> bool:
    if any(part in EXCLUDED_PARTS for part in path.parts):
        return False
    return path.suffix not in EXCLUDED_SUFFIXES


def write_archive(stage_dir: Path, output: Path, force: bool) -> None:
    output = require_within_repo(output, "output")
    if output.exists() and not force:
        raise SystemExit(f"output already exists; pass --force to overwrite: {output}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(stage_dir.rglob("*")):
            if path.is_file() and should_zip(path.relative_to(stage_dir)):
                archive.write(path, path.relative_to(stage_dir).as_posix())


def main() -> None:
    args = parse_args()
    manifest_path = require_within_repo(args.manifest, "manifest")
    stage_dir = require_within_repo(args.stage_dir, "stage-dir")

    manifest = load_manifest(manifest_path, args.server_name)
    reset_stage(stage_dir)
    copy_runtime_files(stage_dir, manifest)

    if args.skip_dependency_install:
        print("Skipping dependency install; package is a manifest/launcher skeleton only.")
    else:
        install_dependencies(stage_dir)

    write_archive(stage_dir, args.output, args.force)
    print(f"Wrote {args.output.resolve()}")


if __name__ == "__main__":
    main()
