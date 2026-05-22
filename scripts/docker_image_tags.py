"""Render Docker image tags from the Python package version."""

from __future__ import annotations

import argparse
import json
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def package_version(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> str:
    """Return the canonical package version from pyproject.toml."""

    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def image_tags(image: str, *, include_latest: bool = True) -> list[str]:
    """Return versioned Docker tags for the package."""

    version = package_version()
    tags = [f"{image}:{version}"]
    if include_latest:
        tags.append(f"{image}:latest")
    return tags


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render healthcare-data-mcp Docker image tags.")
    parser.add_argument("--image", default="healthcare-data-mcp", help="Base image name, without tag.")
    parser.add_argument(
        "--format",
        choices=("lines", "json", "docker-build-args", "version"),
        default="lines",
        help="Output format.",
    )
    parser.add_argument("--no-latest", action="store_true", help="Only render the version tag.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tags = image_tags(args.image, include_latest=not args.no_latest)

    if args.format == "version":
        print(package_version())
    elif args.format == "json":
        print(json.dumps({"image": args.image, "version": package_version(), "tags": tags}, indent=2))
    elif args.format == "docker-build-args":
        rendered = [f"--build-arg VERSION={package_version()}"]
        rendered.extend(f"-t {tag}" for tag in tags)
        print(" ".join(rendered))
    else:
        print("\n".join(tags))


if __name__ == "__main__":
    main()
