"""
Dataset registry for discovering and validating local data packs.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .duckdb_adapter import get_parquet_pack_path
from .pack_validator import PackValidationResult, validate_data_pack


@dataclass
class DatasetRecord:
    """Metadata and readiness state for one dataset pack."""

    dataset_id: str
    pack_path: Path
    status: str
    is_valid: bool
    display_name: str
    schema_version: str
    imported_at: str | None
    source: str | None
    charges_count: int
    hospitals_count: int
    quality_flags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    validation_errors: list[str] = field(default_factory=list)
    validation_warnings: list[str] = field(default_factory=list)


class DatasetRegistry:
    """Discover and report local parquet data packs."""

    def __init__(self, packs_dir: str | Path | None = None):
        if packs_dir is None:
            packs_dir = get_parquet_pack_path()
        self.packs_dir = Path(packs_dir)

    def _iter_pack_dirs(self) -> list[Path]:
        """Return candidate pack directories sorted by name."""
        if not self.packs_dir.exists() or not self.packs_dir.is_dir():
            return []

        return sorted(
            [entry for entry in self.packs_dir.iterdir() if entry.is_dir()],
            key=lambda path: path.name.lower(),
        )

    def _build_record(self, validation: PackValidationResult) -> DatasetRecord:
        """Build a dataset record from validation output."""
        def _safe_int(value: Any, default: int) -> int:
            try:
                return int(value)
            except Exception:
                return default

        metadata = validation.metadata or {}
        dataset_id = validation.pack_path.name

        schema_version = str(
            metadata.get("schema_version") or metadata.get("version") or "1.0"
        )
        imported_at = metadata.get("created")
        source = metadata.get("source")
        display_name = str(metadata.get("name") or dataset_id)

        charges_count = _safe_int(
            metadata.get(
                "charges_count", validation.table_row_counts.get("charges", 0)
            ),
            validation.table_row_counts.get("charges", 0),
        )
        hospitals_count = _safe_int(
            metadata.get(
                "hospitals_count", validation.table_row_counts.get("hospitals", 0)
            ),
            validation.table_row_counts.get("hospitals", 0),
        )

        quality_flags: list[str] = []
        if not validation.is_valid:
            quality_flags.append("validation_failed")
        if validation.metadata_errors:
            quality_flags.append("metadata_error")
        if validation.missing_tables:
            quality_flags.append("missing_required_tables")
        if validation.schema_errors:
            quality_flags.append("schema_error")

        metadata_quality_flags = metadata.get("quality_flags")
        if isinstance(metadata_quality_flags, list):
            for flag in metadata_quality_flags:
                if isinstance(flag, str) and flag not in quality_flags:
                    quality_flags.append(flag)

        validation_errors = list(validation.metadata_errors)
        if validation.missing_tables:
            validation_errors.append(
                f"Missing tables: {', '.join(validation.missing_tables)}"
            )
        validation_errors += validation.schema_errors

        status = "ready" if validation.is_valid else "invalid"

        return DatasetRecord(
            dataset_id=dataset_id,
            pack_path=validation.pack_path,
            status=status,
            is_valid=validation.is_valid,
            display_name=display_name,
            schema_version=schema_version,
            imported_at=imported_at,
            source=source,
            charges_count=charges_count,
            hospitals_count=hospitals_count,
            quality_flags=quality_flags,
            metadata=metadata,
            validation_errors=validation_errors,
            validation_warnings=validation.warnings,
        )

    def list_datasets(self, include_invalid: bool = False) -> list[DatasetRecord]:
        """List discovered datasets."""
        records: list[DatasetRecord] = []
        for pack_dir in self._iter_pack_dirs():
            validation = validate_data_pack(pack_dir)
            record = self._build_record(validation)
            if include_invalid or record.is_valid:
                records.append(record)
        return records

    def get_dataset(self, dataset_id: str) -> DatasetRecord | None:
        """Return one dataset by id, or None if not found."""
        target = self.packs_dir / dataset_id
        if not target.exists() or not target.is_dir():
            return None

        validation = validate_data_pack(target)
        return self._build_record(validation)

    def get_default_dataset_id(self) -> str | None:
        """Select default dataset, preferring `main` when valid."""
        ready = self.list_datasets(include_invalid=False)
        if not ready:
            return None

        for record in ready:
            if record.dataset_id == "main":
                return "main"
        return ready[0].dataset_id
