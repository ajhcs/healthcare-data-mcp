"""Transactional admission tests for the AHRQ browser acquisition script."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.download_ahrq import (
    _replace_validated_files,
    _verify_matching_retrievals,
    acquire,
)


@pytest.mark.asyncio
async def test_acquire_defaults_to_revised_2023(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = tmp_path / "receipt.json"

    async def fake_acquire(
        output_dir: Path,
        *,
        force: bool,
        receipt_path: Path | None,
    ) -> Path:
        assert output_dir == tmp_path
        assert force is False
        assert receipt_path is None
        return expected

    monkeypatch.setattr("scripts.download_ahrq._acquire_revised_2023", fake_acquire)

    assert await acquire(tmp_path, force=False) == expected


def test_verify_matching_retrievals_accepts_complete_identical_pair() -> None:
    verified = _verify_matching_retrievals(
        {
            "system_universe": [b"system", b"system"],
            "hospital_linkage": [b"hospital", b"hospital"],
        }
    )

    assert verified == {
        "system_universe": b"system",
        "hospital_linkage": b"hospital",
    }


@pytest.mark.parametrize(
    ("retrievals", "message"),
    [
        (
            {
                "system_universe": [b"first", b"second"],
                "hospital_linkage": [b"hospital", b"hospital"],
            },
            "byte drift",
        ),
        (
            {"system_universe": [b"system", b"system"]},
            "both artifact roles",
        ),
        (
            {
                "system_universe": [b"system"],
                "hospital_linkage": [b"hospital", b"hospital"],
            },
            "exactly two retrievals",
        ),
    ],
)
def test_verify_matching_retrievals_fails_closed(
    retrievals: dict[str, list[bytes]],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        _verify_matching_retrievals(retrievals)


def test_replace_validated_files_promotes_release_together(tmp_path: Path) -> None:
    system_path = tmp_path / "system.csv"
    hospital_path = tmp_path / "hospital.csv"
    receipt_path = tmp_path / "receipt.json"

    _replace_validated_files(
        {
            system_path: b"system-new",
            hospital_path: b"hospital-new",
            receipt_path: b"receipt-new",
        }
    )

    assert system_path.read_bytes() == b"system-new"
    assert hospital_path.read_bytes() == b"hospital-new"
    assert receipt_path.read_bytes() == b"receipt-new"
    assert not tuple(tmp_path.glob(".*.candidate"))
    assert not tuple(tmp_path.glob(".*.backup"))


def test_replace_validated_files_restores_release_after_mid_promotion_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    system_path = tmp_path / "system.csv"
    hospital_path = tmp_path / "hospital.csv"
    receipt_path = tmp_path / "receipt.json"
    system_path.write_bytes(b"system-old")
    hospital_path.write_bytes(b"hospital-old")
    receipt_path.write_bytes(b"receipt-old")
    original_replace = Path.replace

    def fail_second_candidate(source: Path, target: Path) -> Path:
        if source.name.endswith(".candidate") and target == hospital_path:
            raise OSError("injected promotion failure")
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_second_candidate)

    with pytest.raises(OSError, match="injected promotion failure"):
        _replace_validated_files(
            {
                system_path: b"system-new",
                hospital_path: b"hospital-new",
                receipt_path: b"receipt-new",
            }
        )

    assert system_path.read_bytes() == b"system-old"
    assert hospital_path.read_bytes() == b"hospital-old"
    assert receipt_path.read_bytes() == b"receipt-old"
    assert not tuple(tmp_path.glob(".*.candidate"))
    assert not tuple(tmp_path.glob(".*.backup"))
