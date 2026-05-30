"""Cache atomic write helper tests."""

from __future__ import annotations

import json

import pandas as pd

from shared.utils.cache import (
    CacheMetadata,
    read_cache_metadata,
    write_atomic_dataframe_csv,
    write_atomic_json,
    write_atomic_parquet,
    write_atomic_text,
    write_cache_metadata,
)


def test_atomic_text_and_json_writes(tmp_path) -> None:
    text_path = tmp_path / "artifact.txt"
    json_path = tmp_path / "artifact.json"

    write_atomic_text(text_path, "hello")
    write_atomic_json(json_path, {"b": 2, "a": 1})

    assert text_path.read_text(encoding="utf-8") == "hello"
    assert json.loads(json_path.read_text(encoding="utf-8")) == {"a": 1, "b": 2}


def test_cache_metadata_write_is_atomic_and_readable(tmp_path) -> None:
    artifact = tmp_path / "artifact.csv"
    artifact.write_text("x\n", encoding="utf-8")

    metadata_path = write_cache_metadata(
        artifact,
        CacheMetadata(source_url="https://example.org/source.csv", fetched_at="2026-05-29T00:00:00Z"),
    )

    assert metadata_path.exists()
    metadata = read_cache_metadata(artifact)
    assert metadata is not None
    assert metadata.source_url == "https://example.org/source.csv"


def test_atomic_dataframe_writes(tmp_path) -> None:
    df = pd.DataFrame([{"a": "1", "b": "2"}])
    csv_path = tmp_path / "frame.csv"
    parquet_path = tmp_path / "frame.parquet"

    write_atomic_dataframe_csv(csv_path, df, index=False)
    write_atomic_parquet(parquet_path, df, compression="zstd", index=False)

    assert pd.read_csv(csv_path, dtype=str).to_dict(orient="records") == [{"a": "1", "b": "2"}]
    assert pd.read_parquet(parquet_path).to_dict(orient="records") == [{"a": "1", "b": "2"}]
