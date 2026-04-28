from __future__ import annotations

import pytest

from servers.public_records import data_loaders, server


@pytest.fixture
def opais_cache(tmp_path, monkeypatch: pytest.MonkeyPatch):
    cache_dir = tmp_path / "public-records"
    cache_dir.mkdir()
    json_path = cache_dir / "340b_covered_entities.json"
    parquet_path = cache_dir / "340b_covered_entities.parquet"
    json_path.write_text(
        """
        [
          {
            "340B ID": "PA001",
            "Name of Covered Entity": "Thomas Jefferson University Hospital",
            "Covered Entity Type": "DSH",
            "Covered Entity Street Address": "111 S 11th St",
            "Covered Entity City": "Philadelphia",
            "Covered Entity State": "PA",
            "Covered Entity Zip Code": "19107",
            "Participating": "true"
          },
          {
            "340B ID": "PA002",
            "Name of Covered Entity": "Jefferson Einstein Hospital",
            "Covered Entity Type": "DSH",
            "Covered Entity City": "Philadelphia",
            "Covered Entity State": "PA",
            "Participating": "true"
          },
          {
            "340B ID": "PA003",
            "Name of Covered Entity": "Lehigh Valley Hospital - Cedar Crest",
            "Covered Entity Type": "DSH",
            "Covered Entity City": "Allentown",
            "Covered Entity State": "PA",
            "Participating": "true"
          },
          {
            "340B ID": "PA004",
            "Name of Covered Entity": "LVHN Coordinated Health Hospital",
            "Covered Entity Type": "DSH",
            "Covered Entity City": "Bethlehem",
            "Covered Entity State": "PA",
            "Participating": "true"
          }
        ]
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr(data_loaders, "_340B_JSON", json_path)
    monkeypatch.setattr(data_loaders, "_340B_PARQUET", parquet_path)
    return parquet_path


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "expected_name"),
    [
        ("Jefferson", "Thomas Jefferson University Hospital"),
        ("Einstein", "Jefferson Einstein Hospital"),
        ("LVHN", "LVHN Coordinated Health Hospital"),
        ("Thomas Jefferson University Hospital", "Thomas Jefferson University Hospital"),
    ],
)
async def test_get_340b_status_opais_aliases(opais_cache, query: str, expected_name: str) -> None:
    result = await server.get_340b_status(entity_name=query, state="PA")

    assert result["total_results"] >= 1
    assert any(entity["entity_name"] == expected_name for entity in result["entities"])
    assert opais_cache.exists()


@pytest.mark.asyncio
async def test_check_340b_status_alias_delegates_to_get_340b_status(opais_cache) -> None:
    result = await server.check_340b_status(entity_name="Einstein", state="PA")

    assert result["total_results"] == 1
    assert result["entities"][0]["entity_name"] == "Jefferson Einstein Hospital"
