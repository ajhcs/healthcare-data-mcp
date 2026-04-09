"""Tests for physician referral leakage classification."""

from pathlib import Path

import pandas as pd

from servers.physician_referral_network import referral_network


def test_detect_leakage_uses_hsa_service_area_zip_membership(monkeypatch):
    outbound = pd.DataFrame(
        [
            {"npi_to": "sys-1", "total_shared": 40},
            {"npi_to": "dest-in-area", "total_shared": 35},
            {"npi_to": "dest-out-area", "total_shared": 25},
        ]
    )

    monkeypatch.setattr(referral_network, "is_docgraph_cached", lambda: True)
    monkeypatch.setattr(
        referral_network,
        "_get_outbound_referrals",
        lambda system_npis, min_shared=11: outbound,
    )

    result = referral_network.detect_leakage(
        system_npis={"sys-1"},
        system_zips={"44106", "44195"},
        destination_zip_by_npi={
            "dest-in-area": "44106",
            "dest-out-area": "10001",
        },
        min_shared=11,
    )

    assert result["total_referrals"] == 100
    assert result["in_network_pct"] == 40.0
    assert result["out_of_network_in_area_pct"] == 35.0
    assert result["out_of_area_pct"] == 25.0
    assert result["top_leakage_destinations"][0]["classification"] == "out_of_network_in_area"
    assert result["top_leakage_destinations"][1]["classification"] == "out_of_area"


def test_detect_leakage_falls_back_to_out_of_area_without_destination_zip(monkeypatch):
    outbound = pd.DataFrame([{"npi_to": "dest-unknown", "total_shared": 20}])

    monkeypatch.setattr(referral_network, "is_docgraph_cached", lambda: True)
    monkeypatch.setattr(
        referral_network,
        "_get_outbound_referrals",
        lambda system_npis, min_shared=11: outbound,
    )

    result = referral_network.detect_leakage(
        system_npis={"sys-1"},
        system_zips={"44106"},
        destination_zip_by_npi={},
        min_shared=11,
    )

    assert result["in_network_pct"] == 0.0
    assert result["out_of_network_in_area_pct"] == 0.0
    assert result["out_of_area_pct"] == 100.0
    assert result["top_leakage_destinations"][0]["classification"] == "out_of_area"


def test_load_docgraph_csv_normalizes_columns_and_writes_cache(monkeypatch, tmp_path):
    csv_path = tmp_path / "docgraph.csv"
    cache_path = tmp_path / "shared_patients.parquet"
    captured: dict[str, object] = {}

    csv_path.write_text(
        "NPI 1,NPI 2,Shared Patients,Transactions\n"
        "1111111111,2222222222,14,20\n"
        "3333333333,4444444444,18,21\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(referral_network, "_SHARED_PATIENTS_CACHE", Path(cache_path))
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, path, compression="zstd", index=False: captured.update({
            "path": Path(path),
            "rows": self.to_dict(orient="records"),
        }),
    )

    rows_loaded = referral_network.load_docgraph_csv(csv_path)

    assert rows_loaded == 2
    assert captured["path"] == cache_path
    assert captured["rows"] == [
        {
            "npi_from": "1111111111",
            "npi_to": "2222222222",
            "shared_count": 14,
            "transaction_count": 20,
            "same_day_count": 0,
        },
        {
            "npi_from": "3333333333",
            "npi_to": "4444444444",
            "shared_count": 18,
            "transaction_count": 21,
            "same_day_count": 0,
        },
    ]
