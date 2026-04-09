"""Tests for physician referral leakage classification."""

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
