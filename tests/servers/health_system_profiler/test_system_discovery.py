"""Tests for AHRQ-based system discovery."""

import pandas as pd
import pytest

from servers.health_system_profiler.system_discovery import (
    fuzzy_search_systems,
    resolve_system_ccns,
)


@pytest.fixture
def systems_df():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "health_sys_name": "Jefferson Health", "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 14},
        {"health_sys_id": "SYS_002", "health_sys_name": "Lehigh Valley Health Network", "health_sys_city": "Allentown", "health_sys_state": "PA", "hosp_count": 8},
        {"health_sys_id": "SYS_003", "health_sys_name": "Penn Medicine", "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 6},
        {"health_sys_id": "SYS_004", "health_sys_name": "Thomas Jefferson University Hospitals", "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 3},
    ])


@pytest.fixture
def hospitals_df():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "ccn": "390001", "hospital_name": "Thomas Jefferson University Hospital", "hos_beds": 900},
        {"health_sys_id": "SYS_001", "ccn": "390149", "hospital_name": "Jefferson Einstein Philadelphia", "hos_beds": 500},
        {"health_sys_id": "SYS_002", "ccn": "390133", "hospital_name": "Lehigh Valley Hospital-Cedar Crest", "hos_beds": 1190},
        {"health_sys_id": "SYS_002", "ccn": "390263", "hospital_name": "Lehigh Valley Hospital-Muhlenberg", "hos_beds": 184},
    ])


def test_fuzzy_search_exact(systems_df):
    results = fuzzy_search_systems("Jefferson Health", systems_df)
    assert len(results) >= 1
    assert results[0]["system_id"] == "SYS_001"


def test_fuzzy_search_partial(systems_df):
    results = fuzzy_search_systems("Jefferson", systems_df)
    assert len(results) >= 1
    names = [r["name"] for r in results]
    assert any("Jefferson" in n for n in names)


def test_fuzzy_search_case_insensitive(systems_df):
    results = fuzzy_search_systems("lehigh valley", systems_df)
    assert len(results) >= 1
    assert results[0]["system_id"] == "SYS_002"


def test_fuzzy_search_no_match(systems_df):
    results = fuzzy_search_systems("Mayo Clinic", systems_df)
    assert len(results) == 0


def test_resolve_system_ccns(hospitals_df):
    ccns = resolve_system_ccns("SYS_001", hospitals_df)
    assert set(ccns) == {"390001", "390149"}


def test_resolve_system_ccns_not_found(hospitals_df):
    ccns = resolve_system_ccns("SYS_999", hospitals_df)
    assert ccns == []
