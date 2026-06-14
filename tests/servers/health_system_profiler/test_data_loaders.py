"""Tests for health system profiler data loaders."""

import pytest

from servers.health_system_profiler.data_loaders import (
    parse_ahrq_hospital_linkage,
    parse_ahrq_system_file,
    parse_pos_file,
)


@pytest.fixture
def sample_ahrq_system_csv(tmp_path):
    """Create a minimal AHRQ system file CSV."""
    csv_path = tmp_path / "system.csv"
    csv_path.write_text(
        "health_sys_id,health_sys_name,health_sys_city,health_sys_state,hosp_count,phys_grp_count\n"
        "SYS_001,Jefferson Health,Philadelphia,PA,14,25\n"
        "SYS_002,Lehigh Valley Health Network,Allentown,PA,8,12\n"
        "SYS_003,Penn Medicine,Philadelphia,PA,6,30\n"
    )
    return csv_path


@pytest.fixture
def sample_ahrq_hospital_linkage_csv(tmp_path):
    """Create a minimal AHRQ hospital linkage CSV."""
    csv_path = tmp_path / "hospital_linkage.csv"
    csv_path.write_text(
        "health_sys_id,ccn,hospital_name,hosp_addr,hosp_city,hosp_state,hosp_zip,hos_beds,hos_dsch,ownership,revenue,teaching\n"
        "SYS_001,390001,Thomas Jefferson University Hospital,111 S 11th St,Philadelphia,PA,19107,900,40000,Voluntary nonprofit,1000000,Yes\n"
        "SYS_001,390149,Jefferson Einstein Philadelphia,5501 Old York Rd,Philadelphia,PA,19141,500,20000,Voluntary nonprofit,500000,Yes\n"
        "SYS_002,390133,Lehigh Valley Hospital-Cedar Crest,1200 S Cedar Crest Blvd,Allentown,PA,18103,1190,55000,Voluntary nonprofit,2000000,Yes\n"
        "SYS_002,390263,Lehigh Valley Hospital-Muhlenberg,2545 Schoenersville Rd,Bethlehem,PA,18017,184,8000,Voluntary nonprofit,300000,No\n"
    )
    return csv_path


@pytest.fixture
def sample_pos_csv(tmp_path):
    """Create a minimal POS CSV."""
    csv_path = tmp_path / "pos.csv"
    csv_path.write_text(
        "PRVDR_NUM,FAC_NAME,BED_CNT,STATE_CD\n"
        "390001,Jefferson Main,900,PA\n"
        "390149,Jefferson Einstein,500,PA\n"
    )
    return csv_path


def test_parse_ahrq_system_file(sample_ahrq_system_csv):
    df = parse_ahrq_system_file(sample_ahrq_system_csv)
    assert len(df) == 3
    assert "health_sys_id" in df.columns
    assert "health_sys_name" in df.columns
    assert df.iloc[0]["health_sys_name"] == "Jefferson Health"


def test_parse_ahrq_system_file_preserves_snapshot_metric_fields(tmp_path):
    csv_path = tmp_path / "system_2023.csv"
    csv_path.write_text(
        "health_sys_id,health_sys_name,health_sys_city,health_sys_state,total_mds,prim_care_mds,total_nps,total_pas,grp_cnt,hosp_cnt,acutehosp_cnt,sys_beds,sys_dsch\n"
        "HSI00000123,Example Health,Example City,AL,012,5,7,3,4,2,1,090,1000\n",
        encoding="utf-8",
    )

    df = parse_ahrq_system_file(csv_path)

    row = df.iloc[0]
    assert row["health_sys_id"] == "HSI00000123"
    assert row["total_mds"] == 12
    assert row["prim_care_mds"] == 5
    assert row["grp_cnt"] == 4
    assert row["phys_grp_count"] == 4
    assert row["acutehosp_cnt"] == 1
    assert row["sys_beds"] == 90


def test_parse_ahrq_hospital_linkage(sample_ahrq_hospital_linkage_csv):
    df = parse_ahrq_hospital_linkage(sample_ahrq_hospital_linkage_csv)
    assert len(df) == 4
    assert "health_sys_id" in df.columns
    assert "ccn" in df.columns
    assert df.iloc[0]["ccn"] == "390001"
    assert df.iloc[0]["hos_beds"] == 900
    jefferson = df[df["health_sys_id"] == "SYS_001"]
    assert len(jefferson) == 2


def test_parse_ahrq_hospital_linkage_preserves_identifiers_and_leading_zeroes(tmp_path):
    csv_path = tmp_path / "hospital_linkage_2023.csv"
    csv_path.write_text(
        "compendium_hospital_id,ccn,hospital_name,hospital_street,hospital_city,hospital_state,hospital_zip,acutehosp_flag,health_sys_id,hos_beds,hos_dsch\n"
        "CHSP00000001,010001,Example Hospital,1 Main St,Dothan,AL,00301,1,HSI00000123,080,100\n"
        "CHSP00000002,,Missing CCN Hospital,2 Main St,Dothan,AL,00302,0,HSI00000123,,50\n",
        encoding="utf-8",
    )

    df = parse_ahrq_hospital_linkage(csv_path)

    assert df.iloc[0]["compendium_hospital_id"] == "CHSP00000001"
    assert df.iloc[0]["ccn"] == "010001"
    assert df.iloc[0]["hospital_zip"] == "00301"
    assert df.iloc[0]["hos_beds"] == 80
    assert df.iloc[1]["ccn"] == ""
    assert df.iloc[1]["hospital_zip"] == "00302"


def test_parse_pos_file(sample_pos_csv):
    df = parse_pos_file(sample_pos_csv)
    assert len(df) == 2
    assert "PRVDR_NUM" in df.columns
    assert "FAC_NAME" in df.columns
    assert df.iloc[0]["BED_CNT"] == "900"  # dtype=str
