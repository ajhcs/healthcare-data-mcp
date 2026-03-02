"""Tests for health system profiler response models."""

from servers.health_system_profiler.models import (
    BedBreakdown,
    FacilitySummary,
    HealthSystemSummary,
    OffSiteSummary,
    SystemProfileResponse,
    SystemSearchResult,
)


def test_system_search_result_defaults():
    result = SystemSearchResult(system_id="SYS_001", name="Test Health")
    assert result.system_id == "SYS_001"
    assert result.name == "Test Health"
    assert result.hq_city == ""
    assert result.hq_state == ""
    assert result.hospital_count == 0
    assert result.total_beds == 0


def test_bed_breakdown_defaults():
    beds = BedBreakdown()
    assert beds.total == 0
    assert beds.certified == 0
    assert beds.psychiatric == 0
    assert beds.rehabilitation == 0


def test_facility_summary_serialization():
    facility = FacilitySummary(
        ccn="390133",
        name="Test Hospital",
        beds=BedBreakdown(total=500, certified=480),
    )
    d = facility.model_dump()
    assert d["ccn"] == "390133"
    assert d["beds"]["total"] == 500
    assert d["beds"]["certified"] == 480


def test_system_profile_response_structure():
    profile = SystemProfileResponse(
        system=HealthSystemSummary(
            system_id="SYS_001",
            name="Test Health",
            hq_city="Philadelphia",
            hq_state="PA",
            hospital_count=3,
            total_beds=1500,
            total_discharges=50000,
        ),
        inpatient_facilities=[],
        sub_entities=[],
        outpatient_sites=[],
        off_site_summary=OffSiteSummary(),
    )
    d = profile.model_dump()
    assert d["system"]["name"] == "Test Health"
    assert d["system"]["hospital_count"] == 3
    assert isinstance(d["inpatient_facilities"], list)
    assert isinstance(d["outpatient_sites"], list)
