"""Reviewed source registry for the all-six roster and bed-basis handoff.

This is intentionally a compact acquisition specification, not an evidence
bundle. The workflow verifies every populated row against frozen source bytes
before mechanically generating the checked-in handoff input.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from shared.acquisition.scale_roster_bed_models import (
    AcquisitionSpec,
    ConflictSpec,
    EntitySpec,
    FactSpec,
    Missingness,
    SourceSpec,
)

SYSTEM_IDS = {
    "christianacare": "data-mcp:system:christianacare",
    "jefferson-health": "data-mcp:system:jefferson-health",
    "temple-health": "data-mcp:system:temple-health",
    "penn-medicine": "data-mcp:system:penn-medicine",
    "cooper-university-health-care": "data-mcp:system:cooper-university-health-care",
    "main-line-health": "data-mcp:system:main-line-health",
}


@dataclass(frozen=True)
class Candidate:
    system_slug: str
    slug: str
    name: str
    source_id: str
    disposition: Literal["included", "excluded", "unresolved"] = "included"
    entity_type: Literal["hospital", "facility", "campus"] = "hospital"
    ccn: str = ""
    state_license_id: str = ""
    missingness: Missingness = "not_yet_researched"
    missingness_reason: str = "A known primary or regulatory source still requires facility-level extraction."
    source_name: str = ""

    @property
    def entity_id(self) -> str:
        if self.ccn:
            return f"data-mcp:facility:ccn:{self.ccn}"
        return f"data-mcp:facility:official:{self.system_slug}:{self.slug}"


@dataclass(frozen=True)
class OfficialBedFact:
    fact_id: str
    entity_id: str
    source_id: str
    measure_id: str
    extraction_pattern: str
    period: str
    basis: str
    row_locator: str


@dataclass(frozen=True)
class StateBedRow:
    entity_id: str
    source_id: str
    source_name: str
    match_field: str
    match_value: str
    licensed_field: str
    staffed_field: str | None = None
    state_license_field: str | None = None


def _source(
    source_id: str,
    name: str,
    url: str,
    period: str,
    media: str,
    parser: Literal["html", "pdf", "csv", "xlsx", "text"],
    *,
    landing_page: str | None = None,
    public_domain: bool = False,
    encoding: Literal["utf-8-sig", "cp1252"] = "utf-8-sig",
    header_row: int = 1,
) -> SourceSpec:
    return SourceSpec(
        source_id=source_id,
        source_name=name,
        dataset_id=source_id,
        registry_id=f"scale-source:{source_id}",
        registry_version=period,
        url=url,
        landing_page=landing_page or url,
        source_period=period,
        expected_media_type=media,
        rights_classification="public_domain" if public_domain else "unknown_review_required",
        parser_kind=parser,
        encoding=encoding,
        header_row=header_row,
    )


SOURCES = [
    _source(
        "christianacare-about",
        "ChristianaCare Who We Are",
        "https://christianacare.org/us/en/about-us/who-we-are",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    _source(
        "christianacare-union",
        "ChristianaCare Union Hospital",
        "https://christianacare.org/us/en/facilities/union-hospital",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    _source(
        "christianacare-west-grove",
        "ChristianaCare Hospital West Grove opening announcement",
        "https://news.christianacare.org/2025/08/christianacare-hospital-west-grove-now-open/",
        "2025-08-13",
        "text/html",
        "html",
    ),
    _source(
        "jefferson-enterprise-2025",
        "Jefferson Enterprise Facts and Figures",
        "https://www.jeffersonhealth.org/content/dam/health2021/documents/about/we-are-jefferson-stats-booklet-march-2025.pdf",
        "March 2025",
        "application/pdf",
        "pdf",
        landing_page="https://www.jeffersonhealth.org/about-us",
    ),
    _source(
        "sepa-chna-2025",
        "Southeastern Pennsylvania Regional CHNA 2025 Jefferson Northeast excerpt",
        "https://www.jeffersonhealth.org/content/dam/health2021/documents/informational/26-0036-fy26-jh_hospitals-overview-profiles-jeff-northeast-chna2025-final.pdf",
        "2025",
        "application/pdf",
        "pdf",
        landing_page="https://www.jeffersonhealth.org/about-us/community/community-health-needs-assessment",
    ),
    _source(
        "temple-locations",
        "Temple Health Locations",
        "https://www.templehealth.org/locations",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    _source(
        "temple-episcopal",
        "Temple University Hospital Episcopal Campus About",
        "https://www.templehealth.org/locations/episcopal-campus-tuh/about",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    _source(
        "temple-chestnut-hill",
        "Temple Health Chestnut Hill Hospital About",
        "https://www.templehealth.org/locations/chestnut-hill-hospital/about",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    _source(
        "penn-six-hospitals-2024",
        "Penn Medicine 2024 Healthcare Equality announcement",
        "https://www.pennmedicine.org/news/penn-medicine-named-2024-lgbtq-health-care-leader-by-hrc",
        "2024-05-28",
        "text/html",
        "html",
        landing_page="https://www.pennmedicine.org/about",
    ),
    _source(
        "penn-princeton-house-2024",
        "Penn Medicine Princeton Health hybrid nursing residency announcement",
        "https://www.pennmedicine.org/news/princeton-health-hybrid-nursing-residency",
        "2024-08-14",
        "text/html",
        "html",
        landing_page="https://www.pennmedicine.org/about",
    ),
    _source(
        "cooper-about",
        "Cooper University Health Care About Us",
        "https://www.cooperhealth.org/about-us",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    _source(
        "cooper-camden-2025",
        "Cooper University Hospital Fast Facts",
        "https://www.cooperhealth.org/sites/default/files/about-us/Cooper%20Fast%20Facts%20Q1%202025.pdf",
        "Q1 2025",
        "application/pdf",
        "pdf",
        landing_page="https://www.cooperhealth.org/about-us",
    ),
    _source(
        "cooper-cape-2025",
        "Cooper University Hospital Cape Regional Fast Facts",
        "https://www.cooperhealth.org/sites/default/files/about-us/Cooper%20Cape%20Fast%20Facts%20Q1%202025.pdf",
        "Q1 2025",
        "application/pdf",
        "pdf",
        landing_page="https://www.cooperhealth.org/about-us",
    ),
    _source(
        "main-line-about",
        "Main Line Health About",
        "https://www.mainlinehealth.org/about",
        "current page retrieved at acquisition cutoff",
        "text/html",
        "html",
    ),
    *[
        _source(
            source_id,
            name,
            url,
            "current fact sheet retrieved at acquisition cutoff",
            "application/pdf",
            "pdf",
            landing_page="https://www.mainlinehealth.org/about",
        )
        for source_id, name, url in (
            (
                "main-line-lankenau",
                "Lankenau Medical Center Fact Sheet",
                "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/lmc-fact-sheet.pdf",
            ),
            (
                "main-line-bryn-mawr",
                "Bryn Mawr Hospital Fact Sheet",
                "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/bmh-fact-sheet.pdf",
            ),
            (
                "main-line-paoli",
                "Paoli Hospital Fact Sheet",
                "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/ph-fact-sheet.pdf",
            ),
            (
                "main-line-riddle",
                "Riddle Hospital Fact Sheet",
                "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/rh-fact-sheet.pdf",
            ),
        )
    ],
    _source(
        "cms-pos-q1-2026",
        "CMS Provider of Services File",
        "https://data.cms.gov/sites/default/files/2026-04/8ff9bcf4-032e-4a6f-b1c1-d8f1c2e96885/Hospital_and_other.DATA.Q1_2026.csv",
        "Q1 2026",
        "text/csv",
        "csv",
        landing_page="https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-quality-improvement-and-evaluation-system",
        public_domain=True,
    ),
    _source(
        "ahrq-compendium-linkage-2023",
        "AHRQ Compendium of U.S. Health Systems Hospital Linkage",
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
        "2023 dated linkage crosscheck",
        "text/csv",
        "csv",
        landing_page="https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
        public_domain=True,
        encoding="cp1252",
    ),
    _source(
        "cms-hgi-current",
        "CMS Hospital General Information",
        "https://data.cms.gov/provider-data/sites/default/files/resources/893c372430d9d71a1c52737d01239d47_1777413958/Hospital_General_Information.csv",
        "current release retrieved at acquisition cutoff",
        "text/csv",
        "csv",
        landing_page="https://data.cms.gov/provider-data/dataset/xubh-q36u",
        public_domain=True,
    ),
    _source(
        "cms-hcris-2023-final",
        "CMS Hospital Provider Cost Report PUF",
        "https://data.cms.gov/sites/default/files/2026-01/3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv",
        "2023 final cost-report public use file",
        "text/csv",
        "csv",
        landing_page="https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
        public_domain=True,
    ),
    _source(
        "pa-hospital-report-2024-1a",
        "Pennsylvania Hospital Report 1-A",
        "https://www.pa.gov/content/dam/copapwp-pagov/en/health/documents/topics/healthstatistics/healthfacilities/hospitalreports/documents/hospital_report_2024_1a.xlsx",
        "2024 calendar year; general acute hospitals",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xlsx",
        landing_page="https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports",
        public_domain=True,
        header_row=7,
    ),
    _source(
        "pa-hospital-report-2024-1b",
        "Pennsylvania Hospital Report 1-B",
        "https://www.pa.gov/content/dam/copapwp-pagov/en/health/documents/topics/healthstatistics/healthfacilities/hospitalreports/documents/hospital_report_2024_1b.xlsx",
        "2024 calendar year; specialty and federal hospitals",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xlsx",
        landing_page="https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports",
        public_domain=True,
        header_row=7,
    ),
    _source(
        "nj-acute-care-current",
        "New Jersey Licensed Acute-Care Facilities",
        "https://healthapps.nj.gov/Facilities/documents2/All_Acute.xlsx",
        "current workbook retrieved at acquisition cutoff",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xlsx",
        landing_page="https://healthapps.nj.gov/Facilities/acSearch.aspx",
        public_domain=True,
    ),
    _source(
        "de-hospitals-index",
        "Delaware Licensed Hospitals",
        "https://dhss.delaware.gov/dhcq/dhcq/hospitals/",
        "current index retrieved at acquisition cutoff",
        "text/html",
        "html",
        public_domain=True,
    ),
    _source(
        "md-licensed-acute-beds-fy2026",
        "Maryland Licensed Acute Care Beds by Hospital and Service",
        "https://mhcc.maryland.gov/sites/default/files/reports/licensed_acute_care_beds_fy2026.pdf",
        "FY26; effective 2025-07-01",
        "application/pdf",
        "pdf",
        landing_page="https://mhcc.maryland.gov/hospital",
        public_domain=True,
    ),
]


JEFFERSON_NAMES = [
    "Jefferson Abington Hospital",
    "Jefferson Bucks Hospital",
    "Jefferson Cherry Hill Hospital",
    "Jefferson Einstein Montgomery Hospital",
    "Jefferson Einstein Philadelphia Hospital",
    "Jefferson Frankford Hospital",
    "Jefferson Hospital for Neuroscience",
    "Jefferson Lansdale Hospital",
    "Jefferson Methodist Hospital",
    "Jefferson Moss-Magee Rehabilitation - Center City",
    "Jefferson Moss-Magee Rehabilitation - Elkins Park",
    "Jefferson Stratford Hospital",
    "Jefferson Torresdale Hospital",
    "Jefferson Washington Township Hospital",
    "Lehigh Valley Hospital - Carbon",
    "Lehigh Valley Hospital - Cedar Crest",
    "Lehigh Valley Hospital - 1503 N. Cedar Crest",
    "Lehigh Valley Hospital - Dickson City",
    "Lehigh Valley Hospital - Gilbertsville",
    "Lehigh Valley Hospital - Hazleton",
    "Lehigh Valley Hospital - Hecktown Oaks",
    "Lehigh Valley Hospital - Highland Avenue",
    "Lehigh Valley Hospital - Macungie",
    "Lehigh Valley Hospital - Muhlenberg",
    "Lehigh Valley Hospital - Pocono",
    "Lehigh Valley Hospital - Schuylkill E. Norwegian St.",
    "Lehigh Valley Hospital - Schuylkill S. Jackson St.",
    "Lehigh Valley Hospital - 17th Street",
    "Lehigh Valley Hospital - Tilghman",
    "Lehigh Valley Reilly Children's Hospital",
    "Physicians Care Surgical Hospital",
    "Rothman Orthopaedic Specialty Hospital",
    "Thomas Jefferson University Hospital",
]


def _slug(value: str) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", value.casefold()))


CANDIDATES = [
    Candidate(
        "christianacare",
        "christiana-hospital",
        "Christiana Hospital",
        "christianacare-about",
        entity_type="campus",
        state_license_id="HSPTL-002",
        missingness="blocked_source_conflict",
        missingness_reason="CMS CCN 080001 combines ChristianaCare reporting entities; a campus-specific declared basis was not safely separable.",
    ),
    Candidate(
        "christianacare",
        "wilmington-hospital",
        "Wilmington Hospital",
        "christianacare-about",
        entity_type="campus",
        state_license_id="HSPTL-001",
        missingness="blocked_source_conflict",
        missingness_reason="CMS CCN 080001 combines ChristianaCare reporting entities; a campus-specific declared basis was not safely separable.",
    ),
    Candidate(
        "christianacare",
        "union-hospital",
        "Union Hospital",
        "christianacare-union",
        ccn="210032",
        missingness="blocked_source_conflict",
        missingness_reason="Current official, CMS POS, and HCRIS values use differing periods or bed bases; no common declared basis was selected.",
    ),
    Candidate("christianacare", "west-grove", "ChristianaCare Hospital, West Grove", "christianacare-west-grove"),
    *[
        Candidate(
            "jefferson-health",
            _slug(name),
            name,
            "jefferson-enterprise-2025",
            missingness="unavailable_public" if "1503 N. Cedar Crest" in name else "not_yet_researched",
            missingness_reason=(
                "The official enterprise roster and PA/CMS public source families were searched, but no separate current bed basis was published for this named location."
                if "1503 N. Cedar Crest" in name
                else "The March 2025 enterprise roster identifies the candidate; current state/CMS facility crosswalk extraction remains incomplete."
            ),
        )
        for name in JEFFERSON_NAMES
    ],
    Candidate(
        "temple-health",
        "temple-university-hospital",
        "Temple University Hospital",
        "temple-locations",
        entity_type="campus",
        missingness="blocked_source_conflict",
        missingness_reason="CMS CCN 390027 and official campus reporting do not expose a safely separable Main-versus-Episcopal declared basis.",
    ),
    Candidate(
        "temple-health",
        "episcopal-campus",
        "Temple University Hospital - Episcopal Campus",
        "temple-episcopal",
        entity_type="campus",
        missingness="blocked_source_conflict",
        missingness_reason="The official campus value cannot be reconciled to the shared CMS reporting entity without allocation.",
    ),
    Candidate(
        "temple-health",
        "jeanes-campus",
        "Temple University Hospital - Jeanes Campus",
        "temple-locations",
        ccn="390080",
        source_name="Jeanes Campus",
    ),
    Candidate("temple-health", "fox-chase", "Fox Chase Cancer Center", "temple-locations", ccn="390196"),
    Candidate(
        "temple-health",
        "chestnut-hill",
        "Temple Health - Chestnut Hill Hospital",
        "temple-chestnut-hill",
        disposition="unresolved",
        ccn="390026",
        missingness="blocked_source_conflict",
        missingness_reason="Temple's alliance language, joint ownership, and differing official/CMS/HCRIS bases require Toolkit adjudication.",
    ),
    Candidate(
        "temple-health",
        "northeastern-campus",
        "Temple University Hospital - Northeastern Campus",
        "sepa-chna-2025",
        disposition="excluded",
        entity_type="facility",
        missingness="not_applicable",
        missingness_reason="The current Temple locations source treats Northeastern as a multispecialty facility rather than a separate hospital.",
    ),
    Candidate(
        "penn-medicine", "hup", "Hospital of the University of Pennsylvania", "penn-six-hospitals-2024", ccn="390111"
    ),
    Candidate(
        "penn-medicine",
        "penn-presbyterian",
        "Penn Presbyterian Medical Center",
        "penn-six-hospitals-2024",
        ccn="390223",
    ),
    Candidate("penn-medicine", "chester-county", "Chester County Hospital", "penn-six-hospitals-2024", ccn="390179"),
    Candidate(
        "penn-medicine",
        "lancaster-general",
        "Lancaster General Hospital",
        "penn-six-hospitals-2024",
        ccn="390100",
        source_name="Lancaster General Health",
    ),
    Candidate(
        "penn-medicine",
        "princeton",
        "Penn Medicine Princeton Medical Center",
        "penn-six-hospitals-2024",
        ccn="310010",
        source_name="Princeton Health",
    ),
    Candidate(
        "penn-medicine", "pennsylvania-hospital", "Pennsylvania Hospital", "penn-six-hospitals-2024", ccn="390226"
    ),
    Candidate(
        "penn-medicine",
        "hup-cedar",
        "Hospital of the University of Pennsylvania - Cedar Avenue",
        "sepa-chna-2025",
        disposition="unresolved",
        entity_type="campus",
        missingness="blocked_source_conflict",
        missingness_reason="The location is publicly named as a hospital campus but described as a remote HUP location; no separate CCN/bed allocation was selected.",
    ),
    Candidate(
        "penn-medicine",
        "good-shepherd",
        "Good Shepherd Penn Partners Rehabilitation",
        "cms-hcris-2023-final",
        disposition="unresolved",
        entity_type="facility",
        ccn="392050",
        source_name="Good Shepherd Penn Partners",
        missingness="unavailable_public",
        missingness_reason="The current Penn enterprise page was searched, while HCRIS/POS identify this reporting entity; current ownership and product inclusion remain unresolved.",
    ),
    Candidate(
        "penn-medicine",
        "lancaster-behavioral",
        "Lancaster Behavioral Health Hospital",
        "cms-hcris-2023-final",
        disposition="unresolved",
        ccn="394055",
    ),
    Candidate(
        "penn-medicine",
        "princeton-house",
        "Princeton House Behavioral Health",
        "penn-princeton-house-2024",
        disposition="unresolved",
        entity_type="facility",
    ),
    Candidate(
        "cooper-university-health-care",
        "camden",
        "Cooper University Hospital",
        "cooper-about",
        ccn="310014",
        state_license_id="10402",
    ),
    Candidate(
        "cooper-university-health-care",
        "cape-regional",
        "Cooper University Hospital Cape Regional",
        "cooper-about",
        ccn="310011",
        state_license_id="10501",
    ),
    Candidate(
        "cooper-university-health-care",
        "md-anderson",
        "MD Anderson Cancer Center at Cooper",
        "cooper-about",
        disposition="excluded",
        entity_type="facility",
        missingness="not_applicable",
        missingness_reason="The current official source lists MD Anderson under 'also home to,' not in its hospital list.",
    ),
    Candidate(
        "cooper-university-health-care",
        "childrens-regional",
        "Children's Regional Hospital at Cooper",
        "cooper-about",
        disposition="unresolved",
        entity_type="facility",
        missingness="blocked_source_conflict",
        missingness_reason="The current official page calls Children's a third hospital, while its Camden co-location and reporting relationship prevent a separate one-to-one bed basis without adjudication.",
    ),
    Candidate("main-line-health", "lankenau", "Lankenau Medical Center", "main-line-about", ccn="390195"),
    Candidate("main-line-health", "bryn-mawr", "Bryn Mawr Hospital", "main-line-about", ccn="390139"),
    Candidate("main-line-health", "paoli", "Paoli Hospital", "main-line-about", ccn="390153"),
    Candidate("main-line-health", "riddle", "Riddle Hospital", "main-line-about", ccn="390222"),
    Candidate(
        "main-line-health",
        "bryn-mawr-rehab",
        "Bryn Mawr Rehabilitation Hospital",
        "main-line-about",
        ccn="393025",
        source_name="Bryn Mawr Rehab Hospital",
        missingness="not_yet_researched",
        missingness_reason="A one-to-one CMS value exists, but a current source-defined licensed/approved rehabilitation basis still requires state extraction; no residual was calculated.",
    ),
    Candidate(
        "main-line-health",
        "mirmont",
        "Mirmont Treatment Center",
        "main-line-about",
        disposition="excluded",
        entity_type="facility",
        missingness="not_applicable",
        missingness_reason="The official system page identifies a treatment center, not a separate hospital in the five-hospital roster.",
    ),
]

STATE_LICENSE_BY_NAME = {
    "Jefferson Cherry Hill Hospital": "10401",
    "Jefferson Stratford Hospital": "10403",
    "Jefferson Washington Township Hospital": "10802",
}

REPORTING_ENTITIES = (
    EntitySpec(
        entity_id="data-mcp:facility:ccn:080001",
        canonical_name="ChristianaCare combined CMS reporting entity",
        entity_type="facility",
        system_slug="christianacare",
        ccn="080001",
        aliases=["CHRISTIANA CARE HEALTH SYSTEM"],
        owner_entity_id=SYSTEM_IDS["christianacare"],
        identity_conflicts=[{"type": "shared_ccn_and_campus_allocation", "status": "open"}],
    ),
    EntitySpec(
        entity_id="data-mcp:facility:ccn:390027",
        canonical_name="Temple University Hospital combined reporting entity",
        entity_type="facility",
        system_slug="temple-health",
        ccn="390027",
        aliases=["TEMPLE UNIVERSITY HOSPITAL"],
        owner_entity_id=SYSTEM_IDS["temple-health"],
        identity_conflicts=[{"type": "shared_ccn_and_campus_allocation", "status": "open"}],
    ),
    EntitySpec(
        entity_id="data-mcp:facility:state:pa:jefferson-health-northeast",
        canonical_name="Jefferson Health Northeast state reporting entity",
        entity_type="facility",
        system_slug="jefferson-health",
        aliases=["JEFFERSON HEALTH NORTHEAST"],
        owner_entity_id=SYSTEM_IDS["jefferson-health"],
        identity_conflicts=[{"type": "shared_state_reporting_entity", "status": "open"}],
    ),
)


SYSTEM_SOURCE = {
    "christianacare": "christianacare-about",
    "jefferson-health": "jefferson-enterprise-2025",
    "temple-health": "temple-locations",
    "penn-medicine": "penn-six-hospitals-2024",
    "cooper-university-health-care": "cooper-about",
    "main-line-health": "main-line-about",
}

SYSTEM_NAMES = {
    "christianacare": "ChristianaCare",
    "jefferson-health": "Jefferson Health",
    "temple-health": "Temple Health",
    "penn-medicine": "Penn Medicine",
    "cooper-university-health-care": "Cooper University Health Care",
    "main-line-health": "Main Line Health",
}


def _flex(value: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9]+", value)
    return r"\W+".join(r"\s*".join(re.escape(character) for character in token) for token in tokens)


def _fact(
    fact_id: str,
    entity_id: str,
    measure_id: str,
    source_id: str,
    value: str,
    pattern_name: str,
    *,
    denominator_scope: str,
) -> FactSpec:
    return FactSpec(
        fact_id=fact_id,
        entity_id=entity_id,
        measure_id=measure_id,
        value_type="string",
        unit="source-local value",
        period_label="source period recorded in receipt",
        denominator_scope=denominator_scope,
        source_id=source_id,
        row_locator=f"source text matching {pattern_name}",
        match_basis="Exact source-local name or reviewed official alias; no automatic entity merge.",
        confidence="high for source-local assertion; Toolkit adjudication not performed",
        extraction_pattern=_flex(pattern_name),
        literal_value=value,
    )


def _bed_pattern_fact(
    fact_id: str,
    entity_id: str,
    source_id: str,
    measure_id: str,
    pattern: str,
    period: str,
    basis: str,
    row_locator: str,
) -> FactSpec:
    return FactSpec(
        fact_id=fact_id,
        entity_id=entity_id,
        measure_id=measure_id,
        value_type="integer",
        unit="beds",
        period_label=period,
        denominator_scope=f"facility-level; basis={basis}; raw source label preserved in row_locator",
        source_id=source_id,
        row_locator=row_locator,
        match_basis="Exact official facility name and source-local bed label.",
        confidence="high for source transcription; comparability not adjudicated",
        caveat="Source-local basis only; do not combine with licensed, certified, staffed, available, or acute beds.",
        extraction_pattern=pattern,
    )


OFFICIAL_BEDS = [
    OfficialBedFact(
        "christiana-newark-licensed-906",
        "data-mcp:facility:official:christianacare:christiana-hospital",
        "christianacare-about",
        "bed_count.licensed",
        r"licensed\W+beds\W+Newark\W+Campus\W+(?P<value>906)",
        "2023",
        "licensed",
        "2023 Statistics: Licensed beds Newark Campus: 906",
    ),
    OfficialBedFact(
        "christiana-wilmington-licensed-321",
        "data-mcp:facility:official:christianacare:wilmington-hospital",
        "christianacare-about",
        "bed_count.licensed",
        r"Wilmington\W+Campus\W+(?P<value>321)",
        "2023",
        "licensed",
        "2023 Statistics: Licensed beds Wilmington Campus: 321",
    ),
    OfficialBedFact(
        "christiana-cecil-licensed-109",
        "data-mcp:facility:ccn:210032",
        "christianacare-about",
        "bed_count.licensed",
        r"Cecil\W+County\W+Campus\W+(?P<value>109)",
        "2023",
        "licensed",
        "2023 Statistics: Licensed beds Cecil County Campus: 109",
    ),
    OfficialBedFact(
        "union-official-103",
        "data-mcp:facility:ccn:210032",
        "christianacare-union",
        "bed_count.official_unspecified",
        r"(?P<value>103)\W+bed\W+facility",
        "current page",
        "official unspecified",
        "Union Hospital bullet: 103-bed facility",
    ),
    OfficialBedFact(
        "west-grove-official-10",
        "data-mcp:facility:official:christianacare:west-grove",
        "christianacare-west-grove",
        "bed_count.official_unspecified",
        r"(?P<value>10)\W+inpatient\W+beds",
        "2025-08-13",
        "official inpatient, otherwise unspecified",
        "opening announcement: 10 inpatient beds",
    ),
    OfficialBedFact(
        "episcopal-official-139",
        "data-mcp:facility:official:temple-health:episcopal-campus",
        "temple-episcopal",
        "bed_count.licensed",
        r"(?P<value>139)\W+licensed\W+beds",
        "current page",
        "licensed",
        "About section: 139 licensed beds",
    ),
    OfficialBedFact(
        "chestnut-official-148",
        "data-mcp:facility:ccn:390026",
        "temple-chestnut-hill",
        "bed_count.official_unspecified",
        r"(?P<value>148)\W+bed",
        "current page",
        "official unspecified",
        "About section: 148-bed hospital",
    ),
    OfficialBedFact(
        "cooper-camden-licensed-663",
        "data-mcp:facility:ccn:310014",
        "cooper-camden-2025",
        "bed_count.licensed",
        r"(?P<value>663)\W+licensed\W+beds",
        "Q1 2025",
        "licensed",
        "Fast Facts: 663 licensed beds",
    ),
    OfficialBedFact(
        "cooper-cape-licensed-242",
        "data-mcp:facility:ccn:310011",
        "cooper-cape-2025",
        "bed_count.licensed",
        r"(?P<value>242)\W+licensed\W+beds",
        "Q1 2025",
        "licensed",
        "Fast Facts: 242 licensed beds",
    ),
    OfficialBedFact(
        "lankenau-official-370",
        "data-mcp:facility:ccn:390195",
        "main-line-lankenau",
        "bed_count.licensed",
        r"licensed\W+beds\W+(?P<value>370)",
        "FY25",
        "licensed",
        "Hospital statistics: LICENSED BEDS 370",
    ),
    OfficialBedFact(
        "bryn-mawr-official-284",
        "data-mcp:facility:ccn:390139",
        "main-line-bryn-mawr",
        "bed_count.licensed",
        r"licensed\W+beds\W+(?P<value>284)",
        "FY25",
        "licensed",
        "Hospital statistics: LICENSED BEDS 284",
    ),
    OfficialBedFact(
        "paoli-official-261",
        "data-mcp:facility:ccn:390153",
        "main-line-paoli",
        "bed_count.licensed",
        r"licensed\W+beds\W+(?P<value>261)",
        "FY25",
        "licensed",
        "Hospital statistics: LICENSED BEDS 261",
    ),
    OfficialBedFact(
        "riddle-official-243",
        "data-mcp:facility:ccn:390222",
        "main-line-riddle",
        "bed_count.licensed",
        r"licensed\W+beds\W+(?P<value>243)",
        "FY25",
        "licensed",
        "Hospital statistics: LICENSED BEDS 243",
    ),
]


FEDERAL_CCN_CANDIDATES = {candidate.ccn: candidate for candidate in CANDIDATES if candidate.ccn}
HGI_CCNS = {
    "210032",
    "310010",
    "310011",
    "310014",
    "390026",
    "390100",
    "390111",
    "390139",
    "390153",
    "390179",
    "390195",
    "390222",
    "390223",
    "390226",
    "394055",
}

CANDIDATE_BY_NAME = {candidate.name: candidate for candidate in CANDIDATES}

STATE_BED_ROWS = (
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Abington Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "ABINGTON MEMORIAL HOSPITAL",
        "FACILITY NAME",
        "ABINGTON MEMORIAL HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Einstein Montgomery Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "EINSTEIN MEDICAL CENTER MONTGOMERY",
        "FACILITY NAME",
        "EINSTEIN MEDICAL CENTER MONTGOMERY",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Einstein Philadelphia Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "ALBERT EINSTEIN MEDICAL CENTER",
        "FACILITY NAME",
        "ALBERT EINSTEIN MEDICAL CENTER",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Lansdale Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "LANSDALE HOSPITAL",
        "FACILITY NAME",
        "LANSDALE HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Thomas Jefferson University Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "THOMAS JEFFERSON UNIVERSITY HOSPITAL",
        "FACILITY NAME",
        "THOMAS JEFFERSON UNIVERSITY HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        "data-mcp:facility:state:pa:jefferson-health-northeast",
        "pa-hospital-report-2024-1a",
        "JEFFERSON HEALTH NORTHEAST",
        "FACILITY NAME",
        "JEFFERSON HEALTH NORTHEAST",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        "data-mcp:facility:ccn:390027",
        "pa-hospital-report-2024-1a",
        "TEMPLE UNIVERSITY HOSPITAL",
        "FACILITY NAME",
        "TEMPLE UNIVERSITY HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Temple Health - Chestnut Hill Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "TEMPLE HEALTH CHESTNUT HILL HOSPITAL",
        "FACILITY NAME",
        "TEMPLE HEALTH CHESTNUT HILL HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Hospital of the University of Pennsylvania"].entity_id,
        "pa-hospital-report-2024-1a",
        "HOSPITAL OF THE UNIVERSITY OF PENNSYLVANIA",
        "FACILITY NAME",
        "HOSPITAL OF THE UNIVERSITY OF PENNSYLVANIA",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Penn Presbyterian Medical Center"].entity_id,
        "pa-hospital-report-2024-1a",
        "PENN PRESBYTERIAN MEDICAL CENTER",
        "FACILITY NAME",
        "PENN PRESBYTERIAN MEDICAL CENTER",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Chester County Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "CHESTER COUNTY HOSPITAL",
        "FACILITY NAME",
        "CHESTER COUNTY HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Lancaster General Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "LANCASTER GENERAL HOSPITAL",
        "FACILITY NAME",
        "LANCASTER GENERAL HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Pennsylvania Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "PENNSYLVANIA HOSPITAL OF THE UNIVERSITY OF PENNSYLVANIA HEALTH SYSTEM",
        "FACILITY NAME",
        "PENNSYLVANIA HOSPITAL OF THE UNIVERSITY OF PENNSYLVANIA HEALTH SYSTEM",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Lankenau Medical Center"].entity_id,
        "pa-hospital-report-2024-1a",
        "MAIN LINE HOSPITAL LANKENAU MEDICAL CENTER",
        "FACILITY NAME",
        "MAIN LINE HOSPITAL LANKENAU MEDICAL CENTER",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Bryn Mawr Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "MAIN LINE HOSPITAL BRYN MAWR",
        "FACILITY NAME",
        "MAIN LINE HOSPITAL BRYN MAWR",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Paoli Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "MAIN LINE HOSPITAL PAOLI",
        "FACILITY NAME",
        "MAIN LINE HOSPITAL PAOLI",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Riddle Hospital"].entity_id,
        "pa-hospital-report-2024-1a",
        "RIDDLE MEMORIAL HOSPITAL",
        "FACILITY NAME",
        "RIDDLE MEMORIAL HOSPITAL",
        "LICENSED BEDS",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Bryn Mawr Rehabilitation Hospital"].entity_id,
        "pa-hospital-report-2024-1b",
        "MAIN LINE HOSPITAL BRYN MAWR REHABILITATION",
        "FACILITY NAME",
        "MAIN LINE HOSPITAL BRYN MAWR REHABILITATION",
        "LICENSED BEDS**",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Lancaster Behavioral Health Hospital"].entity_id,
        "pa-hospital-report-2024-1b",
        "LANCASTER BEHAVIORAL HEALTH HOSPITAL",
        "FACILITY NAME",
        "LANCASTER BEHAVIORAL HEALTH HOSPITAL",
        "LICENSED BEDS**",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Rothman Orthopaedic Specialty Hospital"].entity_id,
        "pa-hospital-report-2024-1b",
        "ROTHMAN ORTHOPAEDIC SPECIALTY HOSPITAL",
        "FACILITY NAME",
        "ROTHMAN ORTHOPAEDIC SPECIALTY HOSPITAL",
        "LICENSED BEDS**",
        "BEDS SET UP AND STAFFED",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Cherry Hill Hospital"].entity_id,
        "nj-acute-care-current",
        "JEFFERSON CHERRY HILL HOSPITAL (NJ10401)",
        "FacID",
        "NJ10401",
        "Lic_Beds_Slots",
        state_license_field="LIC#",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Stratford Hospital"].entity_id,
        "nj-acute-care-current",
        "JEFFERSON STRATFORD HOSPITAL (NJ10403)",
        "FacID",
        "NJ10403",
        "Lic_Beds_Slots",
        state_license_field="LIC#",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Jefferson Washington Township Hospital"].entity_id,
        "nj-acute-care-current",
        "JEFFERSON WASHINGTON TOWNSHIP HOSPITAL (NJ10802-1)",
        "FacID",
        "NJ10802-1",
        "Lic_Beds_Slots",
        state_license_field="LIC#",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Cooper University Hospital"].entity_id,
        "nj-acute-care-current",
        "COOPER UNIVERSITY HOSPITAL (NJ10402)",
        "FacID",
        "NJ10402",
        "Lic_Beds_Slots",
        state_license_field="LIC#",
    ),
    StateBedRow(
        CANDIDATE_BY_NAME["Cooper University Hospital Cape Regional"].entity_id,
        "nj-acute-care-current",
        "COOPER UNIVERSITY HOSPITAL CAPE REGIONAL, INC. (NJ10501)",
        "FacID",
        "NJ10501",
        "Lic_Beds_Slots",
        state_license_field="LIC#",
    ),
)

HCRIS_PERIODS = {
    "080001": ("07/01/2023", "06/30/2024"),
    "210032": ("07/01/2023", "06/30/2024"),
    "310010": ("07/01/2023", "06/30/2024"),
    "310011": ("01/01/2023", "12/31/2023"),
    "310014": ("01/01/2023", "12/31/2023"),
    "390026": ("09/02/2023", "06/30/2024"),
    "390027": ("07/01/2023", "06/30/2024"),
    "390100": ("07/01/2023", "06/30/2024"),
    "390111": ("07/01/2023", "06/30/2024"),
    "390139": ("07/01/2023", "06/30/2024"),
    "390153": ("07/01/2023", "06/30/2024"),
    "390179": ("07/01/2023", "06/30/2024"),
    "390195": ("07/01/2023", "06/30/2024"),
    "390196": ("07/01/2023", "06/30/2024"),
    "390222": ("07/01/2023", "06/30/2024"),
    "390223": ("07/01/2023", "06/30/2024"),
    "390226": ("07/01/2023", "06/30/2024"),
    "392050": ("07/01/2023", "06/30/2024"),
    "393025": ("07/01/2023", "06/30/2024"),
    "394055": ("07/01/2023", "06/30/2024"),
}

AHRQ_LINKS = {
    "210032": ("HSI00000218", "124"),
    "310010": ("HSI00000820", "206"),
    "310014": ("HSI00001079", "578"),
    "390026": ("HSI00001065", "128"),
    "390080": ("HSI00001065", ""),
    "390100": ("HSI00000820", "620"),
    "390111": ("HSI00000820", "1051"),
    "390139": ("HSI00000608", "244"),
    "390153": ("HSI00000608", "249"),
    "390179": ("HSI00000820", "299"),
    "390195": ("HSI00000608", "370"),
    "390196": ("HSI00001065", "100"),
    "390222": ("HSI00000608", "186"),
    "390223": ("HSI00000820", "328"),
    "390226": ("HSI00000820", "425"),
    "392050": ("HSI00000820", "18"),
    "393025": ("HSI00000608", "148"),
    "394055": ("HSI00000820", "126"),
}


def acquisition_spec() -> AcquisitionSpec:
    entities = [
        EntitySpec(
            entity_id=SYSTEM_IDS[slug],
            canonical_name=name,
            entity_type="health_system",
            system_slug=slug,
            aliases=[name],
        )
        for slug, name in SYSTEM_NAMES.items()
    ]
    for candidate in CANDIDATES:
        conflicts = []
        if candidate.missingness == "blocked_source_conflict":
            conflicts.append({"type": "identity_or_basis", "status": "open"})
        entities.append(
            EntitySpec(
                entity_id=candidate.entity_id,
                canonical_name=candidate.name,
                entity_type=candidate.entity_type,
                system_slug=candidate.system_slug,
                ccn=candidate.ccn,
                state_license_id=candidate.state_license_id or STATE_LICENSE_BY_NAME.get(candidate.name, ""),
                aliases=[candidate.source_name or candidate.name],
                owner_entity_id=SYSTEM_IDS[candidate.system_slug],
                identity_conflicts=conflicts,
            )
        )
    entities.extend(REPORTING_ENTITIES)

    facts: list[FactSpec] = []
    for slug, entity_id in SYSTEM_IDS.items():
        facts.append(
            _fact(
                f"system-identity:{slug}",
                entity_id,
                "system_identity",
                SYSTEM_SOURCE[slug],
                SYSTEM_NAMES[slug],
                SYSTEM_NAMES[slug],
                denominator_scope="public enterprise identity; no product identity adjudication",
            )
        )
    for candidate in CANDIDATES:
        if candidate.source_id == "cms-hcris-2023-final":
            facts.append(
                FactSpec(
                    fact_id=f"roster:{candidate.system_slug}:{candidate.slug}",
                    entity_id=candidate.entity_id,
                    measure_id="hospital_roster_disposition",
                    value_type="string",
                    unit="source-local value",
                    period_label="2023 final HCRIS PUF",
                    denominator_scope="source-local facility candidate; not a Toolkit inclusion decision",
                    source_id=candidate.source_id,
                    row_locator=f"Provider CCN={candidate.ccn}; field=Hospital Name",
                    match_basis="Exact CCN; ownership remains unresolved.",
                    confidence="high for facility existence; ownership not adjudicated",
                    table_match={"Provider CCN": candidate.ccn},
                    table_value_field="Hospital Name",
                    literal_value=candidate.disposition,
                )
            )
        else:
            facts.append(
                _fact(
                    f"roster:{candidate.system_slug}:{candidate.slug}",
                    candidate.entity_id,
                    "hospital_roster_disposition",
                    candidate.source_id,
                    candidate.disposition,
                    candidate.source_name or candidate.name,
                    denominator_scope="source-local enterprise roster candidate; not a Toolkit inclusion decision",
                )
            )

    official_bed_entities = set()
    for row in OFFICIAL_BEDS:
        facts.append(
            _bed_pattern_fact(
                row.fact_id,
                row.entity_id,
                row.source_id,
                row.measure_id,
                row.extraction_pattern,
                row.period,
                row.basis,
                row.row_locator,
            )
        )
        official_bed_entities.add(row.entity_id)

    for fact_id, entity_id, pattern, period, row_locator in (
        (
            "de-license:christiana-newark",
            CANDIDATE_BY_NAME["Christiana Hospital"].entity_id,
            r"ChristianaCare\W+Newark.*?License\W+ID\W+(?P<value>HSPTL-002)",
            "current license retrieved at acquisition cutoff",
            "ChristianaCare (Newark); License ID HSPTL-002",
        ),
        (
            "de-license:wilmington",
            CANDIDATE_BY_NAME["Wilmington Hospital"].entity_id,
            r"ChristianaCare\W+Wilmington.*?License\W+ID\W+(?P<value>HSPTL-001)",
            "current license retrieved at acquisition cutoff",
            "ChristianaCare (Wilmington); License ID HSPTL-001",
        ),
    ):
        facts.append(
            FactSpec(
                fact_id=fact_id,
                entity_id=entity_id,
                measure_id="facility_identity.state_license_id",
                value_type="string",
                unit="Delaware hospital license ID",
                period_label=period,
                denominator_scope="one Delaware licensed hospital campus",
                source_id="de-hospitals-index",
                row_locator=row_locator,
                match_basis="Exact state license ID and named campus; no address-only merge.",
                confidence="high for current state license identity",
                extraction_pattern=pattern,
            )
        )
    for fact in (
        _bed_pattern_fact(
            "de-bed:christiana-newark:licensed",
            CANDIDATE_BY_NAME["Christiana Hospital"].entity_id,
            "de-hospitals-index",
            "bed_count.licensed",
            r"ChristianaCare\W+Newark.*?Licensed\W+Beds\W+(?P<value>1,039)",
            "license expiration 2026-11-30",
            "Delaware Licensed Beds",
            "ChristianaCare (Newark); Licensed Beds 1,039; License ID HSPTL-002",
        ),
        _bed_pattern_fact(
            "de-bed:wilmington:licensed",
            CANDIDATE_BY_NAME["Wilmington Hospital"].entity_id,
            "de-hospitals-index",
            "bed_count.licensed",
            r"ChristianaCare\W+Wilmington.*?Licensed\W+Beds\W+(?P<value>321)",
            "license expiration 2026-11-30",
            "Delaware Licensed Beds",
            "ChristianaCare (Wilmington); Licensed Beds 321; License ID HSPTL-001",
        ),
        _bed_pattern_fact(
            "md-bed:union:licensed-fy2026",
            CANDIDATE_BY_NAME["Union Hospital"].entity_id,
            "md-licensed-acute-beds-fy2026",
            "bed_count.licensed",
            r"Cecil\W+County\W+ChristianaCare\W+Union\W+Hospital\W+81\W+4\W+2\W+12\W+(?P<value>99)",
            "FY26; effective 2025-07-01",
            "Maryland licensed acute care total",
            "Cecil County; ChristianaCare Union Hospital; Total 99",
        ),
    ):
        facts.append(fact)
        official_bed_entities.add(fact.entity_id)

    for row in STATE_BED_ROWS:
        match = {row.match_field: row.match_value}
        state_prefix = row.source_id.split("-", 1)[0]
        for measure_id, field, basis in (
            ("bed_count.licensed", row.licensed_field, f"{state_prefix.upper()} source field {row.licensed_field}"),
            (
                "bed_count.setup_and_staffed",
                row.staffed_field,
                f"{state_prefix.upper()} source field {row.staffed_field}",
            ),
        ):
            if field is None:
                continue
            facts.append(
                FactSpec(
                    fact_id=f"state-bed:{row.source_id}:{_slug(row.source_name)}:{field.casefold().replace(' ', '-')}",
                    entity_id=row.entity_id,
                    measure_id=measure_id,
                    value_type="integer",
                    unit="beds",
                    period_label="2024 calendar year"
                    if row.source_id.startswith("pa-")
                    else "current license at acquisition cutoff",
                    denominator_scope=f"facility-level; basis={basis}; raw source field={field}",
                    source_id=row.source_id,
                    row_locator=f"{row.match_field}={row.match_value}; field={field}",
                    match_basis="Exact state row identifier or exact state-reported facility name; no fuzzy merge.",
                    confidence="high for state row transcription; ownership and cross-basis comparability not adjudicated",
                    caveat="State source-local basis only; no rollup or substitution across bed bases.",
                    table_match=match,
                    table_value_field=field,
                )
            )
        if row.state_license_field is not None:
            facts.append(
                FactSpec(
                    fact_id=f"state-license:{row.source_id}:{_slug(row.source_name)}",
                    entity_id=row.entity_id,
                    measure_id="facility_identity.state_license_id",
                    value_type="string",
                    unit="state license ID",
                    period_label="current license at acquisition cutoff",
                    denominator_scope="one state-licensed acute-care facility",
                    source_id=row.source_id,
                    row_locator=f"{row.match_field}={row.match_value}; field={row.state_license_field}",
                    match_basis="Exact state facility identifier; no name-only entity merge.",
                    confidence="high for state license identity",
                    table_match=match,
                    table_value_field=row.state_license_field,
                )
            )
        official_bed_entities.add(row.entity_id)

    federal_entity_ids = {ccn: candidate.entity_id for ccn, candidate in FEDERAL_CCN_CANDIDATES.items()}
    federal_entity_ids.update({"080001": "data-mcp:facility:ccn:080001", "390027": "data-mcp:facility:ccn:390027"})
    for ccn, entity_id in sorted(federal_entity_ids.items()):
        if ccn in HGI_CCNS:
            facts.append(
                FactSpec(
                    fact_id=f"cms-hgi-identity:{ccn}",
                    entity_id=entity_id,
                    measure_id="facility_identity.cms_ccn",
                    value_type="string",
                    unit="CCN",
                    period_label="current CMS HGI release at acquisition cutoff",
                    denominator_scope="one-to-one CMS facility identity candidate",
                    source_id="cms-hgi-current",
                    row_locator=f"Facility ID={ccn}; field=Facility ID",
                    match_basis="Exact six-character CCN; names retained as aliases only.",
                    confidence="high for CMS identity, not product ownership",
                    table_match={"Facility ID": ccn},
                    table_value_field="Facility ID",
                )
            )
        for measure_id, field, basis in (
            ("bed_count.cms_pos", "BED_CNT", "CMS POS BED_CNT"),
            ("bed_count.cms_pos_certified", "CRTFD_BED_CNT", "CMS POS CRTFD_BED_CNT"),
        ):
            facts.append(
                FactSpec(
                    fact_id=f"cms-pos:{ccn}:{field.casefold()}",
                    entity_id=entity_id,
                    measure_id=measure_id,
                    value_type="integer",
                    unit="beds",
                    period_label="Q1 2026",
                    denominator_scope=f"facility-level; basis={basis}; raw source field={field}",
                    source_id="cms-pos-q1-2026",
                    row_locator=f"PRVDR_NUM={ccn}; field={field}",
                    match_basis="Exact CCN only; source name is an alias and not an auto-merge key.",
                    confidence="high for exact Q1 2026 CCN row identity",
                    caveat="Q1 2026 POS source-local value; do not recast or combine its bed basis.",
                    table_match={"PRVDR_NUM": ccn},
                    table_value_field=field,
                )
            )
        if ccn in HCRIS_PERIODS:
            period_start, period_end = HCRIS_PERIODS[ccn]
            facts.append(
                FactSpec(
                    fact_id=f"cms-hcris:{ccn}:number-of-beds",
                    entity_id=entity_id,
                    measure_id="bed_count.hcris_period_end_available",
                    value_type="integer",
                    unit="beds",
                    period_label=f"provider fiscal period {period_start} through {period_end}",
                    period_start=f"{period_start[6:10]}-{period_start[:2]}-{period_start[3:5]}",
                    period_end=f"{period_end[6:10]}-{period_end[:2]}-{period_end[3:5]}",
                    denominator_scope="facility-level; basis=HCRIS Number of Beds; raw source field=Number of Beds",
                    source_id="cms-hcris-2023-final",
                    row_locator=f"Provider CCN={ccn}; Fiscal Year Begin Date={period_start}; Fiscal Year End Date={period_end}; field=Number of Beds",
                    match_basis="Exact CCN only.",
                    confidence="high for source row; period and basis differ from POS and official values",
                    caveat="HCRIS reporting period is provider-specific and must not be treated as a current licensed bed value.",
                    table_match={
                        "Provider CCN": ccn,
                        "Fiscal Year Begin Date": period_start,
                        "Fiscal Year End Date": period_end,
                    },
                    table_value_field="Number of Beds",
                )
            )

    for ccn, (health_system_id, beds) in sorted(AHRQ_LINKS.items()):
        candidate = FEDERAL_CCN_CANDIDATES[ccn]
        table_match = {"ccn": ccn, "health_sys_id": health_system_id}
        facts.append(
            FactSpec(
                fact_id=f"ahrq-linkage:{ccn}",
                entity_id=candidate.entity_id,
                measure_id="facility_identity.ahrq_2023_linkage",
                value_type="string",
                unit="AHRQ 2023 health-system linkage ID",
                period_label="2023 dated linkage crosscheck",
                denominator_scope="one AHRQ Compendium hospital linkage row; not current ownership authority",
                source_id="ahrq-compendium-linkage-2023",
                row_locator=f"ccn={ccn}; health_sys_id={health_system_id}; field=health_sys_id",
                match_basis="Exact CCN plus dated AHRQ health-system identifier.",
                confidence="high for dated linkage row; current ownership not inferred",
                caveat="AHRQ 2023 is a dated linkage crosscheck only.",
                table_match=table_match,
                table_value_field="health_sys_id",
            )
        )
        if beds:
            facts.append(
                FactSpec(
                    fact_id=f"ahrq-bed:{ccn}:hos_beds",
                    entity_id=candidate.entity_id,
                    measure_id="bed_count.ahrq_acute",
                    value_type="integer",
                    unit="beds",
                    period_label="2023 dated linkage crosscheck",
                    denominator_scope="facility-level; basis=AHRQ 2023 hos_beds acute; raw source field=hos_beds",
                    source_id="ahrq-compendium-linkage-2023",
                    row_locator=f"ccn={ccn}; health_sys_id={health_system_id}; field=hos_beds",
                    match_basis="Exact CCN plus dated AHRQ health-system identifier.",
                    confidence="high for dated source row; current comparability not inferred",
                    caveat="Dated AHRQ acute-bed crosscheck only; do not substitute for a current licensed or staffed basis.",
                    table_match=table_match,
                    table_value_field="hos_beds",
                )
            )

    bed_entities = official_bed_entities | {candidate.entity_id for candidate in FEDERAL_CCN_CANDIDATES.values()}
    for candidate in CANDIDATES:
        if candidate.entity_id in bed_entities and candidate.missingness != "blocked_source_conflict":
            continue
        facts.append(
            FactSpec(
                fact_id=f"bed-missing:{candidate.system_slug}:{candidate.slug}",
                entity_id=candidate.entity_id,
                measure_id="bed_count.declared",
                value_type="integer",
                unit="beds",
                period_label="acquisition cutoff",
                denominator_scope="facility-level; basis=declared bed basis not established",
                missingness=candidate.missingness,
                missingness_reason=candidate.missingness_reason,
                absence_checks=(
                    [
                        {
                            "source_id": "pa-hospital-report-2024-1a",
                            "table_match": {"FACILITY NAME": candidate.name.upper()},
                        },
                        {
                            "source_id": "pa-hospital-report-2024-1b",
                            "table_match": {"FACILITY NAME": candidate.name.upper()},
                        },
                        {
                            "source_id": "cms-pos-q1-2026",
                            "table_match": {"FAC_NAME": candidate.name.upper()},
                        },
                    ]
                    if candidate.missingness == "unavailable_public"
                    else []
                ),
            )
        )

    conflict_rows = [
        ConflictSpec(
            conflict_id="conflict:union-bed-bases",
            conflict_type="bed_basis_and_period",
            entity_ids=["data-mcp:facility:ccn:210032"],
            fact_ids=[
                "christiana-cecil-licensed-109",
                "union-official-103",
                "cms-pos:210032:bed_cnt",
                "cms-pos:210032:crtfd_bed_cnt",
                "cms-hcris:210032:number-of-beds",
                "md-bed:union:licensed-fy2026",
            ],
            source_ids=[
                "christianacare-about",
                "christianacare-union",
                "cms-pos-q1-2026",
                "cms-hcris-2023-final",
                "md-licensed-acute-beds-fy2026",
            ],
            rationale="Official 2023 licensed 109-bed, current official 103-bed, Maryland FY26 licensed 99-bed, Q1 2026 POS, and HCRIS 124-bed rows have differing labels and periods; none was selected as the common value.",
        ),
        ConflictSpec(
            conflict_id="conflict:christianacare-shared-cms-reporting-entity",
            conflict_type="shared_ccn_and_campus_allocation",
            entity_ids=[
                "data-mcp:facility:official:christianacare:christiana-hospital",
                "data-mcp:facility:official:christianacare:wilmington-hospital",
                "data-mcp:facility:ccn:080001",
            ],
            fact_ids=[
                "christiana-newark-licensed-906",
                "christiana-wilmington-licensed-321",
                "de-bed:christiana-newark:licensed",
                "de-bed:wilmington:licensed",
                "cms-pos:080001:bed_cnt",
                "cms-pos:080001:crtfd_bed_cnt",
                "cms-hcris:080001:number-of-beds",
            ],
            source_ids=[
                "christianacare-about",
                "de-hospitals-index",
                "cms-pos-q1-2026",
                "cms-hcris-2023-final",
            ],
            rationale="Official and Delaware state pages disagree for the Newark campus, while CMS CCN 080001 and HCRIS are combined reporting entities; no federal value was allocated to Newark or Wilmington.",
        ),
        ConflictSpec(
            conflict_id="conflict:temple-shared-cms-reporting-entity",
            conflict_type="shared_ccn_and_campus_allocation",
            entity_ids=[
                CANDIDATE_BY_NAME["Temple University Hospital"].entity_id,
                CANDIDATE_BY_NAME["Temple University Hospital - Episcopal Campus"].entity_id,
                "data-mcp:facility:ccn:390027",
            ],
            fact_ids=[
                "episcopal-official-139",
                "state-bed:pa-hospital-report-2024-1a:temple-university-hospital:licensed-beds",
                "state-bed:pa-hospital-report-2024-1a:temple-university-hospital:beds-set-up-and-staffed",
                "cms-pos:390027:bed_cnt",
                "cms-pos:390027:crtfd_bed_cnt",
                "cms-hcris:390027:number-of-beds",
            ],
            source_ids=[
                "temple-episcopal",
                "pa-hospital-report-2024-1a",
                "cms-pos-q1-2026",
                "cms-hcris-2023-final",
            ],
            rationale="Pennsylvania, POS, and HCRIS report the combined Temple entity while the official source separately labels Episcopal; no shared value was allocated to either campus.",
        ),
        ConflictSpec(
            conflict_id="conflict:chestnut-hill-ownership-and-bases",
            conflict_type="ownership_and_bed_basis",
            entity_ids=["data-mcp:facility:ccn:390026"],
            fact_ids=[
                "chestnut-official-148",
                "state-bed:pa-hospital-report-2024-1a:temple-health-chestnut-hill-hospital:licensed-beds",
                "state-bed:pa-hospital-report-2024-1a:temple-health-chestnut-hill-hospital:beds-set-up-and-staffed",
                "cms-pos:390026:bed_cnt",
                "cms-pos:390026:crtfd_bed_cnt",
                "cms-hcris:390026:number-of-beds",
            ],
            source_ids=[
                "temple-chestnut-hill",
                "pa-hospital-report-2024-1a",
                "cms-pos-q1-2026",
                "cms-hcris-2023-final",
            ],
            rationale="Joint-ownership/alliance language and official, POS-certified, and HCRIS bases require downstream adjudication.",
        ),
        ConflictSpec(
            conflict_id="conflict:cooper-childrens-separate-hospital",
            conflict_type="roster_identity_and_reporting_entity",
            entity_ids=["data-mcp:facility:official:cooper-university-health-care:childrens-regional"],
            fact_ids=["roster:cooper-university-health-care:childrens-regional"],
            source_ids=["cooper-about"],
            rationale="Current official language calls Children's a third hospital, while the governed slice must preserve its co-located/embedded reporting ambiguity rather than count it automatically.",
        ),
    ]
    return AcquisitionSpec(
        bundle_id="scale-roster-bed-basis-all-six-2026-07",
        producer_version="0.4.0",
        systems=list(SYSTEM_IDS.values()),
        market={"scope": "enterprise-wide", "states": ["DE", "MD", "NJ", "PA"]},
        periods=[
            "2023 HCRIS provider periods",
            "2023 AHRQ dated crosscheck",
            "2024 state hospital reports",
            "March 2025 official roster",
            "FY26 Maryland licensed beds",
            "Q1 2026 POS",
            "acquisition cutoff",
        ],
        sources=SOURCES,
        entities=entities,
        facts=facts,
        conflicts=conflict_rows,
    )


__all__ = ["CANDIDATES", "SYSTEM_IDS", "acquisition_spec"]
