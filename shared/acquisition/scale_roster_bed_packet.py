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
    missingness: Missingness = "not_yet_researched"
    missingness_reason: str = "A known primary or regulatory source still requires facility-level extraction."
    source_name: str = ""

    @property
    def entity_id(self) -> str:
        if self.ccn:
            return f"data-mcp:facility:ccn:{self.ccn}"
        return f"data-mcp:facility:official:{self.system_slug}:{self.slug}"


def _source(
    source_id: str,
    name: str,
    url: str,
    period: str,
    media: str,
    parser: Literal["html", "pdf", "csv", "text"],
    *,
    landing_page: str | None = None,
    public_domain: bool = False,
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
        _source(source_id, name, url, "current fact sheet retrieved at acquisition cutoff", "application/pdf", "pdf", landing_page="https://www.mainlinehealth.org/about")
        for source_id, name, url in (
            ("main-line-lankenau", "Lankenau Medical Center Fact Sheet", "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/lmc-fact-sheet.pdf"),
            ("main-line-bryn-mawr", "Bryn Mawr Hospital Fact Sheet", "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/bmh-fact-sheet.pdf"),
            ("main-line-paoli", "Paoli Hospital Fact Sheet", "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/ph-fact-sheet.pdf"),
            ("main-line-riddle", "Riddle Hospital Fact Sheet", "https://www.mainlinehealth.org/-/media/files/pdf/basic-content/about/communications/fact-sheets/rh-fact-sheet.pdf"),
        )
    ],
    _source(
        "cms-pos-q4-2025",
        "CMS Provider of Services File",
        "https://data.cms.gov/sites/default/files/2026-01/c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv",
        "Q4 2025 fallback; stale-source caveat applies",
        "text/csv",
        "csv",
        landing_page="https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-quality-improvement-and-evaluation-system",
        public_domain=True,
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
        "pa-hospital-reports-index",
        "Pennsylvania Hospital Reports",
        "https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports",
        "current index retrieved at acquisition cutoff",
        "text/html",
        "html",
        public_domain=True,
    ),
    _source(
        "nj-facilities-index",
        "New Jersey Licensed Health Care Facility Search",
        "https://healthapps.nj.gov/Facilities/acSetSearch.aspx?by=county",
        "current index retrieved at acquisition cutoff",
        "text/html",
        "html",
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
    Candidate("christianacare", "christiana-hospital", "Christiana Hospital", "christianacare-about", entity_type="campus", missingness="blocked_source_conflict", missingness_reason="CMS CCN 080001 combines ChristianaCare reporting entities; a campus-specific declared basis was not safely separable."),
    Candidate("christianacare", "wilmington-hospital", "Wilmington Hospital", "christianacare-about", entity_type="campus", missingness="blocked_source_conflict", missingness_reason="CMS CCN 080001 combines ChristianaCare reporting entities; a campus-specific declared basis was not safely separable."),
    Candidate("christianacare", "union-hospital", "Union Hospital", "christianacare-union", ccn="210032", missingness="blocked_source_conflict", missingness_reason="Current official, CMS POS, and HCRIS values use differing periods or bed bases; no common declared basis was selected."),
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
    Candidate("temple-health", "temple-university-hospital", "Temple University Hospital", "temple-locations", entity_type="campus", missingness="blocked_source_conflict", missingness_reason="CMS CCN 390027 and official campus reporting do not expose a safely separable Main-versus-Episcopal declared basis."),
    Candidate("temple-health", "episcopal-campus", "Temple University Hospital - Episcopal Campus", "temple-episcopal", entity_type="campus", missingness="blocked_source_conflict", missingness_reason="The official campus value cannot be reconciled to the shared CMS reporting entity without allocation."),
    Candidate("temple-health", "jeanes-campus", "Temple University Hospital - Jeanes Campus", "temple-locations", ccn="390080", source_name="Jeanes Campus"),
    Candidate("temple-health", "fox-chase", "Fox Chase Cancer Center", "temple-locations", ccn="390196"),
    Candidate("temple-health", "chestnut-hill", "Temple Health - Chestnut Hill Hospital", "temple-chestnut-hill", disposition="unresolved", ccn="390026", missingness="blocked_source_conflict", missingness_reason="Temple's alliance language, joint ownership, and differing official/CMS/HCRIS bases require Toolkit adjudication."),
    Candidate("temple-health", "northeastern-campus", "Temple University Hospital - Northeastern Campus", "sepa-chna-2025", disposition="excluded", entity_type="facility", missingness="not_applicable", missingness_reason="The current Temple locations source treats Northeastern as a multispecialty facility rather than a separate hospital."),
    Candidate("penn-medicine", "hup", "Hospital of the University of Pennsylvania", "penn-six-hospitals-2024", ccn="390111"),
    Candidate("penn-medicine", "penn-presbyterian", "Penn Presbyterian Medical Center", "penn-six-hospitals-2024", ccn="390223"),
    Candidate("penn-medicine", "chester-county", "Chester County Hospital", "penn-six-hospitals-2024", ccn="390179"),
    Candidate("penn-medicine", "lancaster-general", "Lancaster General Hospital", "penn-six-hospitals-2024", ccn="390100", source_name="Lancaster General Health"),
    Candidate("penn-medicine", "princeton", "Penn Medicine Princeton Medical Center", "penn-six-hospitals-2024", ccn="310010", source_name="Princeton Health"),
    Candidate("penn-medicine", "pennsylvania-hospital", "Pennsylvania Hospital", "penn-six-hospitals-2024", ccn="390226"),
    Candidate("penn-medicine", "hup-cedar", "Hospital of the University of Pennsylvania - Cedar Avenue", "sepa-chna-2025", disposition="unresolved", entity_type="campus", missingness="blocked_source_conflict", missingness_reason="The location is publicly named as a hospital campus but described as a remote HUP location; no separate CCN/bed allocation was selected."),
    Candidate("penn-medicine", "good-shepherd", "Good Shepherd Penn Partners Rehabilitation", "cms-hcris-2023-final", disposition="unresolved", entity_type="facility", ccn="392050", source_name="Good Shepherd Penn Partners", missingness="unavailable_public", missingness_reason="The current Penn enterprise page was searched, while HCRIS/POS identify this reporting entity; current ownership and product inclusion remain unresolved."),
    Candidate("penn-medicine", "lancaster-behavioral", "Lancaster Behavioral Health Hospital", "cms-hcris-2023-final", disposition="unresolved", ccn="394055"),
    Candidate("penn-medicine", "princeton-house", "Princeton House Behavioral Health", "penn-princeton-house-2024", disposition="unresolved", entity_type="facility"),
    Candidate("cooper-university-health-care", "camden", "Cooper University Hospital", "cooper-about", ccn="310014"),
    Candidate("cooper-university-health-care", "cape-regional", "Cooper University Hospital Cape Regional", "cooper-about", ccn="310011"),
    Candidate("cooper-university-health-care", "md-anderson", "MD Anderson Cancer Center at Cooper", "cooper-about", disposition="excluded", entity_type="facility", missingness="not_applicable", missingness_reason="The current official source lists MD Anderson under 'also home to,' not in its hospital list."),
    Candidate("cooper-university-health-care", "childrens-regional", "Children's Regional Hospital at Cooper", "cooper-about", disposition="unresolved", entity_type="facility", missingness="blocked_source_conflict", missingness_reason="The current official page calls Children's a third hospital, while its Camden co-location and reporting relationship prevent a separate one-to-one bed basis without adjudication."),
    Candidate("main-line-health", "lankenau", "Lankenau Medical Center", "main-line-about", ccn="390195"),
    Candidate("main-line-health", "bryn-mawr", "Bryn Mawr Hospital", "main-line-about", ccn="390139"),
    Candidate("main-line-health", "paoli", "Paoli Hospital", "main-line-about", ccn="390153"),
    Candidate("main-line-health", "riddle", "Riddle Hospital", "main-line-about", ccn="390222"),
    Candidate("main-line-health", "bryn-mawr-rehab", "Bryn Mawr Rehabilitation Hospital", "main-line-about", ccn="393025", source_name="Bryn Mawr Rehab Hospital", missingness="not_yet_researched", missingness_reason="A one-to-one CMS value exists, but a current source-defined licensed/approved rehabilitation basis still requires state extraction; no residual was calculated."),
    Candidate("main-line-health", "mirmont", "Mirmont Treatment Center", "main-line-about", disposition="excluded", entity_type="facility", missingness="not_applicable", missingness_reason="The official system page identifies a treatment center, not a separate hospital in the five-hospital roster."),
]


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
    ("christiana-newark-licensed-906", "data-mcp:facility:official:christianacare:christiana-hospital", "christianacare-about", "bed_count.licensed", r"licensed\W+beds\W+Newark\W+Campus\W+(?P<value>906)", "2023", "licensed", "2023 Statistics: Licensed beds Newark Campus: 906"),
    ("christiana-wilmington-licensed-321", "data-mcp:facility:official:christianacare:wilmington-hospital", "christianacare-about", "bed_count.licensed", r"Wilmington\W+Campus\W+(?P<value>321)", "2023", "licensed", "2023 Statistics: Licensed beds Wilmington Campus: 321"),
    ("christiana-cecil-licensed-109", "data-mcp:facility:ccn:210032", "christianacare-about", "bed_count.licensed", r"Cecil\W+County\W+Campus\W+(?P<value>109)", "2023", "licensed", "2023 Statistics: Licensed beds Cecil County Campus: 109"),
    ("union-official-103", "data-mcp:facility:ccn:210032", "christianacare-union", "bed_count.official_unspecified", r"(?P<value>103)\W+bed\W+facility", "current page", "official unspecified", "Union Hospital bullet: 103-bed facility"),
    ("west-grove-official-10", "data-mcp:facility:official:christianacare:west-grove", "christianacare-west-grove", "bed_count.official_unspecified", r"(?P<value>10)\W+inpatient\W+beds", "2025-08-13", "official inpatient, otherwise unspecified", "opening announcement: 10 inpatient beds"),
    ("episcopal-official-139", "data-mcp:facility:official:temple-health:episcopal-campus", "temple-episcopal", "bed_count.licensed", r"(?P<value>139)\W+licensed\W+beds", "current page", "licensed", "About section: 139 licensed beds"),
    ("chestnut-official-148", "data-mcp:facility:ccn:390026", "temple-chestnut-hill", "bed_count.official_unspecified", r"(?P<value>148)\W+bed", "current page", "official unspecified", "About section: 148-bed hospital"),
    ("cooper-camden-licensed-663", "data-mcp:facility:ccn:310014", "cooper-camden-2025", "bed_count.licensed", r"(?P<value>663)\W+licensed\W+beds", "Q1 2025", "licensed", "Fast Facts: 663 licensed beds"),
    ("cooper-cape-licensed-242", "data-mcp:facility:ccn:310011", "cooper-cape-2025", "bed_count.licensed", r"(?P<value>242)\W+licensed\W+beds", "Q1 2025", "licensed", "Fast Facts: 242 licensed beds"),
    ("lankenau-official-370", "data-mcp:facility:ccn:390195", "main-line-lankenau", "bed_count.licensed", r"licensed\W+beds\W+(?P<value>370)", "FY25", "licensed", "Hospital statistics: LICENSED BEDS 370"),
    ("bryn-mawr-official-284", "data-mcp:facility:ccn:390139", "main-line-bryn-mawr", "bed_count.licensed", r"licensed\W+beds\W+(?P<value>284)", "FY25", "licensed", "Hospital statistics: LICENSED BEDS 284"),
    ("paoli-official-261", "data-mcp:facility:ccn:390153", "main-line-paoli", "bed_count.licensed", r"licensed\W+beds\W+(?P<value>261)", "FY25", "licensed", "Hospital statistics: LICENSED BEDS 261"),
    ("riddle-official-243", "data-mcp:facility:ccn:390222", "main-line-riddle", "bed_count.licensed", r"licensed\W+beds\W+(?P<value>243)", "FY25", "licensed", "Hospital statistics: LICENSED BEDS 243"),
]


FEDERAL_CCN_CANDIDATES = {candidate.ccn: candidate for candidate in CANDIDATES if candidate.ccn}
SHARED_OR_UNSAFE_CCNS = {"390027"}
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
                aliases=[candidate.source_name or candidate.name],
                owner_entity_id=SYSTEM_IDS[candidate.system_slug],
                identity_conflicts=conflicts,
            )
        )

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
        facts.append(_bed_pattern_fact(*row))
        official_bed_entities.add(row[1])

    for ccn, candidate in sorted(FEDERAL_CCN_CANDIDATES.items()):
        if ccn in SHARED_OR_UNSAFE_CCNS:
            continue
        if ccn in HGI_CCNS:
            facts.append(
                FactSpec(
                    fact_id=f"cms-hgi-identity:{ccn}",
                    entity_id=candidate.entity_id,
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
                    entity_id=candidate.entity_id,
                    measure_id=measure_id,
                    value_type="integer",
                    unit="beds",
                    period_label="Q4 2025 fallback",
                    denominator_scope=f"facility-level; basis={basis}; raw source field={field}",
                    source_id="cms-pos-q4-2025",
                    row_locator=f"PRVDR_NUM={ccn}; field={field}",
                    match_basis="Exact CCN only; source name is an alias and not an auto-merge key.",
                    confidence="high for row identity; stale-source caveat applies",
                    caveat="Q1 2026 was not discoverable at acquisition; Q4 2025 fallback retained without recasting its basis.",
                    table_match={"PRVDR_NUM": ccn},
                    table_value_field=field,
                )
            )
        if ccn != "390080":
            facts.append(
                FactSpec(
                    fact_id=f"cms-hcris:{ccn}:number-of-beds",
                    entity_id=candidate.entity_id,
                    measure_id="bed_count.hcris_period_end_available",
                    value_type="integer",
                    unit="beds",
                    period_label="provider fiscal period in 2023 final HCRIS PUF",
                    denominator_scope="facility-level; basis=HCRIS Number of Beds; raw source field=Number of Beds",
                    source_id="cms-hcris-2023-final",
                    row_locator=f"Provider CCN={ccn}; field=Number of Beds; fiscal dates remain in raw row",
                    match_basis="Exact CCN only.",
                    confidence="high for source row; period and basis differ from POS and official values",
                    caveat="HCRIS reporting period is provider-specific and must not be treated as a current licensed bed value.",
                    table_match={"Provider CCN": ccn},
                    table_value_field="Number of Beds",
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
            ],
            source_ids=["christianacare-union", "cms-pos-q4-2025", "cms-hcris-2023-final"],
            rationale="Official 2023 licensed 109-bed, current official 103-bed, POS 166-bed, and HCRIS 124-bed rows have differing labels and periods; none was selected as the common value.",
        ),
        ConflictSpec(
            conflict_id="conflict:christianacare-shared-cms-reporting-entity",
            conflict_type="shared_ccn_and_campus_allocation",
            entity_ids=[
                "data-mcp:facility:official:christianacare:christiana-hospital",
                "data-mcp:facility:official:christianacare:wilmington-hospital",
            ],
            fact_ids=["christiana-newark-licensed-906", "christiana-wilmington-licensed-321"],
            source_ids=["christianacare-about", "cms-pos-q4-2025"],
            rationale="The official source reports campus-specific licensed values while CMS CCN 080001 is a combined reporting entity; no CMS total was allocated to either campus.",
        ),
        ConflictSpec(
            conflict_id="conflict:chestnut-hill-ownership-and-bases",
            conflict_type="ownership_and_bed_basis",
            entity_ids=["data-mcp:facility:ccn:390026"],
            fact_ids=[
                "chestnut-official-148",
                "cms-pos:390026:bed_cnt",
                "cms-pos:390026:crtfd_bed_cnt",
                "cms-hcris:390026:number-of-beds",
            ],
            source_ids=["temple-chestnut-hill", "cms-pos-q4-2025", "cms-hcris-2023-final"],
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
        periods=["2023 HCRIS", "March 2025 official roster", "Q4 2025 POS fallback", "acquisition cutoff"],
        sources=SOURCES,
        entities=entities,
        facts=facts,
        conflicts=conflict_rows,
    )


__all__ = ["CANDIDATES", "SYSTEM_IDS", "acquisition_spec"]
