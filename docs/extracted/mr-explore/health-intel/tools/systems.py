"""Health system definitions and identifiers.

Central registry of health systems and their IDs across all data sources.
"""

from dataclasses import dataclass


@dataclass
class HealthSystem:
    key: str
    name: str
    short_name: str
    city: str
    state: str
    cms_facility_id: str
    ein: str
    primary_npi: str
    system_type: str  # "academic", "community", "for-profit"
    description: str


SYSTEMS = {
    "jefferson_health": HealthSystem(
        key="jefferson_health",
        name="Jefferson Health (Thomas Jefferson University Hospitals)",
        short_name="Jefferson Health",
        city="Philadelphia",
        state="PA",
        cms_facility_id="390174",
        ein="232829095",
        primary_npi="1215916002",
        system_type="academic",
        description="One of the largest academic health systems in the Philadelphia region. "
        "Affiliated with Thomas Jefferson University. Operates 18 hospitals and a network "
        "of outpatient facilities across the Delaware Valley.",
    ),
    "cooper_health": HealthSystem(
        key="cooper_health",
        name="Cooper University Health Care",
        short_name="Cooper Health",
        city="Camden",
        state="NJ",
        cms_facility_id="310014",
        ein="210634462",
        primary_npi="1215165832",
        system_type="academic",
        description="South Jersey's only Level I Trauma Center and tertiary care academic "
        "health system. Affiliated with Cooper Medical School of Rowan University. "
        "Operates Cooper University Hospital and a network of outpatient centers.",
    ),
    "temple_health": HealthSystem(
        key="temple_health",
        name="Temple University Health System",
        short_name="Temple Health",
        city="Philadelphia",
        state="PA",
        cms_facility_id="390027",
        ein="232825878",
        primary_npi="1962579029",
        system_type="academic",
        description="Major academic health system in North Philadelphia affiliated with "
        "Temple University Lewis Katz School of Medicine. Known nationally for "
        "pulmonary medicine and lung transplant programs. Operates Temple University "
        "Hospital, Jeanes Hospital, and Fox Chase Cancer Center.",
    ),
}


def get_system(key: str) -> HealthSystem:
    """Get a health system by key. Raises KeyError if not found."""
    if key not in SYSTEMS:
        raise KeyError(
            f"Unknown system: {key}. Available: {list(SYSTEMS.keys())}"
        )
    return SYSTEMS[key]


def list_systems() -> list[dict]:
    """List all available health systems."""
    return [
        {
            "key": s.key,
            "name": s.name,
            "city": s.city,
            "state": s.state,
            "type": s.system_type,
        }
        for s in SYSTEMS.values()
    ]
