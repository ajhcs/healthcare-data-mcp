"""Static Public Alpha patient-volume evidence contract values."""

METRIC_KEYS = ["geography.primary_service_area", "market.effective_local_market_share"]
ROW_TYPES = ("zip_demand", "competitor_access_point", "distance_friction", "attractiveness_input")
DENOMINATOR_SCOPES = ("all_payer_inpatient", "medicare_inpatient", "modeled_population_utilization")
MISSINGNESS_STATES = ("not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict")

SOURCE_HIERARCHY = [
    {
        "rank": 1,
        "source_family": "state_all_payer_discharge_zip_origin",
        "denominator_scopes_allowed": ["all_payer_inpatient"],
        "rule": "Prefer public all-payer hospital discharge rows with patient ZIP/ZCTA origin when every launch system and material competitor is covered comparably.",
    },
    {
        "rank": 2,
        "source_family": "cms_hospital_service_area_file",
        "denominator_scopes_allowed": ["medicare_inpatient"],
        "rule": "Use CMS HSAF Medicare inpatient ZIP-origin discharges only if payer and age bias are explicit and coverage is fair for all six systems.",
    },
    {
        "rank": 3,
        "source_family": "cms_medicare_provider_utilization_puf",
        "denominator_scopes_allowed": ["medicare_inpatient"],
        "rule": "Use CMS Medicare provider utilization aggregates as service-line or denominator context, not as standalone all-payer demand.",
    },
    {
        "rank": 4,
        "source_family": "acs_population_cms_utilization_model",
        "denominator_scopes_allowed": ["modeled_population_utilization"],
        "rule": "Use modeled public population and utilization rates only when observed patient-volume sources fail comparability review.",
    },
    {
        "rank": 5,
        "source_family": "public_facility_and_routing_context",
        "denominator_scopes_allowed": ["all_payer_inpatient", "medicare_inpatient", "modeled_population_utilization"],
        "rule": "Use facility, access-point, drive-time, and capacity rows as ELMS inputs; they cannot establish ZIP demand alone.",
    },
]


def denominator_scope(value: object) -> str:
    scope = str(value or "").strip().lower().replace("-", "_")
    if scope in {"all_payer", "all_payer_discharge", "inpatient_all_payer"}:
        return "all_payer_inpatient"
    if scope in {"medicare", "medicare_ffs", "medicare_discharges"}:
        return "medicare_inpatient"
    if scope in {"modeled", "population_model", "modeled_demand"}:
        return "modeled_population_utilization"
    return scope


def source_rank(source_family: str) -> int | None:
    for source in SOURCE_HIERARCHY:
        if source["source_family"] == source_family:
            return int(source["rank"])
    return None


def allowed_scopes(source_family: str) -> list[str]:
    for source in SOURCE_HIERARCHY:
        if source["source_family"] == source_family:
            return [str(scope) for scope in source["denominator_scopes_allowed"]]
    return []
