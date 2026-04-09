"""NPPES-based physician search and profile retrieval.

Uses the shared CMS client for NPPES API calls, enriched with cached
CMS Physician Compare and Medicare Utilization PUF data.
"""

import logging
from pathlib import Path

import duckdb
from shared.utils.duckdb_safe import safe_parquet_sql
import httpx

from shared.utils.http_client import resilient_request, get_client
import pandas as pd

import sys as _sys
_project_root = __import__("pathlib").Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in _sys.path:
    _sys.path.insert(0, str(_project_root))

from shared.utils.cache import is_cache_valid  # noqa: E402

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "physician"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_PHYSICIAN_COMPARE_CACHE = _CACHE_DIR / "physician_compare.parquet"
_UTILIZATION_CACHE = _CACHE_DIR / "utilization.parquet"
_CACHE_TTL_DAYS = 30

# CMS Physician Compare dataset (data.medicare.gov Socrata API)
PHYSICIAN_COMPARE_CSV_URL = (
    "https://data.medicare.gov/api/views/mj5m-pzi6/rows.csv?accessType=DOWNLOAD"
)

# Medicare Physician & Other Supplier utilization PUF
# Using data.cms.gov dataset for "by Provider and Service"
UTILIZATION_DATASET_URL = (
    "https://data.cms.gov/provider-summary-by-type-of-service/"
    "medicare-physician-other-practitioners/"
    "medicare-physician-other-practitioners-by-provider-and-service"
)
NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"

# Mapping from common/shorthand specialty names to official NPPES taxonomy
# descriptions.  Keys are lowercased for case-insensitive lookup.
_SPECIALTY_ALIASES: dict[str, str] = {
    "cardiology": "Cardiovascular Disease",
    "cardiac surgery": "Thoracic Surgery (Cardiothoracic Vascular Surgery)",
    "dermatology": "Dermatology",
    "emergency medicine": "Emergency Medicine",
    "endocrinology": "Endocrinology, Diabetes & Metabolism",
    "ent": "Otolaryngology",
    "family medicine": "Family Medicine",
    "gastroenterology": "Gastroenterology",
    "general surgery": "Surgery",
    "geriatrics": "Geriatric Medicine",
    "hematology": "Hematology & Oncology",
    "hospitalist": "Hospitalist",
    "infectious disease": "Infectious Disease",
    "internal medicine": "Internal Medicine",
    "nephrology": "Nephrology",
    "neurology": "Neurology",
    "neurosurgery": "Neurological Surgery",
    "ob/gyn": "Obstetrics & Gynecology",
    "obstetrics": "Obstetrics & Gynecology",
    "gynecology": "Obstetrics & Gynecology",
    "oncology": "Medical Oncology",
    "ophthalmology": "Ophthalmology",
    "orthopedic surgery": "Orthopaedic Surgery",
    "orthopedics": "Orthopaedic Surgery",
    "pain management": "Pain Medicine",
    "pathology": "Pathology",
    "pediatrics": "Pediatrics",
    "physical medicine": "Physical Medicine & Rehabilitation",
    "plastic surgery": "Plastic and Reconstructive Surgery",
    "psychiatry": "Psychiatry & Neurology",
    "pulmonology": "Pulmonary Disease",
    "radiology": "Diagnostic Radiology",
    "rheumatology": "Rheumatology",
    "sports medicine": "Sports Medicine",
    "urology": "Urology",
    "vascular surgery": "Vascular Surgery",
}


# ---------------------------------------------------------------------------
# NPPES Search
# ---------------------------------------------------------------------------

def _resolve_specialty(specialty: str) -> str:
    """Resolve a user-facing specialty name to an NPPES taxonomy description.

    If the specialty matches a known alias (case-insensitive), the official
    taxonomy description is returned.  Otherwise the input is passed through
    unchanged so the NPPES API can attempt its own matching.
    """
    if not specialty:
        return specialty
    return _SPECIALTY_ALIASES.get(specialty.strip().lower(), specialty)


async def search_physicians(
    query: str,
    specialty: str = "",
    state: str = "",
    limit: int = 25,
) -> list[dict]:
    """Search NPPES for individual physicians (NPI-1).

    Args:
        query: First/last name, full name, or NPI number.
        specialty: Specialty filter — accepts common names (e.g. "Cardiology")
                   which are mapped to official NPPES taxonomy descriptions.
        state: Two-letter state code.
        limit: Max results (1-200).

    Returns:
        List of physician summary dicts.
    """
    params: dict = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "limit": min(limit, 200),
    }

    # Detect if query is an NPI number (10 digits)
    if query.strip().isdigit() and len(query.strip()) == 10:
        params["number"] = query.strip()
    elif " " in query.strip():
        # Assume "First Last" format
        parts = query.strip().split(None, 1)
        params["first_name"] = parts[0]
        params["last_name"] = parts[1]
    else:
        # Single name — try as last name
        params["last_name"] = query.strip()

    if specialty:
        params["taxonomy_description"] = _resolve_specialty(specialty)
    if state:
        params["state"] = state.upper()

    resp = await resilient_request("GET", NPPES_API_URL, params=params, timeout=30.0)
    data = resp.json()

    results = data.get("results", [])
    physicians = []
    for r in results:
        basic = r.get("basic", {})
        taxonomies = r.get("taxonomies", [])
        primary_tax = next((t for t in taxonomies if t.get("primary")), taxonomies[0] if taxonomies else {})
        addresses = r.get("addresses", [])
        practice_addr = next((a for a in addresses if a.get("address_purpose") == "LOCATION"), addresses[0] if addresses else {})

        physicians.append({
            "npi": r.get("number", ""),
            "first_name": basic.get("first_name", ""),
            "last_name": basic.get("last_name", ""),
            "credential": basic.get("credential", ""),
            "specialty": primary_tax.get("desc", ""),
            "city": practice_addr.get("city", ""),
            "state": practice_addr.get("state", ""),
            "org_name": basic.get("organization_name", ""),
            "gender": basic.get("gender", ""),
            "enumeration_date": basic.get("enumeration_date", ""),
        })

    return physicians


# ---------------------------------------------------------------------------
# Physician Profile (NPPES detail + enrichment)
# ---------------------------------------------------------------------------

async def get_physician_detail(npi: str) -> dict | None:
    """Get full physician profile from NPPES, enriched with cached data.

    Args:
        npi: 10-digit NPI number.

    Returns:
        Physician profile dict or None if not found.
    """
    params = {"version": "2.1", "number": npi.strip()}

    resp = await resilient_request("GET", NPPES_API_URL, params=params, timeout=30.0)
    data = resp.json()

    results = data.get("results", [])
    if not results:
        return None

    r = results[0]
    basic = r.get("basic", {})
    taxonomies = r.get("taxonomies", [])
    addresses = r.get("addresses", [])

    specialties = [t.get("desc", "") for t in taxonomies if t.get("desc")]
    org_affiliations = []
    if basic.get("organization_name"):
        org_affiliations.append(basic["organization_name"])

    practice_locations = []
    for addr in addresses:
        if addr.get("address_purpose") == "LOCATION":
            practice_locations.append({
                "address_1": addr.get("address_1", ""),
                "city": addr.get("city", ""),
                "state": addr.get("state", ""),
                "postal_code": addr.get("postal_code", ""),
                "telephone_number": addr.get("telephone_number", ""),
            })

    profile = {
        "npi": npi,
        "first_name": basic.get("first_name", ""),
        "last_name": basic.get("last_name", ""),
        "credential": basic.get("credential", ""),
        "specialties": specialties,
        "practice_locations": practice_locations,
        "org_affiliations": org_affiliations,
        "gender": basic.get("gender", ""),
        "enumeration_date": basic.get("enumeration_date", ""),
    }

    # Enrich with utilization data
    utilization = get_utilization_summary(npi)
    if utilization:
        profile["utilization"] = utilization

    # Enrich with Physician Compare quality data
    quality = get_quality_info(npi)
    if quality:
        profile["quality"] = quality

    return profile


# ---------------------------------------------------------------------------
# Physician Compare (cached Parquet)
# ---------------------------------------------------------------------------

def _is_cache_valid(path: Path) -> bool:
    """Check if a cached Parquet file exists and is within TTL."""
    return is_cache_valid(path, max_age_days=_CACHE_TTL_DAYS)


async def ensure_physician_compare_cached() -> bool:
    """Download Physician Compare CSV and convert to Parquet if needed.

    Returns True if cache is available, False on failure.
    """
    if _is_cache_valid(_PHYSICIAN_COMPARE_CACHE):
        return True

    logger.info("Downloading Physician Compare data...")
    try:
        resp = await resilient_request("GET", PHYSICIAN_COMPARE_CSV_URL, timeout=600.0)

        # Write CSV temporarily, convert to Parquet
        csv_path = _CACHE_DIR / "physician_compare_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_PHYSICIAN_COMPARE_CACHE, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("Physician Compare cached: %d rows", len(df))
        return True

    except Exception as e:
        logger.warning("Failed to cache Physician Compare: %s", e)
        return False


def get_quality_info(npi: str) -> dict | None:
    """Query cached Physician Compare data for quality/affiliation info.

    Returns dict with group_practice, hospital_affiliations, graduation_year, etc.
    """
    if not _PHYSICIAN_COMPARE_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(
            f"CREATE VIEW pc AS SELECT * FROM {safe_parquet_sql(_PHYSICIAN_COMPARE_CACHE)}"
        )

        # Find NPI column (may be "npi" or "ind_pac_id" etc.)
        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='pc'").fetchall()]
        npi_col = next((c for c in cols if c in ("npi", "ind_npi", "npi_number")), None)
        if not npi_col:
            con.close()
            return None

        rows = con.execute(f"SELECT * FROM pc WHERE {npi_col} = ? LIMIT 1", [npi]).fetchdf()
        con.close()

        if rows.empty:
            return None

        row = rows.iloc[0]

        # Extract hospital affiliations (columns like hosp_afl_1, hosp_afl_2, etc.)
        hospital_cols = [c for c in row.index if c.startswith("hosp_afl")]
        hospitals = [str(row[c]) for c in hospital_cols if row[c] and str(row[c]).strip()]

        return {
            "group_practice_pac_id": str(row.get("org_pac_id", "")),
            "group_practice_name": str(row.get("org_nm", row.get("organization_legal_name", ""))),
            "hospital_affiliations": hospitals,
            "graduation_year": str(row.get("grd_yr", row.get("graduation_year", ""))),
            "medical_school": str(row.get("med_sch", row.get("medical_school_name", ""))),
        }

    except Exception as e:
        logger.warning("Physician Compare query failed for NPI %s: %s", npi, e)
        return None


# ---------------------------------------------------------------------------
# Medicare Utilization PUF (cached Parquet)
# ---------------------------------------------------------------------------

async def ensure_utilization_cached() -> bool:
    """Download Medicare Utilization PUF and convert to Parquet if needed.

    This downloads the "by Provider" aggregate file (one row per NPI) rather
    than the per-service file, to keep the dataset manageable.

    Returns True if cache is available, False on failure.
    """
    if _is_cache_valid(_UTILIZATION_CACHE):
        return True

    # Try the "by Provider" aggregate dataset from data.cms.gov
    # Dataset: Medicare Physician & Other Practitioners - by Provider
    PROVIDER_AGG_URL = (
        "https://data.cms.gov/provider-summary-by-type-of-service/"
        "medicare-physician-other-practitioners/"
        "medicare-physician-other-practitioners-by-provider"
    )

    logger.info("Downloading Medicare Utilization data (by Provider)...")
    try:
        # First, get the dataset page to find the download URL
        resp = await resilient_request("GET", f"{PROVIDER_AGG_URL}?format=csv", timeout=60.0)
        # If this doesn't work, try the direct download pattern
        if resp.status_code != 200 or len(resp.content) < 1000:
            # Try data-api pattern
            resp = await client.get(
                "https://data.cms.gov/data-api/v1/dataset/3614c3f0-21a5-4a7f-8e37-7cf21b6caa5d/data",
                params={"size": 0},
                timeout=30.0,
            )
            resp.raise_for_status()

        # If we got CSV data, save it
        if resp.headers.get("content-type", "").startswith("text/csv") or len(resp.content) > 10000:
            csv_path = _CACHE_DIR / "utilization_raw.csv"
            csv_path.write_bytes(resp.content)

            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            df.to_parquet(_UTILIZATION_CACHE, compression="zstd", index=False)

            csv_path.unlink(missing_ok=True)
            logger.info("Utilization data cached: %d rows", len(df))
            return True

        logger.warning("Could not download utilization data — unexpected response")
        return False

    except Exception as e:
        logger.warning("Failed to cache utilization data: %s", e)
        return False


def get_utilization_summary(npi: str) -> dict | None:
    """Query cached utilization data for an NPI.

    Returns summary with total services, beneficiaries, payments, top HCPCS codes.
    """
    if not _UTILIZATION_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(
            f"CREATE VIEW util AS SELECT * FROM {safe_parquet_sql(_UTILIZATION_CACHE)}"
        )

        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='util'").fetchall()]
        npi_col = next((c for c in cols if c in ("npi", "rndrng_npi", "national_provider_identifier")), None)
        if not npi_col:
            con.close()
            return None

        rows = con.execute(f"SELECT * FROM util WHERE {npi_col} = ? LIMIT 1", [npi]).fetchdf()
        con.close()

        if rows.empty:
            return None

        row = rows.iloc[0]

        def _safe_float(val):
            try:
                v = str(val).replace(",", "").strip()
                return float(v) if v and v.lower() not in ("", "nan", "none") else None
            except (ValueError, TypeError):
                return None

        def _safe_int(val):
            f = _safe_float(val)
            return int(f) if f is not None else 0

        # Column name candidates (CMS changes names between releases)
        total_services = _safe_int(
            row.get("tot_srvcs", row.get("total_services", row.get("tot_hcpcs_cds", 0)))
        )
        total_benes = _safe_int(
            row.get("tot_benes", row.get("total_unique_benes", row.get("tot_bene_cnt", 0)))
        )
        total_payment = _safe_float(
            row.get("tot_mdcr_pymt_amt", row.get("total_medicare_payment_amt", None))
        )
        avg_allowed = _safe_float(
            row.get("avg_mdcr_alowd_amt", row.get("avg_medicare_allowed_amt", None))
        )
        avg_charge = _safe_float(
            row.get("avg_sbmtd_chrg", row.get("avg_submitted_charge_amt", None))
        )

        return {
            "total_services": total_services,
            "total_beneficiaries": total_benes,
            "total_medicare_payment": total_payment,
            "avg_allowed_amount": avg_allowed,
            "avg_submitted_charge": avg_charge,
            "top_hcpcs": [],  # Populated when using per-service dataset
        }

    except Exception as e:
        logger.warning("Utilization query failed for NPI %s: %s", npi, e)
        return None
