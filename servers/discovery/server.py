"""Discovery resources and prompts for the healthcare-data-mcp collection.

The discovery server is intentionally metadata-only. Importing it must not
import data loaders, create cache directories, call network APIs, or require
API keys. Cache status is computed from filesystem metadata under the expected
cache root.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "discovery"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8015"))
mcp = FastMCP(**_mcp_kwargs)

DEFAULT_CACHE_ROOT = Path.home() / ".healthcare-data-mcp" / "cache"
REPO_ROOT = Path(__file__).resolve().parents[2]


DATASET_CATALOG: dict[str, dict[str, Any]] = {
    "cms_hospital_general_info": {
        "title": "CMS Hospital General Information",
        "server": ["cms-facility", "hospital-quality", "service-area", "drive-time"],
        "category": "facility_master",
        "grain": "one row per Medicare-certified hospital",
        "description": "Hospital identity, address, ownership, emergency service, and star rating fields.",
        "source_system": "CMS Provider Data Catalog",
        "source_urls": [
            "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv",
        ],
        "cache_files": [
            "hospital_general_info.csv",
            "hospital_quality_hospital_info.csv",
        ],
        "schema": {
            "identity_fields": ["facility_id", "ccn", "provider_id"],
            "common_fields": [
                "facility_name",
                "address",
                "city",
                "state",
                "zip_code",
                "hospital_type",
                "hospital_ownership",
                "emergency_services",
                "hospital_overall_rating",
            ],
            "join_keys": ["ccn", "facility_id"],
        },
        "workflows": ["facility lookup", "competitor list building", "quality benchmarking"],
    },
    "cms_provider_of_services": {
        "title": "CMS Provider of Services",
        "server": ["health-system-profiler", "cms-facility", "public-records"],
        "category": "facility_master",
        "grain": "one row per certified provider location",
        "description": "CMS POS facility attributes, bed counts, services, staffing, and provider category codes.",
        "source_system": "CMS quarterly POS public use file",
        "source_urls": [
            "https://data.cms.gov/sites/default/files/2026-01/"
            "c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv",
        ],
        "cache_files": [
            "pos_q4_2025.csv",
            "public-records/pos_q4_2025.parquet",
        ],
        "schema": {
            "identity_fields": ["PRVDR_NUM", "FAC_NAME"],
            "common_fields": [
                "CITY_NAME",
                "STATE_CD",
                "ZIP_CD",
                "BED_CNT",
                "CRTFD_BED_CNT",
                "RN_CNT",
                "PHYSN_CNT",
                "OPRTG_ROOM_CNT",
                "PRVDR_CTGRY_CD",
            ],
            "join_keys": ["PRVDR_NUM", "ccn"],
        },
        "workflows": ["bed inventory", "system facility enrichment", "staffing snapshots"],
    },
    "ahrq_health_system_compendium": {
        "title": "AHRQ Compendium of U.S. Health Systems",
        "server": ["health-system-profiler"],
        "category": "system_affiliation",
        "grain": "system and hospital-linkage files",
        "description": "Health system names, headquarters, hospital counts, and hospital-to-system linkage.",
        "source_system": "AHRQ Comparative Health System Performance Initiative",
        "source_urls": [
            "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv",
            "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
        ],
        "cache_files": [
            "ahrq_system_2023.csv",
            "ahrq_hospital_linkage_2023.csv",
        ],
        "schema": {
            "identity_fields": ["health_sys_id", "health_sys_name", "ccn"],
            "common_fields": [
                "health_sys_city",
                "health_sys_state",
                "hosp_count",
                "phys_grp_count",
                "hospital_name",
                "hos_beds",
                "hos_dsch",
            ],
            "join_keys": ["health_sys_id", "ccn"],
        },
        "workflows": ["system discovery", "facility rollups", "market footprint"],
    },
    "cms_hospital_quality": {
        "title": "CMS Hospital Quality Programs",
        "server": ["hospital-quality"],
        "category": "quality",
        "grain": "facility-measure rows",
        "description": "Quality star ratings, HRRP readmissions, HAC safety scores, HCAHPS, and complications.",
        "source_system": "CMS Provider Data Catalog",
        "source_urls": [
            "https://data.cms.gov/provider-data/api/1/datastore/query/9n3s-kdb3/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/yq43-i98g/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/dgck-syfz/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/ynj2-r877/0/download?format=csv",
        ],
        "cache_files": [
            "hospital_quality_hrrp.csv",
            "hospital_quality_hac.csv",
            "hospital_quality_hcahps.csv",
            "hospital_quality_complications.csv",
        ],
        "schema": {
            "identity_fields": ["facility_id", "provider_id", "measure_id"],
            "common_fields": [
                "facility_name",
                "measure_name",
                "score",
                "compared_to_national",
                "number_of_discharges",
                "payment_reduction",
            ],
            "join_keys": ["facility_id", "ccn"],
        },
        "workflows": ["quality comparison", "readmission risk", "patient experience analysis"],
    },
    "cms_cost_report": {
        "title": "CMS Hospital Cost Report PUF",
        "server": ["hospital-quality", "workforce-analytics"],
        "category": "financial_workforce",
        "grain": "hospital cost report worksheet rows",
        "description": "Hospital cost report financial, utilization, and staffing worksheet extracts.",
        "source_system": "CMS Cost Reports",
        "source_urls": [
            "https://data.cms.gov/sites/default/files/2026-01/"
            "3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv",
            "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
        ],
        "cache_files": [
            "hospital_quality_cost_report.csv",
            "workforce/hcris_staffing.parquet",
        ],
        "schema": {
            "identity_fields": ["provider_ccn", "rpt_rec_num", "fiscal_year"],
            "common_fields": ["worksheet", "line_num", "clmn_num", "itm_val_num", "provider_name"],
            "join_keys": ["provider_ccn", "ccn"],
        },
        "workflows": ["financial profile", "staffing by bed", "cost report audit trail"],
    },
    "cms_hsaf": {
        "title": "CMS Hospital Service Area File",
        "server": ["service-area"],
        "category": "market_share",
        "grain": "hospital-ZIP service area rows",
        "description": "Medicare hospital service areas used for ZIP-level market share and patient origin.",
        "source_system": "CMS data.cms.gov",
        "source_urls": [
            "https://data.cms.gov/sites/default/files/2025-07/"
            "8fca1932-adaa-411d-a912-78fb0854a286/Hospital_Service_Area_2024.csv",
            "https://data.cms.gov/data-api/v1/dataset/8708ca8b-8636-44ed-8303-724cbfaf78ad/data",
        ],
        "cache_files": ["hsaf.csv"],
        "schema": {
            "identity_fields": ["ccn", "zip_code"],
            "common_fields": ["hospital_name", "state", "total_cases", "market_share", "hsa", "hrr"],
            "join_keys": ["ccn", "zip_code"],
        },
        "workflows": ["market share", "service area definition", "competitor overlap"],
    },
    "dartmouth_hsa_hrr": {
        "title": "Dartmouth Atlas ZIP-HSA-HRR Crosswalk",
        "server": ["service-area", "physician-referral-network"],
        "category": "geography",
        "grain": "ZIP to HSA/HRR crosswalk",
        "description": "Dartmouth Atlas healthcare geography crosswalks for HSA and HRR analysis.",
        "source_system": "Dartmouth Atlas",
        "source_urls": [
            "https://data.dartmouthatlas.org/downloads/geography/ZipHsaHrr19.csv.zip",
            "https://data.dartmouthatlas.org/downloads/geography/ZipHsaHrr18.csv",
        ],
        "cache_files": [
            "dartmouth_zip_crosswalk.csv",
            "dartmouth/zip_hsa_hrr.parquet",
        ],
        "schema": {
            "identity_fields": ["zipcode", "hsanum", "hrrnum"],
            "common_fields": ["hsacity", "hsastate", "hrrcity", "hrrstate"],
            "join_keys": ["zip_code", "zipcode"],
        },
        "workflows": ["HSA rollup", "HRR rollup", "market geography normalization"],
    },
    "cms_geographic_variation": {
        "title": "CMS Geographic Variation Public Use File",
        "server": ["geo-demographics"],
        "category": "demographics_utilization",
        "grain": "state/county geography by year",
        "description": "Medicare spending and utilization by geography.",
        "source_system": "CMS Geographic Variation PUF",
        "source_urls": [
            "https://data.cms.gov/sites/default/files/2025-03/"
            "a40ac71d-9f80-4d99-92d2-fd149433d7d8/"
            "2014-2023%20Medicare%20Fee-for-Service%20Geographic%20Variation%20Public%20Use%20File.csv",
        ],
        "cache_files": ["geo-demographics/geographic_variation.parquet"],
        "schema": {
            "identity_fields": ["state", "county", "year"],
            "common_fields": ["beneficiaries", "per_capita_spending", "risk_score", "utilization"],
            "join_keys": ["state", "county_fips"],
        },
        "workflows": ["market demographics", "Medicare intensity", "county benchmarking"],
    },
    "census_acs": {
        "title": "Census ACS 5-Year API",
        "server": ["geo-demographics"],
        "category": "demographics",
        "grain": "Census geography",
        "description": "ACS population, income, age, race, and socioeconomic variables.",
        "source_system": "U.S. Census Bureau API",
        "source_urls": ["https://api.census.gov/data"],
        "cache_files": [],
        "schema": {
            "identity_fields": ["state", "county", "tract", "zcta"],
            "common_fields": ["population", "median_income", "age_bands", "race_ethnicity"],
            "join_keys": ["fips", "zcta", "zip_code"],
        },
        "workflows": ["demographic profile", "access analysis", "community need"],
    },
    "cms_medicare_claims_pufs": {
        "title": "CMS Medicare Provider Utilization PUFs",
        "server": ["claims-analytics"],
        "category": "claims",
        "grain": "provider-service rows by discharge or service year",
        "description": "Inpatient DRG and outpatient APC utilization, charges, and payment public use files.",
        "source_system": "CMS Medicare Provider Utilization and Payment Data",
        "source_urls": [
            "https://data.cms.gov/sites/default/files/2025-05/"
            "ca1c9013-8c7c-4560-a4a1-28cf7e43ccc8/MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV",
            "https://data.cms.gov/sites/default/files/2025-08/"
            "bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv",
        ],
        "cache_files": [
            "claims-analytics/inpatient_dy23.parquet",
            "claims-analytics/outpatient_dy23.parquet",
        ],
        "schema": {
            "identity_fields": ["rndrng_prvdr_ccn", "drg_cd", "apc_cd"],
            "common_fields": [
                "rndrng_prvdr_org_name",
                "tot_dschrgs",
                "outptnt_srvcs",
                "avg_submtd_chrgs",
                "avg_tot_pymt_amt",
            ],
            "join_keys": ["rndrng_prvdr_ccn", "ccn", "drg_cd", "apc_cd"],
        },
        "workflows": ["service-line volume", "Medicare payment benchmark", "competitor mix"],
    },
    "cms_price_transparency_mrf": {
        "title": "CMS Hospital Price Transparency MRFs",
        "server": ["price-transparency"],
        "category": "price_transparency",
        "grain": "hospital item/service payer-plan rate rows",
        "description": "Hospital machine-readable standard charge files normalized to queryable Parquet.",
        "source_system": "Hospital-hosted CMS standard charge files",
        "source_urls": ["https://{hospital-domain}/cms-hpt.txt"],
        "cache_files": ["mrf/registry.json", "mrf/{ccn-or-domain}/normalized.parquet"],
        "schema": {
            "identity_fields": ["ccn", "billing_code", "payer_name", "plan_name"],
            "common_fields": [
                "description",
                "setting",
                "gross_charge",
                "cash_price",
                "negotiated_rate",
                "billing_code_type",
            ],
            "join_keys": ["ccn", "billing_code", "payer_name"],
        },
        "workflows": ["payer rate benchmark", "shoppable service pricing", "MRF compliance"],
    },
    "nppes_registry": {
        "title": "NPPES NPI Registry",
        "server": ["cms-facility", "health-system-profiler", "physician-referral-network"],
        "category": "provider_identity",
        "grain": "provider organization or individual NPI",
        "description": "NPI registry search for organizations, outpatient locations, and physicians.",
        "source_system": "CMS NPPES Registry API",
        "source_urls": ["https://npiregistry.cms.hhs.gov/api/"],
        "cache_files": [],
        "schema": {
            "identity_fields": ["npi", "enumeration_type"],
            "common_fields": ["basic.name", "addresses", "taxonomies", "identifiers"],
            "join_keys": ["npi", "organization_name"],
        },
        "workflows": ["outpatient discovery", "physician lookup", "organization identity resolution"],
    },
    "physician_compare_utilization": {
        "title": "Physician Compare and Medicare Utilization",
        "server": ["physician-referral-network"],
        "category": "physician_network",
        "grain": "physician profile and provider-service utilization rows",
        "description": "Physician demographics, specialties, practice locations, and Medicare service utilization.",
        "source_system": "CMS Physician Compare and Provider Summary datasets",
        "source_urls": [
            "https://data.medicare.gov/api/views/mj5m-pzi6/rows.csv?accessType=DOWNLOAD",
            "https://data.cms.gov/provider-summary-by-type-of-service/",
        ],
        "cache_files": [
            "physician/physician_compare.parquet",
            "physician/utilization.parquet",
        ],
        "schema": {
            "identity_fields": ["npi", "specialty"],
            "common_fields": ["first_name", "last_name", "organization", "address", "hcpcs_code", "services"],
            "join_keys": ["npi", "specialty", "zip_code"],
        },
        "workflows": ["physician mix", "specialty supply", "service-line referral context"],
    },
    "docgraph_referrals": {
        "title": "DocGraph Shared Patient Referral Data",
        "server": ["physician-referral-network"],
        "category": "physician_network",
        "grain": "directed NPI-to-NPI shared patient edge",
        "description": "CareSet DocGraph referral graph loaded from a manually downloaded CSV.",
        "source_system": "CareSet DocGraph",
        "source_urls": ["https://careset.com/datasets/"],
        "cache_files": ["docgraph/shared_patients.parquet"],
        "schema": {
            "identity_fields": ["npi_from", "npi_to"],
            "common_fields": ["shared_count", "transaction_count", "same_day_count"],
            "join_keys": ["npi_from", "npi_to", "npi"],
        },
        "workflows": ["referral leakage", "physician network graph", "service-line inflow"],
    },
    "public_records": {
        "title": "Healthcare Public Records",
        "server": ["public-records"],
        "category": "compliance_contracting",
        "grain": "entity, certification, contract, or breach records",
        "description": "SAM.gov opportunities, USAspending awards, CHPL certifications, 340B, and HIPAA breaches.",
        "source_system": "SAM.gov, USAspending, CHPL, HRSA, HHS OCR",
        "source_urls": [
            "https://api.sam.gov/prod/opportunities/v2/search",
            "https://api.usaspending.gov/api/v2",
            "https://chpl.healthit.gov/rest/certification_ids/{cehrt_id}",
            "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
        ],
        "cache_files": [
            "public-records/340b_covered_entities.parquet",
            "public-records/hipaa_breaches.parquet",
            "public-records/340b_covered_entities.json",
            "public-records/hipaa_breaches.csv",
        ],
        "schema": {
            "identity_fields": ["entity_name", "ein", "uei", "cehrt_id"],
            "common_fields": ["award_id", "obligation", "breach_date", "covered_entity_type", "certification_status"],
            "join_keys": ["entity_name", "ein", "uei", "ccn"],
        },
        "workflows": ["contracting scan", "compliance check", "public-records due diligence"],
    },
    "web_intelligence": {
        "title": "Healthcare Web Intelligence",
        "server": ["web-intelligence"],
        "category": "web_osint",
        "grain": "search result, page, executive, news, or GPO association",
        "description": "Google CSE, news RSS, EHR vendor, executive, and GPO discovery enrichment.",
        "source_system": "Google CSE, Google News RSS, Proxycurl, CMS PI, bundled GPO directory",
        "source_urls": [
            "https://www.googleapis.com/customsearch/v1",
            "https://news.google.com/rss/search",
            "https://nubela.co/proxycurl/api/v2/linkedin",
        ],
        "cache_files": [
            "web-intelligence/pi_hospital.parquet",
            "web-intelligence/api_*.json",
        ],
        "schema": {
            "identity_fields": ["system_name", "domain", "url"],
            "common_fields": ["title", "snippet", "source_url", "cached_at", "confidence"],
            "join_keys": ["system_name", "ccn", "domain"],
        },
        "workflows": ["executive research", "EHR vendor detection", "GPO discovery", "news scan"],
    },
    "workforce_labor": {
        "title": "Healthcare Workforce and Labor Datasets",
        "server": ["workforce-analytics"],
        "category": "workforce",
        "grain": "shortage area, cost report staffing, residency program, or labor action",
        "description": "HRSA HPSA, CMS PBJ/HCRIS staffing, ACGME programs, NLRB cases, and BLS stoppages.",
        "source_system": "HRSA, CMS, ACGME, NLRB, BLS",
        "source_urls": [
            "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv",
            "https://data.cms.gov/data-api/v1/dataset/7e0d53ba-8f02-4c66-98a5-14a1c997c50d/data",
            "https://github.com/labordata/nlrb-data/releases/download/nightly/nlrb.db.zip",
            "https://download.bls.gov/pub/time.series/ws/ws.data.1.AllData",
        ],
        "cache_files": [
            "workforce/hpsa.parquet",
            "workforce/hcris_staffing.parquet",
            "workforce/nlrb.db",
            "workforce/work_stoppages.parquet",
        ],
        "schema": {
            "identity_fields": ["hpsa_id", "provider_ccn", "case_number", "series_id"],
            "common_fields": ["state", "county", "discipline", "hpsa_score", "staffing_metric", "union_name"],
            "join_keys": ["state", "county_fips", "provider_ccn"],
        },
        "workflows": ["shortage analysis", "labor risk scan", "residency pipeline", "staffing benchmark"],
    },
}


CACHE_ENTRIES: tuple[dict[str, Any], ...] = tuple(
    {
        "dataset_id": dataset_id,
        "relative_path": relative_path,
        "ttl_days": 90 if "{ccn-or-domain}" not in relative_path and "*" not in relative_path else None,
    }
    for dataset_id, dataset in DATASET_CATALOG.items()
    for relative_path in dataset["cache_files"]
)


RUNBOOKS: dict[str, dict[str, Any]] = {
    "cache-prime": {
        "title": "Prime local dataset caches",
        "purpose": "Warm cache-backed servers before a market analysis session.",
        "steps": [
            "Run the server tool that owns the dataset with a narrow query first.",
            "Inspect healthcare-data://cache/status for ready, stale, and missing files.",
            "For large CMS files, allow the first call several minutes and rerun the status resource.",
            "If a cache file is corrupt, remove only that file and call the owning tool again.",
        ],
        "notes": [
            "The discovery server never downloads data itself.",
            "Most caches live under ~/.healthcare-data-mcp/cache.",
        ],
    },
    "manual-seed": {
        "title": "Load manually seeded datasets",
        "purpose": "Enable datasets that cannot be fetched automatically.",
        "steps": [
            "Download DocGraph from CareSet and load it through the physician-referral-network loader.",
            "Place 340b_covered_entities.json under public-records cache before 340B lookups.",
            "Place hipaa_breaches.csv under public-records cache before breach lookups.",
            "Run healthcare-data://cache/status to confirm the converted Parquet files exist.",
        ],
        "manual_seed_files": [
            "public-records/340b_covered_entities.json",
            "public-records/hipaa_breaches.csv",
            "docgraph/shared_patients.parquet",
        ],
    },
    "source-audit": {
        "title": "Audit data provenance for a client deliverable",
        "purpose": "Collect source URLs, join keys, and cache freshness before citing findings.",
        "steps": [
            "Read healthcare-data://datasets/catalog to identify datasets used in the workflow.",
            "Read healthcare-data://datasets/{dataset_id}/source for source URLs and owning servers.",
            "Read healthcare-data://datasets/{dataset_id}/schema for join keys and grain.",
            "Attach healthcare-data://cache/status output to the workpaper when cache age matters.",
        ],
    },
}


def _json(data: Any) -> str:
    return json.dumps(data, indent=2, sort_keys=True)


def _dataset_or_error(dataset_id: str) -> dict[str, Any]:
    dataset = DATASET_CATALOG.get(dataset_id)
    if dataset is None:
        return {
            "error": f"Unknown dataset_id: {dataset_id}",
            "available_dataset_ids": sorted(DATASET_CATALOG),
        }
    return {"dataset_id": dataset_id, **dataset}


def dataset_catalog_payload() -> dict[str, Any]:
    """Return the dataset catalog summary without cache state."""
    datasets = []
    for dataset_id, dataset in sorted(DATASET_CATALOG.items()):
        datasets.append(
            {
                "dataset_id": dataset_id,
                "title": dataset["title"],
                "server": dataset["server"],
                "category": dataset["category"],
                "grain": dataset["grain"],
                "source_system": dataset["source_system"],
                "workflows": dataset["workflows"],
            }
        )
    return {
        "catalog_version": "2026-04-23",
        "dataset_count": len(datasets),
        "resource_templates": [
            "healthcare-data://datasets/{dataset_id}",
            "healthcare-data://datasets/{dataset_id}/schema",
            "healthcare-data://datasets/{dataset_id}/source",
        ],
        "datasets": datasets,
    }


def dataset_metadata_payload(dataset_id: str) -> dict[str, Any]:
    """Return full metadata for one dataset."""
    return _dataset_or_error(dataset_id)


def dataset_schema_payload(dataset_id: str) -> dict[str, Any]:
    """Return schema and join metadata for one dataset."""
    dataset = _dataset_or_error(dataset_id)
    if "error" in dataset:
        return dataset
    return {
        "dataset_id": dataset_id,
        "title": dataset["title"],
        "grain": dataset["grain"],
        "schema": dataset["schema"],
    }


def dataset_source_payload(dataset_id: str) -> dict[str, Any]:
    """Return source and cache metadata for one dataset."""
    dataset = _dataset_or_error(dataset_id)
    if "error" in dataset:
        return dataset
    return {
        "dataset_id": dataset_id,
        "title": dataset["title"],
        "server": dataset["server"],
        "source_system": dataset["source_system"],
        "source_urls": dataset["source_urls"],
        "cache_files": dataset["cache_files"],
    }


def _resolve_cache_path(relative_path: str, cache_root: Path) -> Path | None:
    if "{" in relative_path or "*" in relative_path:
        return None
    return cache_root / relative_path


def _cache_entry_status(entry: dict[str, Any], cache_root: Path, now: datetime) -> dict[str, Any]:
    relative_path = entry["relative_path"]
    path = _resolve_cache_path(relative_path, cache_root)
    payload: dict[str, Any] = {
        "dataset_id": entry["dataset_id"],
        "relative_path": relative_path,
        "ttl_days": entry["ttl_days"],
    }
    if path is None:
        payload["status"] = "pattern"
        payload["note"] = "Path is a glob or template; inspect the owning cache directory."
        return payload

    payload["path"] = str(path)
    if not path.exists():
        payload["status"] = "missing"
        return payload

    stat = path.stat()
    modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
    age_days = (now - modified).total_seconds() / 86400
    ttl_days = entry.get("ttl_days")
    payload.update(
        {
            "status": "stale" if ttl_days is not None and age_days > ttl_days else "ready",
            "size_bytes": stat.st_size,
            "modified_at": modified.isoformat(),
            "age_days": round(age_days, 2),
        }
    )
    return payload


def cache_status_payload(cache_root: str | Path | None = None) -> dict[str, Any]:
    """Return cache status using only local filesystem metadata."""
    root = Path(cache_root) if cache_root is not None else DEFAULT_CACHE_ROOT
    now = datetime.now(timezone.utc)
    entries = [_cache_entry_status(entry, root, now) for entry in CACHE_ENTRIES]
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    return {
        "cache_root": str(root),
        "checked_at": now.isoformat(),
        "summary": counts,
        "entries": entries,
    }


def cache_runbooks_payload() -> dict[str, Any]:
    """Return available discovery/cache runbooks."""
    return {
        "runbook_count": len(RUNBOOKS),
        "resource_template": "healthcare-data://runbooks/{runbook_id}",
        "runbooks": [
            {"runbook_id": runbook_id, "title": runbook["title"], "purpose": runbook["purpose"]}
            for runbook_id, runbook in sorted(RUNBOOKS.items())
        ],
    }


def runbook_payload(runbook_id: str) -> dict[str, Any]:
    """Return one runbook by id."""
    runbook = RUNBOOKS.get(runbook_id)
    if runbook is None:
        return {"error": f"Unknown runbook_id: {runbook_id}", "available_runbook_ids": sorted(RUNBOOKS)}
    return {"runbook_id": runbook_id, **runbook}


@mcp.resource(
    "healthcare-data://datasets/catalog",
    name="dataset_catalog",
    description="Catalog of datasets exposed or used by healthcare-data-mcp servers.",
    mime_type="application/json",
)
def dataset_catalog() -> str:
    """List known datasets, owning servers, source systems, and use cases."""
    return _json(dataset_catalog_payload())


@mcp.resource(
    "healthcare-data://datasets/{dataset_id}",
    name="dataset_metadata",
    description="Full metadata for a healthcare dataset by dataset_id.",
    mime_type="application/json",
)
def dataset_metadata(dataset_id: str) -> str:
    """Read full dataset metadata by dataset_id."""
    return _json(dataset_metadata_payload(dataset_id))


@mcp.resource(
    "healthcare-data://datasets/{dataset_id}/schema",
    name="dataset_schema",
    description="Schema grain, important fields, and join keys for a dataset.",
    mime_type="application/json",
)
def dataset_schema(dataset_id: str) -> str:
    """Read schema and join metadata by dataset_id."""
    return _json(dataset_schema_payload(dataset_id))


@mcp.resource(
    "healthcare-data://datasets/{dataset_id}/source",
    name="dataset_source",
    description="Source URLs, owning servers, and cache files for a dataset.",
    mime_type="application/json",
)
def dataset_source(dataset_id: str) -> str:
    """Read source and cache metadata by dataset_id."""
    return _json(dataset_source_payload(dataset_id))


@mcp.resource(
    "healthcare-data://cache/status",
    name="cache_status",
    description="Filesystem-only status of expected healthcare-data-mcp cache files.",
    mime_type="application/json",
)
def cache_status() -> str:
    """Inspect expected cache files without downloading or importing data loaders."""
    return _json(cache_status_payload())


@mcp.resource(
    "healthcare-data://runbooks/cache",
    name="cache_runbooks",
    description="List cache and source-audit runbooks for dataset discovery.",
    mime_type="application/json",
)
def cache_runbooks() -> str:
    """List available runbooks."""
    return _json(cache_runbooks_payload())


@mcp.resource(
    "healthcare-data://runbooks/{runbook_id}",
    name="runbook",
    description="Read a dataset discovery or cache runbook by id.",
    mime_type="application/json",
)
def runbook(runbook_id: str) -> str:
    """Read one runbook by id."""
    return _json(runbook_payload(runbook_id))


@mcp.prompt(
    name="healthcare_market_scan",
    description="Plan a market scan using system, facility, quality, claims, and demographics resources.",
)
def healthcare_market_scan(market: str, anchor_system: str = "", state: str = "") -> str:
    """Prompt for a healthcare market scan."""
    anchor = f" anchored on {anchor_system}" if anchor_system else ""
    state_hint = f" in {state}" if state else ""
    return (
        f"Prepare a healthcare market scan for {market}{state_hint}{anchor}. "
        "Start with healthcare-data://datasets/catalog, then use AHRQ system affiliation, "
        "CMS hospital general info, HSAF service area, CMS quality, claims PUFs, and ACS/CMS "
        "geography resources. Return recommended MCP tools, join keys, cache prerequisites, "
        "and a source-citation checklist."
    )


@mcp.prompt(
    name="hospital_competitive_profile",
    description="Build a competitive profile for a hospital or health system.",
)
def hospital_competitive_profile(ccn: str, market: str = "", competitors: str = "") -> str:
    """Prompt for hospital competitive profiling."""
    market_hint = f" in {market}" if market else ""
    competitor_hint = f" Compare against: {competitors}." if competitors else ""
    return (
        f"Build a competitive profile for hospital CCN {ccn}{market_hint}. "
        "Use CMS facility identity, POS beds/services, hospital quality, claims service-line mix, "
        "price transparency, and HSAF market-share datasets."
        f"{competitor_hint} Include dataset resource URIs, expected cache files, and join keys."
    )


@mcp.prompt(
    name="service_line_opportunity",
    description="Analyze market opportunity for a clinical service line.",
)
def service_line_opportunity(service_line: str, market: str, anchor_ccn: str = "") -> str:
    """Prompt for service-line opportunity analysis."""
    anchor = f" Anchor the analysis on CCN {anchor_ccn}." if anchor_ccn else ""
    return (
        f"Analyze {service_line} opportunity in {market}. "
        "Use claims PUF DRG/APC volume, hospital quality, service-area overlap, physician supply, "
        "drive-time access, and demographics resources."
        f"{anchor} Identify data gaps, cache prerequisites, and source URLs to cite."
    )


@mcp.prompt(
    name="referral_leakage_review",
    description="Plan physician referral leakage and network analysis.",
)
def referral_leakage_review(system_name: str, geography: str = "") -> str:
    """Prompt for referral leakage review."""
    geo_hint = f" for {geography}" if geography else ""
    return (
        f"Plan a referral leakage review for {system_name}{geo_hint}. "
        "Use AHRQ system discovery, NPPES, physician compare/utilization, DocGraph referral data, "
        "Dartmouth HSA/HRR, and claims service-line datasets. Flag manual-seed requirements and "
        "separate cached evidence from live API enrichment."
    )


@mcp.prompt(
    name="public_records_due_diligence",
    description="Plan public-records and compliance due diligence for a healthcare entity.",
)
def public_records_due_diligence(entity_name: str, ein: str = "", uei: str = "") -> str:
    """Prompt for public records due diligence."""
    identifiers = ", ".join(part for part in (f"EIN {ein}" if ein else "", f"UEI {uei}" if uei else "") if part)
    id_hint = f" ({identifiers})" if identifiers else ""
    return (
        f"Plan public-records due diligence for {entity_name}{id_hint}. "
        "Use financial-intelligence, public-records, CMS facility identity, CHPL, SAM.gov, "
        "USAspending, 340B, HIPAA breach, and web-intelligence resources. Return source URLs, "
        "API key requirements, manually seeded files, and cache status checks."
    )


if __name__ == "__main__":
    mcp.run(transport=_transport)
