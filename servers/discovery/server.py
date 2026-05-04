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
        "description": "Quality star ratings, adjacent HRRP/HAC summaries, and exact CMS row-level measure lookup for HCAHPS, complications, unplanned visits, and HAI.",
        "source_system": "CMS Provider Data Catalog",
        "source_urls": [
            "https://data.cms.gov/provider-data/api/1/datastore/query/9n3s-kdb3/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/yq43-i98g/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/dgck-syfz/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/ynj2-r877/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/77hc-ibv8/0/download?format=csv",
            "https://data.cms.gov/provider-data/api/1/datastore/query/632h-zaca/0/download?format=csv",
        ],
        "cache_files": [
            "hospital_quality_hrrp.csv",
            "hospital_quality_hac.csv",
            "hospital_quality_hcahps.csv",
            "hospital_quality_complications.csv",
            "hospital_quality_hai.csv",
            "hospital_quality_unplanned_visits.csv",
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
        "supports_exact_inventory": True,
        "source_status_tool": "hospital_quality.get_quality_measure_rows",
        "unsupported_assertions": [
            "PHC4 in-hospital mortality as CMS MORT_30_AMI",
            "HRRP condition readmissions as READM_30_HOSP_WIDE",
            "HAC totals as HAI_1_SIR",
        ],
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
        "description": "SAM.gov opportunities and exclusions, USAspending awards, CHPL certifications, HIPAA breaches, state notice source status, cyber source boundaries, and HHS OIG LEIE screening.",
        "source_system": "SAM.gov, USAspending, CHPL, HHS OCR, HHS OIG",
        "source_urls": [
            "https://api.sam.gov/prod/opportunities/v2/search",
            "https://open.gsa.gov/api/exclusions-api/",
            "https://api.usaspending.gov/api/v2",
            "https://chpl.healthit.gov/rest/certification_ids/{cehrt_id}",
            "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
            "https://oig.hhs.gov/exclusions/exclusions_list.asp",
        ],
        "cache_files": [
            "public-records/hipaa_breaches.parquet",
            "public-records/state_breach_notices.parquet",
            "public-records/hipaa_breaches.csv",
        ],
        "schema": {
            "identity_fields": ["entity_name", "ein", "uei", "cehrt_id"],
            "common_fields": ["award_id", "obligation", "breach_date", "covered_entity_type", "certification_status"],
            "join_keys": ["entity_name", "ein", "uei", "ccn"],
        },
        "workflows": ["contracting scan", "compliance check", "public-records due diligence"],
        "supports_exact_inventory": True,
        "requires_import": ["hipaa_breaches.csv", "state_breach_notices.parquet"],
        "source_status_tool": "public_records.get_cyber_attestation_source_status",
        "unsupported_assertions": [
            "CMS Promoting Interoperability rows as broad cybersecurity attestation",
            "CISA KEV as victim attribution",
            "state AG notice coverage as a national cache",
        ],
    },
    "state_health_data": {
        "title": "State Public Hospital Data Acquisition Index",
        "server": ["public-records", "workforce-analytics", "financial-intelligence"],
        "category": "state_public_data",
        "grain": "public source artifact, normalized metadata row, or facility-year public metric",
        "description": "Shared acquisition indexes for PA, NJ, and DE public hospital artifacts used by public financial, staffing, and throughput tools.",
        "source_system": "PA DOH, NJ DOH, Delaware DHSS, PHC4, and AHRQ public artifacts",
        "source_urls": [
            "https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports.html",
            "https://www.nj.gov/health/hcf/financial-reports/",
            "https://dhss.delaware.gov/dph/hp/hosp_dis_data/",
            "https://www.phc4.org/reports-library/",
            "https://www.ahrq.gov/data/innovations/hfmd.html",
        ],
        "cache_files": [
            "state-health-data/pa-hospital-reports/artifact_index.json",
            "state-health-data/nj-hospital-public-data/artifact_index.json",
            "state-health-data/de-hospital-discharge/artifact_index.json",
        ],
        "schema": {
            "identity_fields": ["state", "facility_name", "state_facility_id", "ccn"],
            "common_fields": ["source_name", "artifact_url", "report_year", "metric_name", "metric_value", "confidence"],
            "join_keys": ["ccn", "facility_name", "state_facility_id", "state"],
        },
        "workflows": ["state public data audit", "public throughput profile", "public financial enrichment"],
    },
    "phc4_public_reports": {
        "title": "PHC4 Public Reports",
        "server": ["public-records"],
        "category": "state_public_reports",
        "grain": "PHC4 public report artifact or extracted table row",
        "description": "Indexed public PHC4 Hospital Performance, Financial Analysis, Common Procedures, rural, and special reports. Paid PHC4 discharge files are not used.",
        "source_system": "Pennsylvania Health Care Cost Containment Council public reports",
        "source_urls": ["https://www.phc4.org/reports-library/"],
        "cache_files": [
            "state-health-data/phc4/report_index.json",
            "state-health-data/phc4/report_index.parquet",
            "state-health-data/phc4/tables",
        ],
        "schema": {
            "identity_fields": ["report_title", "hospital_name", "procedure"],
            "common_fields": ["report_type", "report_year", "measure_name", "measure_value", "page", "table_index", "confidence"],
            "join_keys": ["hospital_name", "report_year", "procedure"],
        },
        "workflows": ["PHC4 public report search", "hospital performance public report lookup", "financial report citation"],
    },
    "ahrq_hfmd": {
        "title": "AHRQ Hospital Financial Measures Database",
        "server": ["financial-intelligence"],
        "category": "financial_health",
        "grain": "hospital-year financial measure row",
        "description": "AHRQ HFMD public financial ratios and measures derived from CMS Hospital Cost Report Public Use Files.",
        "source_system": "AHRQ HFMD",
        "source_urls": ["https://www.ahrq.gov/data/innovations/hfmd.html"],
        "cache_files": [
            "state-health-data/ahrq-hfmd",
            "state-health-data/ahrq-hfmd/*.csv",
            "state-health-data/ahrq-hfmd/*.zip",
        ],
        "schema": {
            "identity_fields": ["provider_id", "ccn", "hospital_name"],
            "common_fields": ["fiscal_year", "measure_name", "measure_value", "confidence"],
            "join_keys": ["provider_id", "ccn"],
        },
        "workflows": ["public financial health profile", "financial ratio benchmarking"],
    },
    "pa_hospital_reports": {
        "title": "Pennsylvania DOH Hospital Reports",
        "server": ["workforce-analytics"],
        "category": "state_public_operations",
        "grain": "PA hospital report artifact or facility-year public operations metric",
        "description": "Pennsylvania public hospital reports for utilization, staffed beds, occupancy, ED visits, and procedure volume where extractable.",
        "source_system": "Pennsylvania Department of Health",
        "source_urls": ["https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports.html"],
        "cache_files": [
            "state-health-data/pa-hospital-reports/artifact_index.json",
            "state-health-data/pa-hospital-reports/artifact_metadata.csv",
        ],
        "schema": {
            "identity_fields": ["facility_name", "state_facility_id", "ccn"],
            "common_fields": ["report_year", "metric_name", "metric_value", "page", "table", "confidence"],
            "join_keys": ["ccn", "facility_name", "state_facility_id"],
        },
        "workflows": ["PA throughput profile", "PA staffing productivity enhancement"],
    },
    "nj_hospital_public_data": {
        "title": "New Jersey Hospital Public Data",
        "server": ["financial-intelligence", "workforce-analytics"],
        "category": "state_public_financial",
        "grain": "NJ public hospital financial, charity-care, or utilization artifact row",
        "description": "New Jersey public hospital financial reports, charity-care reports, and accessible public hospital artifacts.",
        "source_system": "New Jersey Department of Health",
        "source_urls": [
            "https://www.nj.gov/health/hcf/financial-reports/",
            "https://www.nj.gov/health/charitycare/subsidy-reports/",
        ],
        "cache_files": [
            "state-health-data/nj-hospital-public-data/artifact_index.json",
            "state-health-data/nj-hospital-public-data/artifact_metadata.csv",
        ],
        "schema": {
            "identity_fields": ["facility_name", "state_facility_id", "ccn"],
            "common_fields": ["report_year", "metric_name", "metric_value", "source_name", "confidence"],
            "join_keys": ["ccn", "facility_name", "state_facility_id"],
        },
        "workflows": ["NJ public financial profile", "NJ charity-care enrichment"],
    },
    "de_hospital_discharge": {
        "title": "Delaware Hospital Discharge Public Data",
        "server": ["workforce-analytics"],
        "category": "state_public_operations",
        "grain": "DE public hospital discharge artifact or facility-year utilization row",
        "description": "Delaware public hospital discharge and utilization summaries where structured fields are available.",
        "source_system": "Delaware DHSS",
        "source_urls": ["https://dhss.delaware.gov/dph/hp/hosp_dis_data/"],
        "cache_files": [
            "state-health-data/de-hospital-discharge/artifact_index.json",
            "state-health-data/de-hospital-discharge/artifact_metadata.csv",
        ],
        "schema": {
            "identity_fields": ["facility_name", "state_facility_id", "ccn"],
            "common_fields": ["report_year", "discharge_volume", "metric_name", "metric_value", "confidence"],
            "join_keys": ["ccn", "facility_name", "state_facility_id"],
        },
        "workflows": ["DE discharge profile", "public throughput context"],
    },
    "cms_pecos_public_provider_enrollment": {
        "title": "CMS PECOS Medicare Fee-For-Service Public Provider Enrollment",
        "server": ["provider-enrollment"],
        "category": "provider_enrollment",
        "grain": "one row per public Medicare enrollment record",
        "description": "PECOS-derived provider enrollment rows with NPI, PAC ID, enrollment ID, provider type, names, and location fields.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/medicare-fee-for-service-public-provider-enrollment",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/medicare_ffs_public_provider_enrollment.parquet",
            "provider-enrollment/medicare_ffs_public_provider_enrollment.meta.json",
        ],
        "schema": {
            "identity_fields": ["npi", "pac_id", "enrollment_id", "associate_id"],
            "common_fields": [
                "provider_name",
                "provider_type",
                "state",
                "city",
                "zip_code",
                "enrollment_status",
                "original_columns",
            ],
            "join_keys": ["npi", "pac_id", "enrollment_id", "state"],
        },
        "workflows": ["provider enrollment lookup", "NPI enrichment", "compliance screening joins"],
    },
    "cms_pecos_hospital_enrollments": {
        "title": "CMS PECOS Hospital Enrollments",
        "server": ["provider-enrollment"],
        "category": "provider_enrollment",
        "grain": "one row per hospital enrollment record",
        "description": "Hospital enrollment details used to connect CCNs, enrollment IDs, and provider-control profiles.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/hospital-enrollments",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/hospital_enrollments.parquet",
            "provider-enrollment/hospital_enrollments.meta.json",
        ],
        "schema": {
            "identity_fields": ["ccn", "npi", "enrollment_id", "associate_id"],
            "common_fields": [
                "facility_name",
                "organization_name",
                "provider_type",
                "state",
                "city",
                "enrollment_status",
            ],
            "join_keys": ["ccn", "npi", "enrollment_id", "facility_name"],
        },
        "workflows": ["facility enrollment detail", "ownership graph joins", "provider-control profiles"],
    },
    "cms_pecos_hospital_owners": {
        "title": "CMS PECOS Hospital All Owners",
        "server": ["provider-enrollment"],
        "category": "provider_ownership",
        "grain": "one row per hospital owner or management-control relationship",
        "description": "Hospital ownership and managing-control relationships with owner identity, role, association, and ownership-percentage fields.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/hospital-all-owners",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/hospital_all_owners.parquet",
            "provider-enrollment/hospital_all_owners.meta.json",
        ],
        "schema": {
            "identity_fields": ["enrollment_id", "owner_pac_id", "owner_associate_id", "owner_name"],
            "common_fields": [
                "facility_name",
                "owner_type",
                "role_code",
                "role_text",
                "association_date",
                "percentage_ownership",
                "state",
            ],
            "join_keys": ["enrollment_id", "owner_pac_id", "owner_name", "ccn"],
        },
        "workflows": ["ownership due diligence", "management-control tracing", "private-equity flag review"],
    },
    "cms_pecos_hospital_chow": {
        "title": "CMS PECOS Hospital Change of Ownership",
        "server": ["provider-enrollment"],
        "category": "provider_ownership",
        "grain": "one row per hospital change-of-ownership event or linked CHOW owner row",
        "description": "Hospital CHOW event history and linked owner information for acquisition and control-change review.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/hospital-change-of-ownership",
            "https://data.cms.gov/provider-enrollment/hospital-change-of-ownership-owner-information",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/hospital_chow.parquet",
            "provider-enrollment/hospital_chow.meta.json",
            "provider-enrollment/hospital_chow_owner_information.parquet",
            "provider-enrollment/hospital_chow_owner_information.meta.json",
        ],
        "schema": {
            "identity_fields": ["ccn", "enrollment_id", "chow_date", "owner_name"],
            "common_fields": [
                "facility_name",
                "state",
                "transaction_type",
                "effective_date",
                "prior_owner",
                "new_owner",
            ],
            "join_keys": ["ccn", "enrollment_id", "owner_name", "state"],
        },
        "workflows": ["CHOW history", "acquisition diligence", "control-change profile"],
    },
    "cms_pecos_snf_enrollments": {
        "title": "CMS PECOS Skilled Nursing Facility Enrollments",
        "server": ["provider-enrollment"],
        "category": "provider_enrollment",
        "grain": "one row per skilled nursing facility enrollment record",
        "description": "SNF enrollment details used to connect CCNs, enrollment IDs, and provider-control profiles.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/skilled-nursing-facility-enrollments",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/snf_enrollments.parquet",
            "provider-enrollment/snf_enrollments.meta.json",
        ],
        "schema": {
            "identity_fields": ["ccn", "npi", "enrollment_id", "associate_id"],
            "common_fields": [
                "facility_name",
                "organization_name",
                "provider_type",
                "state",
                "city",
                "enrollment_status",
            ],
            "join_keys": ["ccn", "npi", "enrollment_id", "facility_name"],
        },
        "workflows": ["SNF enrollment detail", "ownership graph joins", "provider-control profiles"],
    },
    "cms_pecos_snf_owners": {
        "title": "CMS PECOS Skilled Nursing Facility All Owners",
        "server": ["provider-enrollment"],
        "category": "provider_ownership",
        "grain": "one row per skilled nursing facility owner or management-control relationship",
        "description": "SNF ownership and managing-control relationships with owner identity, role, association, and ownership-percentage fields.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/skilled-nursing-facility-all-owners",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/snf_all_owners.parquet",
            "provider-enrollment/snf_all_owners.meta.json",
        ],
        "schema": {
            "identity_fields": ["enrollment_id", "owner_pac_id", "owner_associate_id", "owner_name"],
            "common_fields": [
                "facility_name",
                "owner_type",
                "role_code",
                "role_text",
                "association_date",
                "percentage_ownership",
                "state",
            ],
            "join_keys": ["enrollment_id", "owner_pac_id", "owner_name", "ccn"],
        },
        "workflows": ["SNF ownership due diligence", "management-control tracing", "private-equity flag review"],
    },
    "cms_pecos_snf_chow": {
        "title": "CMS PECOS Skilled Nursing Facility Change of Ownership",
        "server": ["provider-enrollment"],
        "category": "provider_ownership",
        "grain": "one row per skilled nursing facility change-of-ownership event or linked CHOW owner row",
        "description": "SNF CHOW event history and linked owner information for acquisition and control-change review.",
        "source_system": "CMS Provider Enrollment datasets",
        "source_urls": [
            "https://data.cms.gov/provider-enrollment/skilled-nursing-facility-change-of-ownership",
            "https://data.cms.gov/provider-enrollment/skilled-nursing-facility-change-of-ownership-owner-information",
            "https://data.cms.gov/data.json",
        ],
        "cache_files": [
            "provider-enrollment/snf_chow.parquet",
            "provider-enrollment/snf_chow.meta.json",
            "provider-enrollment/snf_chow_owner_information.parquet",
            "provider-enrollment/snf_chow_owner_information.meta.json",
        ],
        "schema": {
            "identity_fields": ["ccn", "enrollment_id", "chow_date", "owner_name"],
            "common_fields": [
                "facility_name",
                "state",
                "transaction_type",
                "effective_date",
                "prior_owner",
                "new_owner",
            ],
            "join_keys": ["ccn", "enrollment_id", "owner_name", "state"],
        },
        "workflows": ["SNF CHOW history", "acquisition diligence", "control-change profile"],
    },
    "cdc_places": {
        "title": "CDC PLACES: Local Data for Better Health",
        "server": ["community-health"],
        "category": "community_health",
        "grain": "measure rows for county, place, census tract, or ZCTA geography",
        "description": "CDC PLACES model-based community health estimates with confidence intervals, populations, and measure metadata.",
        "source_system": "CDC PLACES via Socrata",
        "source_urls": [
            "https://www.cdc.gov/places/tools/data-portal.html",
            "https://api.us.socrata.com/api/catalog/v1",
            "https://data.cdc.gov/",
        ],
        "cache_files": [
            "community-health/places_county.parquet",
            "community-health/places_place.parquet",
            "community-health/places_tract.parquet",
            "community-health/places_zcta.parquet",
            "community-health/places_*.meta.json",
        ],
        "schema": {
            "identity_fields": ["location_id", "measure_id", "data_value_type", "year"],
            "common_fields": [
                "state",
                "location_name",
                "category",
                "measure",
                "data_value",
                "low_confidence_limit",
                "high_confidence_limit",
                "total_population",
            ],
            "join_keys": ["location_id", "state", "county_fips", "zcta"],
        },
        "workflows": ["community health profile", "service-area health context", "market needs comparison"],
    },
    "nih_reporter_projects": {
        "title": "NIH RePORTER Projects",
        "server": ["research-trials"],
        "category": "research_activity",
        "grain": "one row per NIH-funded project result",
        "description": "NIH RePORTER project search and profile metadata with award amounts, fiscal years, organizations, PIs, institutes, and terms.",
        "source_system": "NIH RePORTER API v2",
        "source_urls": [
            "https://api.reporter.nih.gov/",
            "https://api.reporter.nih.gov/v2/projects/search",
            "https://api.reporter.nih.gov/v2/publications/search",
        ],
        "cache_files": [],
        "schema": {
            "identity_fields": ["project_num", "appl_id", "org_uei", "org_name"],
            "common_fields": [
                "project_title",
                "fiscal_year",
                "award_amount",
                "principal_investigators",
                "funding_mechanism",
                "agency_ic_admin",
                "terms",
            ],
            "join_keys": ["org_name", "org_uei", "project_num", "appl_id"],
        },
        "workflows": ["research funding profile", "PI discovery", "organization research activity"],
    },
    "clinicaltrials_gov": {
        "title": "ClinicalTrials.gov Studies",
        "server": ["research-trials"],
        "category": "research_activity",
        "grain": "one row per clinical study",
        "description": "ClinicalTrials.gov v2 study search/detail plus conservative sponsor and site inventories with role, geography, and ambiguity metadata.",
        "source_system": "ClinicalTrials.gov API v2",
        "source_urls": [
            "https://clinicaltrials.gov/data-api/api",
            "https://clinicaltrials.gov/api/v2/version",
            "https://clinicaltrials.gov/api/v2/studies",
        ],
        "cache_files": [],
        "schema": {
            "identity_fields": ["nct_id", "brief_title", "sponsor"],
            "common_fields": [
                "overall_status",
                "phase",
                "conditions",
                "interventions",
                "locations",
                "start_date",
                "completion_date",
            ],
            "join_keys": ["nct_id", "sponsor", "org_name", "location"],
        },
        "workflows": ["trial search", "sponsor activity profile", "research-market scan"],
        "supports_exact_inventory": True,
        "source_status_tool": "research_trials.inventory_clinical_trial_sponsors",
        "unsupported_assertions": [
            "raw sponsor search results as deduped sponsor inventory",
            "facility-name-only location aggregation across cities/states",
        ],
    },
    "hhs_oig_leie": {
        "title": "HHS OIG List of Excluded Individuals/Entities",
        "server": ["public-records"],
        "category": "regulatory_compliance",
        "grain": "one row per currently excluded individual or entity",
        "description": "Current HHS OIG LEIE screening file for provider, vendor, and enrollment exclusion checks. Results are screening support, not a final legal determination.",
        "source_system": "HHS Office of Inspector General",
        "source_urls": [
            "https://oig.hhs.gov/exclusions/exclusions_list.asp",
            "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv",
            "https://www.oig.hhs.gov/exclusions/files/leie_record_layout.pdf",
        ],
        "cache_files": [
            "public-records/leie_current.csv",
            "public-records/leie_current.parquet",
            "public-records/leie_current.meta.json",
        ],
        "cache_ttl_days": 31,
        "schema": {
            "identity_fields": ["LASTNAME", "FIRSTNAME", "BUSNAME", "NPI", "DOB"],
            "common_fields": ["GENERAL", "SPECIALTY", "EXCLTYPE", "EXCLDATE", "WAIVERDATE", "WVRSTATE"],
            "join_keys": ["npi", "name", "state"],
        },
        "workflows": ["provider screening", "vendor screening", "enrollment checks", "monthly exclusion monitoring"],
    },
    "sam_gov_exclusions": {
        "title": "SAM.gov Exclusions",
        "server": ["public-records"],
        "category": "regulatory_compliance",
        "grain": "one row per active SAM.gov exclusion record returned by the v4 API",
        "description": "SAM.gov entity-information exclusions screening for entities and individuals using public API v4 JSON results.",
        "source_system": "SAM.gov Entity Information API",
        "source_urls": [
            "https://open.gsa.gov/api/exclusions-api/",
            "https://api.sam.gov/entity-information/v4/exclusions",
        ],
        "cache_files": [
            "public-records/api_sam_exclusions_*.json",
        ],
        "schema": {
            "identity_fields": ["uei", "cage_code", "npi", "entity_name", "first_name", "last_name"],
            "common_fields": [
                "classification",
                "exclusion_type",
                "excluding_agency",
                "state",
                "country",
                "activation_date",
                "termination_date",
            ],
            "join_keys": ["uei", "cage_code", "npi", "entity_name", "name"],
        },
        "workflows": ["vendor screening", "contracting exclusion checks", "federal award due diligence"],
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
        "description": "HRSA HPSA, CMS PBJ/HCRIS staffing, import-backed ACGME public program inventory, NLRB cases, and BLS stoppages.",
        "source_system": "HRSA, CMS, ACGME, NLRB, BLS",
        "source_urls": [
            "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv",
            "https://data.cms.gov/data-api/v1/dataset/7e0d53ba-8f02-4c66-98a5-14a1c997c50d/data",
            "https://support.acgmecloud.org/hc/en-us/articles/31576594571927-Explore-Public-Data-Programs",
            "https://apps.acgme-i.org/ads/Public/Request/GetDataDictionary",
            "https://github.com/labordata/nlrb-data/releases/download/nightly/nlrb.db.zip",
            "https://download.bls.gov/pub/time.series/ws/ws.data.1.AllData",
        ],
        "cache_files": [
            "workforce/hpsa.parquet",
            "workforce/hcris_staffing.parquet",
            "workforce/acgme_programs.csv",
            "workforce/acgme_programs.meta.json",
            "workforce/nlrb.db",
            "workforce/work_stoppages.parquet",
        ],
        "schema": {
            "identity_fields": ["hpsa_id", "provider_ccn", "case_number", "series_id"],
            "common_fields": ["state", "county", "discipline", "hpsa_score", "staffing_metric", "union_name"],
            "join_keys": ["state", "county_fips", "provider_ccn"],
        },
        "workflows": ["shortage analysis", "labor risk scan", "residency pipeline", "staffing benchmark"],
        "supports_exact_inventory": True,
        "requires_import": ["workforce/acgme_programs.csv"],
        "source_status_tool": "workforce_analytics.get_acgme_source_status",
        "unsupported_assertions": ["complete live ACGME inventory without a ready imported public export"],
    },
}


CACHE_ENTRIES: tuple[dict[str, Any], ...] = tuple(
    {
        "dataset_id": dataset_id,
        "relative_path": relative_path,
        "ttl_days": dataset.get("cache_ttl_days", 90)
        if "{ccn-or-domain}" not in relative_path and "*" not in relative_path
        else None,
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
            "Place hipaa_breaches.csv under public-records cache before breach lookups.",
            "Run healthcare-data://cache/status to confirm the converted Parquet files exist.",
        ],
        "manual_seed_files": [
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
                "supports_exact_inventory": bool(dataset.get("supports_exact_inventory", False)),
                "requires_import": dataset.get("requires_import", []),
                "source_status_tool": dataset.get("source_status_tool", ""),
                "unsupported_assertions": dataset.get("unsupported_assertions", []),
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
        "requires_import": dataset.get("requires_import", []),
        "source_status_tool": dataset.get("source_status_tool", ""),
        "unsupported_assertions": dataset.get("unsupported_assertions", []),
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


@mcp.tool(structured_output=True)
async def list_datasets(query: str = "", server: str = "", tag: str = "", limit: int = 50) -> dict[str, Any]:
    """List dataset catalog entries with optional text, server, and category filters."""

    payload = dataset_catalog_payload()
    query_token = query.strip().lower()
    server_token = server.strip().lower()
    tag_token = tag.strip().lower()
    try:
        parsed_limit = int(limit or 50)
    except (TypeError, ValueError):
        parsed_limit = 50
    bounded_limit = max(1, min(parsed_limit, 200))

    datasets = []
    for dataset in payload["datasets"]:
        haystack = " ".join(
            [
                dataset["dataset_id"],
                dataset["title"],
                dataset["category"],
                dataset["grain"],
                dataset["source_system"],
                " ".join(dataset["server"]),
                " ".join(dataset["workflows"]),
            ]
        ).lower()
        if query_token and query_token not in haystack:
            continue
        if server_token and server_token not in {item.lower() for item in dataset["server"]}:
            continue
        if tag_token and tag_token not in dataset["category"].lower() and tag_token not in haystack:
            continue
        datasets.append(dataset)

    return {
        **{key: value for key, value in payload.items() if key != "datasets"},
        "query": query.strip(),
        "server": server.strip(),
        "tag": tag.strip(),
        "limit": bounded_limit,
        "matched_count": len(datasets),
        "datasets": datasets[:bounded_limit],
    }


@mcp.tool(structured_output=True)
async def get_dataset(dataset_id: str) -> dict[str, Any]:
    """Return full metadata for one dataset by dataset_id."""

    return dataset_metadata_payload(dataset_id)


@mcp.tool(structured_output=True)
async def get_dataset_schema(dataset_id: str) -> dict[str, Any]:
    """Return schema, grain, and join-key metadata for one dataset."""

    return dataset_schema_payload(dataset_id)


@mcp.tool(structured_output=True)
async def get_dataset_source(dataset_id: str) -> dict[str, Any]:
    """Return source URLs and expected cache files for one dataset."""

    return dataset_source_payload(dataset_id)


@mcp.tool(structured_output=True)
async def get_cache_status() -> dict[str, Any]:
    """Return filesystem-only cache status for expected dataset cache files."""

    return cache_status_payload()


@mcp.tool(structured_output=True)
async def list_runbooks() -> dict[str, Any]:
    """List available discovery and cache runbooks."""

    return cache_runbooks_payload()


@mcp.tool(structured_output=True)
async def get_runbook(runbook_id: str) -> dict[str, Any]:
    """Return one discovery/cache runbook by id."""

    return runbook_payload(runbook_id)


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
        "USAspending, HIPAA breach, and web-intelligence resources. Return source URLs, "
        "API key requirements, manually seeded files, and cache status checks."
    )


if __name__ == "__main__":
    mcp.run(transport=_transport)
