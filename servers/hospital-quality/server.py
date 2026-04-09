"""Hospital Quality & Performance MCP Server.

Provides tools for hospital quality metrics, readmission data, safety scores,
patient experience, and financial profiling from public CMS data.
"""

import json
import logging
import os

from mcp.server.fastmcp import FastMCP

from . import data_loaders
from .models import (
    ComplicationRecord,
    ComplicationsData,
    ConditionReadmission,
    DomainScores,
    ExperienceDomain,
    FinancialProfile,
    PatientExperience,
    QualityScores,
    ReadmissionData,
    SafetyScores,
)

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "hospital-quality"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8005"))
mcp = FastMCP(**_mcp_kwargs)


def _col(df, *candidates, default=""):
    """Find the first matching column name in a DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return default


def _safe_float(val) -> float | None:
    """Parse a string to float, returning None on failure."""
    if not val or str(val).strip().lower() in ("", "not available", "n/a", "too few"):
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Parse a string to int, returning None on failure."""
    f = _safe_float(val)
    return int(f) if f is not None else None


def _find_ccn_col(df):
    """Find the CCN/facility_id column in a DataFrame."""
    return _col(df, "facility_id", "ccn", "provider_id", "provider_ccn",
                "cms_certification_number", "provider_number", "prvdr_num")


def _filter_by_ccn(df, ccn: str):
    """Filter a DataFrame to rows matching the given CCN."""
    ccn_col = _find_ccn_col(df)
    if not ccn_col:
        return df.head(0)
    return df[df[ccn_col].str.strip() == ccn.strip()]


# ---------------------------------------------------------------------------
# Tool: get_quality_scores
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_quality_scores(ccn: str) -> str:
    """Get overall quality ratings for a hospital from CMS Hospital General Info.

    Returns star ratings (1-5) and national comparison ratings for mortality,
    safety, readmission, patient experience, and timeliness of care.

    Args:
        ccn: The 6-character CMS Certification Number (e.g. "050454").
    """
    df = await data_loaders.load_hospital_info()
    if df.empty:
        return json.dumps({"error": "Hospital General Info data not available"})

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return json.dumps({"error": f"No hospital found with CCN: {ccn}"})

    row = matches.iloc[0]

    def val(*keys):
        for k in keys:
            if k in row.index and row[k]:
                return str(row[k]).strip()
        return ""

    result = QualityScores(
        ccn=ccn,
        facility_name=val("facility_name", "hospital_name", "provider_name"),
        overall_rating=val("hospital_overall_rating", "overall_rating", "overall_quality_star_rating"),
        mortality_national_comparison=val("mortality_national_comparison", "mortality_rating"),
        safety_national_comparison=val("safety_of_care_national_comparison", "safety_rating"),
        readmission_national_comparison=val("readmission_national_comparison", "readmission_rating"),
        patient_experience_national_comparison=val("patient_experience_national_comparison", "patient_experience_rating"),
        timeliness_national_comparison=val("timeliness_of_care_national_comparison", "timeliness_rating"),
    )
    return json.dumps(result.model_dump())


# ---------------------------------------------------------------------------
# Tool: get_readmission_data
# ---------------------------------------------------------------------------

# HRRP measure IDs map to condition abbreviations
_HRRP_MEASURES = {
    "READM-30-AMI-HRRP": "AMI",
    "READM-30-HF-HRRP": "HF",
    "READM-30-PN-HRRP": "PN",
    "READM-30-COPD-HRRP": "COPD",
    "READM-30-HIP-KNEE-HRRP": "HIP_KNEE",
    "READM-30-CABG-HRRP": "CABG",
}


@mcp.tool()
async def get_readmission_data(ccn: str) -> str:
    """Get Hospital Readmissions Reduction Program (HRRP) data for a hospital.

    Returns excess readmission ratios, predicted/expected readmission rates,
    discharge and readmission counts per condition (AMI, HF, PN, COPD, HIP_KNEE,
    CABG), and the payment reduction percentage.

    Args:
        ccn: The 6-character CMS Certification Number.
    """
    df = await data_loaders.load_hrrp()
    if df.empty:
        return json.dumps({"error": "HRRP data not available"})

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return json.dumps({"error": f"No HRRP data found for CCN: {ccn}"})

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(matches.iloc[0][name_col]).strip()

    # Parse per-condition readmission rows
    measure_col = _col(matches, "measure_id", "measure_name", "hrrp_measure_id")
    conditions = []

    if measure_col:
        for _, row in matches.iterrows():
            measure_raw = str(row.get(measure_col, "")).strip().upper()
            condition = _HRRP_MEASURES.get(measure_raw, "")
            if not condition:
                # Try partial matching
                for key, abbr in _HRRP_MEASURES.items():
                    if abbr in measure_raw or key in measure_raw:
                        condition = abbr
                        break
                if not condition:
                    condition = measure_raw

            conditions.append(ConditionReadmission(
                measure=condition,
                excess_readmission_ratio=_safe_float(row.get("excess_readmission_ratio", "")),
                predicted_readmission_rate=_safe_float(row.get("predicted_readmission_rate", "")),
                expected_readmission_rate=_safe_float(row.get("expected_readmission_rate", "")),
                number_of_discharges=_safe_int(row.get("number_of_discharges", "")),
                number_of_readmissions=_safe_int(row.get("number_of_readmissions", "")),
            ))
    else:
        # Flat layout: columns per condition
        for abbr in ("AMI", "HF", "PN", "COPD", "HIP_KNEE", "CABG"):
            prefix = abbr.lower()
            conditions.append(ConditionReadmission(
                measure=abbr,
                excess_readmission_ratio=_safe_float(
                    matches.iloc[0].get(f"{prefix}_excess_readmission_ratio",
                                        matches.iloc[0].get(f"excess_readmission_ratio_{prefix}", ""))
                ),
                number_of_discharges=_safe_int(
                    matches.iloc[0].get(f"{prefix}_number_of_discharges",
                                        matches.iloc[0].get(f"number_of_discharges_{prefix}", ""))
                ),
            ))

    # Payment reduction: derived from payment_adjustment_factor or payment_reduction columns
    payment_reduction = None
    paf_col = _col(matches, "payment_adjustment_factor", "payment_reduction_percentage",
                   "peer_group_value", "payment_reduction")
    if paf_col:
        paf_val = _safe_float(matches.iloc[0].get(paf_col, ""))
        if paf_val is not None:
            if paf_val <= 1.0 and "factor" in paf_col:
                # Payment adjustment factor: 0.9970 means 0.30% reduction
                payment_reduction = round((1.0 - paf_val) * 100, 4)
            else:
                payment_reduction = paf_val

    result = ReadmissionData(
        ccn=ccn,
        facility_name=facility_name,
        conditions=conditions,
        payment_reduction_percentage=payment_reduction,
    )
    return json.dumps(result.model_dump())


# ---------------------------------------------------------------------------
# Tool: get_safety_scores
# ---------------------------------------------------------------------------

# HAC domain column name mappings (try multiple naming conventions)
_HAC_DOMAINS = {
    "psi90": ("psi_90_safety", "psi90", "psi_90", "cms_psi_90"),
    "clabsi": ("clabsi", "hai_1_clabsi", "central_line_associated_bloodstream_infection"),
    "cauti": ("cauti", "hai_2_cauti", "catheter_associated_urinary_tract_infection"),
    "ssi_colon": ("ssi_colon", "hai_3_ssi_colon", "ssi_abdominal"),
    "ssi_hyst": ("ssi_hyst", "hai_4_ssi_hyst", "ssi_hysterectomy"),
    "mrsa": ("mrsa", "hai_5_mrsa", "mrsa_bacteremia"),
    "cdi": ("cdi", "hai_6_cdi", "c_diff", "clostridium_difficile"),
}


@mcp.tool()
async def get_safety_scores(ccn: str) -> str:
    """Get Hospital-Acquired Condition (HAC) Reduction Program safety scores.

    Returns total HAC score, payment reduction status, and domain scores
    for PSI-90, CLABSI, CAUTI, SSI (colon/hyst), MRSA, and CDI.

    Args:
        ccn: The 6-character CMS Certification Number.
    """
    df = await data_loaders.load_hac()
    if df.empty:
        return json.dumps({"error": "HAC Reduction Program data not available"})

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return json.dumps({"error": f"No HAC data found for CCN: {ccn}"})

    row = matches.iloc[0]

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(row[name_col]).strip()

    total_col = _col(matches, "total_hac_score", "total_score", "hac_score")
    reduction_col = _col(matches, "payment_reduction", "payment_reduction_indicator",
                         "hac_payment_reduction")

    # Build domain scores by searching for matching columns
    domain_kwargs = {}
    for domain_key, candidates in _HAC_DOMAINS.items():
        val = None
        for candidate in candidates:
            # Try exact and with common suffixes
            for col_try in (candidate, f"{candidate}_score", f"{candidate}_measure"):
                if col_try in row.index:
                    val = _safe_float(row[col_try])
                    if val is not None:
                        break
            if val is not None:
                break
        domain_kwargs[domain_key] = val

    result = SafetyScores(
        ccn=ccn,
        facility_name=facility_name,
        total_hac_score=_safe_float(row.get(total_col, "")) if total_col else None,
        payment_reduction=str(row.get(reduction_col, "")).strip() if reduction_col else "",
        domain_scores=DomainScores(**domain_kwargs),
    )
    return json.dumps(result.model_dump())


# ---------------------------------------------------------------------------
# Tool: get_patient_experience
# ---------------------------------------------------------------------------

# HCAHPS measure ID prefix to domain name mapping
_HCAHPS_DOMAINS = {
    "H_COMP_1": "nurse_communication",
    "H_COMP_2": "doctor_communication",
    "H_COMP_3": "staff_responsiveness",
    "H_COMP_4": "pain_management",
    "H_COMP_5": "medicine_communication",
    "H_COMP_6": "discharge_info",
    "H_COMP_7": "care_transition",
    "H_CLEAN": "cleanliness",
    "H_QUIET": "quietness",
    "H_HSP_RATING": "overall_rating",
    "H_RECMND": "recommend",
}

# Star rating measure suffixes
_STAR_SUFFIX = "_STAR_RATING"
# Answer percent measure suffixes for top/middle/bottom box
_LINEAR_SCORE = "_LINEAR_SCORE"


@mcp.tool()
async def get_patient_experience(ccn: str) -> str:
    """Get HCAHPS patient experience survey scores for a hospital.

    Returns star ratings and response percentages for domains: nurse/doctor
    communication, staff responsiveness, pain management, medicine communication,
    discharge info, care transition, cleanliness, quietness, overall rating,
    and recommendation.

    Args:
        ccn: The 6-character CMS Certification Number.
    """
    df = await data_loaders.load_hcahps()
    if df.empty:
        return json.dumps({"error": "HCAHPS data not available"})

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return json.dumps({"error": f"No HCAHPS data found for CCN: {ccn}"})

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(matches.iloc[0][name_col]).strip()

    measure_col = _col(matches, "hcahps_measure_id", "measure_id", "measure_name")
    star_col = _col(matches, "patient_survey_star_rating", "star_rating", "hcahps_star_rating")
    answer_pct_col = _col(matches, "hcahps_answer_percent", "answer_percent", "percent")
    answer_desc_col = _col(matches, "hcahps_answer_description", "answer_description")
    response_rate_col = _col(matches, "survey_response_rate_percent", "response_rate")
    num_surveys_col = _col(matches, "number_of_completed_surveys", "completed_surveys",
                           "num_completed_surveys")

    # Get survey response rate and completed surveys from any row
    survey_response_rate = ""
    num_completed_surveys = ""
    if response_rate_col:
        val = str(matches.iloc[0].get(response_rate_col, "")).strip()
        if val and val.lower() not in ("not available", "n/a"):
            survey_response_rate = val
    if num_surveys_col:
        val = str(matches.iloc[0].get(num_surveys_col, "")).strip()
        if val and val.lower() not in ("not available", "n/a"):
            num_completed_surveys = val

    # Aggregate measures by domain
    domain_data: dict[str, ExperienceDomain] = {}

    if measure_col:
        for _, row in matches.iterrows():
            measure_id = str(row.get(measure_col, "")).strip().upper()

            # Match measure to domain
            matched_domain = None
            for prefix, domain_name in _HCAHPS_DOMAINS.items():
                if measure_id.startswith(prefix):
                    matched_domain = domain_name
                    break

            if not matched_domain:
                continue

            if matched_domain not in domain_data:
                domain_data[matched_domain] = ExperienceDomain(domain=matched_domain)

            domain = domain_data[matched_domain]

            # Star rating measure
            if _STAR_SUFFIX in measure_id or _LINEAR_SCORE in measure_id:
                if star_col:
                    domain.star_rating = str(row.get(star_col, "")).strip()

            # Answer percent measures — categorize by top/middle/bottom box
            if answer_pct_col and answer_desc_col:
                desc = str(row.get(answer_desc_col, "")).strip().lower()
                pct = str(row.get(answer_pct_col, "")).strip()
                if any(kw in desc for kw in ("always", "strongly agree", "9", "10", "yes", "definitely")):
                    domain.top_box_percent = pct
                elif any(kw in desc for kw in ("never", "strongly disagree", "0", "1", "2", "3", "4", "5", "6")):
                    domain.bottom_box_percent = pct
                elif any(kw in desc for kw in ("sometimes", "usually", "somewhat", "7", "8")):
                    domain.middle_box_percent = pct

            # If star_col has data on any row for this domain
            if star_col and not domain.star_rating:
                val = str(row.get(star_col, "")).strip()
                if val and val.lower() not in ("not available", "n/a", "not applicable"):
                    domain.star_rating = val

    result = PatientExperience(
        ccn=ccn,
        facility_name=facility_name,
        survey_response_rate=survey_response_rate,
        num_completed_surveys=num_completed_surveys,
        domains=list(domain_data.values()),
    )
    return json.dumps(result.model_dump())


# ---------------------------------------------------------------------------
# Tool: get_financial_profile
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_financial_profile(ccn: str) -> str:
    """Get financial profile for a hospital from CMS Cost Report data.

    Returns case mix index, discharge/bed counts, teaching status,
    DSH percentage, wage index, and urban/rural classification.

    Args:
        ccn: The 6-character CMS Certification Number.
    """
    df = await data_loaders.load_cost_report()
    if df.empty:
        return json.dumps({"error": "Cost report data not available"})

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return json.dumps({"error": f"No cost report data found for CCN: {ccn}"})

    # Take the most recent row if multiple years exist
    fy_col = _col(matches, "fiscal_year_end", "fy_end", "fiscal_year_end_date", "fy_end_dt",
                  "fiscal_year_end_dt")
    if fy_col and fy_col in matches.columns:
        matches = matches.sort_values(fy_col, ascending=False)

    row = matches.iloc[0]

    def val(*keys):
        for k in keys:
            if k in row.index and row[k]:
                return str(row[k]).strip()
        return ""

    facility_name = val("facility_name", "hospital_name", "provider_name", "name")

    # Case mix index
    cmi = _safe_float(val("case_mix_index", "cmi", "casemix_index", "case_mix"))

    # Discharges and beds
    total_discharges = _safe_int(val("total_discharges", "discharges", "tot_dschrgs"))
    total_beds = _safe_int(val("total_bed_days_available", "beds", "total_beds",
                               "bed_size", "number_of_beds", "hospital_bed_count"))

    # Teaching: resident-to-bed ratio
    rtb_raw = _safe_float(val("resident_to_bed_ratio", "rtb_ratio", "teaching_ratio",
                              "resident_to_adb_ratio", "residents_to_beds"))
    teaching_status = ""
    if rtb_raw is not None:
        if rtb_raw == 0:
            teaching_status = "Non-teaching"
        elif rtb_raw < 0.25:
            teaching_status = "Minor teaching"
        else:
            teaching_status = "Major teaching"

    # DSH
    dsh = _safe_float(val("dsh_pct", "dsh_percent", "disproportionate_share",
                          "dsh_adjustment_percent", "dsh_patient_percent"))

    # Wage index
    wage = _safe_float(val("wage_index", "area_wage_index", "cbsa_wage_index"))

    # Urban/Rural
    geo = val("urban_rural", "urban_rural_indicator", "geographic_location",
              "urban_or_rural", "cbsa_urban_rural")
    if not geo:
        # Derive from other columns if possible
        provider_type = val("provider_type", "hospital_type", "facility_type").lower()
        if "rural" in provider_type:
            geo = "Rural"
        elif "urban" in provider_type:
            geo = "Urban"

    result = FinancialProfile(
        ccn=ccn,
        facility_name=facility_name,
        case_mix_index=cmi,
        total_discharges=total_discharges,
        total_beds=total_beds,
        teaching_status=teaching_status,
        resident_to_bed_ratio=rtb_raw,
        dsh_pct=dsh,
        wage_index=wage,
        geographic_location=geo,
    )
    return json.dumps(result.model_dump())


# ---------------------------------------------------------------------------
# Tool: get_complications_data
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_complications_data(ccn: str) -> str:
    """Get complications and deaths data for a hospital from CMS dataset ynj2-r877.

    Returns per-measure complication and death rates including observed score,
    confidence interval estimates, denominator (case count), and national
    comparison label for each reported measure (e.g. PSI-90, mortality
    indicators, post-surgical complications).

    Args:
        ccn: The 6-character CMS Certification Number (e.g. "050454").
    """
    df = await data_loaders.load_complications()
    if df.empty:
        return json.dumps({"error": "Complications and Deaths data not available"})

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return json.dumps({"error": f"No complications data found for CCN: {ccn}"})

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(matches.iloc[0][name_col]).strip()

    measure_id_col = _col(matches, "measure_id", "measure_code", "hqi_measure_id")
    measure_name_col = _col(matches, "measure_name", "measure_description", "condition")
    compared_col = _col(
        matches,
        "compared_to_national",
        "national_comparison",
        "compared_to_national_rate",
        "compared_to_us_rate",
    )
    denominator_col = _col(matches, "denominator", "cases", "number_of_cases", "eligible_cases")
    score_col = _col(matches, "score", "rate", "observed_rate", "measure_score")
    lower_col = _col(matches, "lower_estimate", "lower_ci", "lower_confidence_limit",
                     "lower_95pct_ci")
    higher_col = _col(matches, "higher_estimate", "upper_ci", "upper_confidence_limit",
                      "upper_95pct_ci")
    footnote_col = _col(matches, "footnote", "footnote_id", "foot_note")

    measures = []
    for _, row in matches.iterrows():
        measures.append(ComplicationRecord(
            measure_id=str(row.get(measure_id_col, "")).strip() if measure_id_col else "",
            measure_name=str(row.get(measure_name_col, "")).strip() if measure_name_col else "",
            compared_to_national=str(row.get(compared_col, "")).strip() if compared_col else "",
            denominator=_safe_int(row.get(denominator_col, "")) if denominator_col else None,
            score=_safe_float(row.get(score_col, "")) if score_col else None,
            lower_estimate=_safe_float(row.get(lower_col, "")) if lower_col else None,
            higher_estimate=_safe_float(row.get(higher_col, "")) if higher_col else None,
            footnote=str(row.get(footnote_col, "")).strip() if footnote_col else "",
        ))

    result = ComplicationsData(
        ccn=ccn,
        facility_name=facility_name,
        measures=measures,
    )
    return json.dumps(result.model_dump())


# ---------------------------------------------------------------------------
# Tool: compare_hospitals
# ---------------------------------------------------------------------------
@mcp.tool()
async def compare_hospitals(ccns: list[str]) -> str:
    """Compare quality, safety, readmission, and experience data across hospitals.

    Pulls all available metrics for each hospital and returns a side-by-side
    comparison as JSON. Useful for benchmarking hospitals against each other.

    Args:
        ccns: List of CMS Certification Numbers to compare (e.g. ["050454", "050755"]).
    """
    if not ccns or len(ccns) < 2:
        return json.dumps({"error": "Provide at least 2 CCNs to compare"})
    if len(ccns) > 10:
        return json.dumps({"error": "Maximum 10 hospitals for comparison"})

    comparisons = []
    for ccn in ccns:
        hospital = {"ccn": ccn}

        # Quality scores
        quality_json = await get_quality_scores(ccn)
        quality = json.loads(quality_json)
        if "error" not in quality:
            hospital["quality"] = quality
        else:
            hospital["quality"] = {"error": quality["error"]}

        # Safety scores
        safety_json = await get_safety_scores(ccn)
        safety = json.loads(safety_json)
        if "error" not in safety:
            hospital["safety"] = safety
        else:
            hospital["safety"] = {"error": safety["error"]}

        # Readmission data
        readmission_json = await get_readmission_data(ccn)
        readmission = json.loads(readmission_json)
        if "error" not in readmission:
            hospital["readmission"] = readmission
        else:
            hospital["readmission"] = {"error": readmission["error"]}

        # Patient experience
        experience_json = await get_patient_experience(ccn)
        experience = json.loads(experience_json)
        if "error" not in experience:
            hospital["patient_experience"] = experience
        else:
            hospital["patient_experience"] = {"error": experience["error"]}

        comparisons.append(hospital)

    return json.dumps({"hospital_count": len(comparisons), "hospitals": comparisons})


if __name__ == "__main__":
    mcp.run(transport=_transport)
