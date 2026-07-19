"""Exact reviewed no-go declarations for emergency-department count."""

from __future__ import annotations

from types import MappingProxyType

from shared.acquisition.scale_service_line_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    IDENTITY_ROWS,
)

HGI_COLUMNS = (
    "Facility ID",
    "Facility Name",
    "Address",
    "City/Town",
    "State",
    "ZIP Code",
    "County/Parish",
    "Telephone Number",
    "Hospital Type",
    "Hospital Ownership",
    "Emergency Services",
    "Meets criteria for birthing friendly designation",
    "Hospital overall rating",
    "Hospital overall rating footnote",
    "MORT Group Measure Count",
    "Count of Facility MORT Measures",
    "Count of MORT Measures Better",
    "Count of MORT Measures No Different",
    "Count of MORT Measures Worse",
    "MORT Group Footnote",
    "Safety Group Measure Count",
    "Count of Facility Safety Measures",
    "Count of Safety Measures Better",
    "Count of Safety Measures No Different",
    "Count of Safety Measures Worse",
    "Safety Group Footnote",
    "READM Group Measure Count",
    "Count of Facility READM Measures",
    "Count of READM Measures Better",
    "Count of READM Measures No Different",
    "Count of READM Measures Worse",
    "READM Group Footnote",
    "Pt Exp Group Measure Count",
    "Count of Facility Pt Exp Measures",
    "Pt Exp Group Footnote",
    "TE Group Measure Count",
    "Count of Facility TE Measures",
    "TE Group Footnote",
)

AHRQ_LINKAGE_COLUMNS = (
    "compendium_hospital_id", "ccn", "hospital_name", "hospital_street",
    "hospital_city", "hospital_state", "hospital_zip", "acutehosp_flag",
    "health_sys_id", "health_sys_name", "health_sys_city", "health_sys_state",
    "corp_parent_id", "corp_parent_name", "corp_parent_type", "hos_beds",
    "hos_dsch", "hos_res", "hos_children", "hos_majteach", "hos_vmajteach",
    "hos_teachint", "hos_highdpp", "hos_ucburden", "hos_highuc",
    "hos_ownership", "hos_net_revenue", "hos_total_revenue",
)

COMMON_BLOCKERS = frozenset(
    {
        "ahrq_system_file_has_no_emergency_department_field",
        "ccn_hospital_membership_not_dedicated_ed_inventory",
        "cms_emergency_services_boolean_not_department_count",
        "common_period_not_receipted",
        "current_roster_membership_unresolved",
        "dedicated_ed_regulatory_unit_not_enumerated",
        "facility_to_system_aggregation_prohibited",
        "hgi_missing_not_no_or_zero",
        "main_campus_off_campus_taxonomy_unresolved",
        "organizational_boundary_unresolved",
        "provider_department_and_campus_multiplicity_unresolved",
        "source_backed_zero_not_available",
        "system_level_count_not_reported",
    }
)

EXTRA_BLOCKERS = MappingProxyType(
    {
        "christianacare": frozenset(
            {"union_hgi_flag_period_conflict", "west_grove_post_vintage_membership"}
        ),
        "jefferson-health": frozenset(
            {"frozen_33_facility_boundary_mismatch", "lvhn_post_vintage_membership"}
        ),
        "temple-health": frozenset(
            {"jeanes_ccn_campus_multiplicity", "episcopal_and_fox_chase_scope_unresolved"}
        ),
        "penn-medicine": frozenset(
            {"multi_campus_ccn_multiplicity", "post_vintage_membership_drift"}
        ),
        "cooper-university-health-care": frozenset(
            {"cape_regional_post_vintage_membership", "childrens_emergency_scope_unresolved"}
        ),
        "main-line-health": frozenset(
            {"rehabilitation_roster_inclusion_mismatch", "current_ed_boundary_unresolved"}
        ),
    }
)

FINDINGS = MappingProxyType(
    {
        "christianacare": "Neither source enumerates dedicated emergency departments, and current West Grove membership and the Union source-period boundary remain unresolved.",
        "jefferson-health": "Neither source enumerates dedicated emergency departments at the frozen product boundary, and later LVHN membership remains outside the approved roster period.",
        "temple-health": "Neither source enumerates dedicated emergency departments; Jeanes campus multiplicity and the Episcopal and Fox Chase scope remain unresolved.",
        "penn-medicine": "Neither source enumerates main-campus and off-campus dedicated emergency departments, and current membership has drifted beyond the frozen roster period.",
        "cooper-university-health-care": "A facility-level Emergency Services flag is not proof of a dedicated-department count, and Cape Regional and children's emergency scope remain unresolved.",
        "main-line-health": "Neither source enumerates dedicated emergency departments or resolves the current acute and rehabilitation product boundary.",
    }
)

__all__ = [
    "AHRQ_HEADER_COLUMNS",
    "AHRQ_LINKAGE_COLUMNS",
    "COMMON_BLOCKERS",
    "EXTRA_BLOCKERS",
    "FINDINGS",
    "HGI_COLUMNS",
    "IDENTITY_ROWS",
]
