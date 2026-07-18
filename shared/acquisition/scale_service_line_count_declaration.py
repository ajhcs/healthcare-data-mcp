"""Exact reviewed source evaluation for Scale service-line count."""

from __future__ import annotations

from types import MappingProxyType

from shared.acquisition.scale_physician_count_declaration import SYSTEM_ROW_DECLARATIONS

AHRQ_HEADER_COLUMNS = (
    "health_sys_id", "health_sys_name", "health_sys_city", "health_sys_state",
    "in_onekey", "in_aha", "onekey_id", "aha_sysid", "total_mds",
    "prim_care_mds", "total_nps", "total_pas", "grp_cnt",
    "grp_cnt_restricted", "hosp_cnt", "acutehosp_cnt", "nh_cnt",
    "nh_cnt_restricted", "hhco_cnt", "hhco_cnt_restricted", "sys_multistate",
    "sys_beds", "sys_dsch", "sys_res", "deg_children",
    "sys_incl_majteachhosp", "sys_incl_vmajteachhosp", "sys_teachint",
    "sys_incl_highdpphosp", "sys_highucburden", "sys_incl_highuchosp",
    "sys_anyins_product", "sys_mcare_adv", "sys_mcaid_mngcare",
    "sys_healthins_mktplc", "sys_ma_plan_contracts", "sys_ma_plan_enroll",
    "sys_ownership", "hos_net_revenue", "hos_total_revenue",
)

COMMON_BLOCKERS = frozenset(
    {
        "claims_activity_not_equivalent_to_service_offering",
        "common_service_line_taxonomy_not_receipted",
        "current_roster_membership_unresolved",
        "facility_to_system_aggregation_prohibited",
        "marketing_page_hand_count_prohibited",
        "organizational_boundary_unresolved",
        "period_alignment_unresolved",
        "service_offering_presence_not_receipted",
        "system_level_count_not_reported",
    }
)

EXTRA_BLOCKERS = MappingProxyType(
    {
        "christianacare": frozenset(
            {"current_four_facility_boundary_mismatch", "west_grove_post_vintage_membership"}
        ),
        "jefferson-health": frozenset(
            {"frozen_33_facility_boundary_mismatch", "lvhn_post_vintage_membership"}
        ),
        "temple-health": frozenset(
            {"fox_chase_service_boundary_unresolved", "faculty_practice_scope_unresolved"}
        ),
        "penn-medicine": frozenset(
            {"faculty_practice_scope_unresolved", "post_vintage_membership_drift"}
        ),
        "cooper-university-health-care": frozenset(
            {"cape_regional_post_vintage_membership", "childrens_boundary_unresolved"}
        ),
        "main-line-health": frozenset(
            {"rehabilitation_roster_inclusion_mismatch", "service_boundary_unresolved"}
        ),
    }
)

FINDINGS = MappingProxyType(
    {
        "christianacare": "The exact AHRQ row binds HSI00000218 but reports no service-line field; the CMS taxonomy classifies billed Part B HCPCS services, not offered system service lines. No common-taxonomy count is available for the current four-facility boundary or post-vintage West Grove membership.",
        "jefferson-health": "The exact AHRQ row binds HSI00000048 but reports no service-line field; the CMS taxonomy classifies billed Part B HCPCS services, not offered system service lines. No common-taxonomy count is available for the frozen 33-facility boundary or later LVHN membership.",
        "temple-health": "The exact AHRQ row binds HSI00001065 but reports no service-line field; the CMS taxonomy classifies billed Part B HCPCS services, not offered system service lines. No common-taxonomy count resolves Fox Chase, faculty-practice, or current product boundaries.",
        "penn-medicine": "The exact AHRQ row binds HSI00000820 but reports no service-line field; the CMS taxonomy classifies billed Part B HCPCS services, not offered system service lines. No common-taxonomy count resolves faculty-practice or post-vintage organizational membership.",
        "cooper-university-health-care": "The exact AHRQ row binds HSI00001079 but reports no service-line field; the CMS taxonomy classifies billed Part B HCPCS services, not offered system service lines. No common-taxonomy count covers Cape Regional or resolves the Children's boundary.",
        "main-line-health": "The exact AHRQ row binds HSI00000608 but reports no service-line field; the CMS taxonomy classifies billed Part B HCPCS services, not offered system service lines. No common-taxonomy count resolves rehabilitation inclusion or the current service boundary.",
    }
)

IDENTITY_ROWS = MappingProxyType(
    {
        slug: (
            row.row_number,
            row.source_row_sha256,
            row.health_sys_id,
            row.health_sys_name,
            row.health_sys_city,
            row.health_sys_state,
        )
        for slug, row in SYSTEM_ROW_DECLARATIONS.items()
    }
)

__all__ = [
    "AHRQ_HEADER_COLUMNS",
    "COMMON_BLOCKERS",
    "EXTRA_BLOCKERS",
    "FINDINGS",
    "IDENTITY_ROWS",
]
