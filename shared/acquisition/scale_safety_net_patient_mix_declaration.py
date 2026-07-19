"""Exact reviewed missingness declarations for safety-net patient mix."""

from __future__ import annotations

from types import MappingProxyType

from shared.acquisition.scale_service_line_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    IDENTITY_ROWS,
)

SAFETY_NET_INDICATOR_COLUMNS = (
    "sys_incl_highdpphosp",
    "sys_highucburden",
    "sys_incl_highuchosp",
)

COMMON_BLOCKERS = frozenset(
    {
        "ahrq_binary_flags_not_patient_mix_percentage",
        "approved_patient_denominator_not_receipted",
        "approved_safety_net_numerator_not_receipted",
        "common_period_not_receipted",
        "current_roster_membership_unresolved",
        "dpp_combines_two_different_denominators",
        "facility_to_system_aggregation_prohibited",
        "hospital_ipps_scope_not_product_system_scope",
        "no_system_level_patient_mix_percentage_reported",
        "organizational_boundary_unresolved",
        "setting_and_encounter_basis_unresolved",
        "uncompensated_care_cost_not_patient_mix",
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
            {"faculty_practice_scope_unresolved", "fox_chase_patient_boundary_unresolved"}
        ),
        "penn-medicine": frozenset(
            {"faculty_practice_scope_unresolved", "post_vintage_membership_drift"}
        ),
        "cooper-university-health-care": frozenset(
            {"cape_regional_post_vintage_membership", "childrens_patient_boundary_unresolved"}
        ),
        "main-line-health": frozenset(
            {"rehabilitation_patient_scope_unresolved", "system_patient_boundary_unresolved"}
        ),
    }
)

FINDINGS = MappingProxyType(
    {
        "christianacare": "The exact AHRQ system row supplies only binary high-DSH/uncompensated-care indicators, while CMS DPP is a hospital IPPS measure formed from two fractions with different denominators. Neither source reports a comparable patient-mix percentage for the current four-facility product boundary or post-vintage West Grove membership.",
        "jefferson-health": "The exact AHRQ system row supplies only binary high-DSH/uncompensated-care indicators, while CMS DPP is a hospital IPPS measure formed from two fractions with different denominators. Neither source reports a comparable patient-mix percentage for the frozen 33-facility product boundary or later LVHN membership.",
        "temple-health": "The exact AHRQ system row supplies only binary high-DSH/uncompensated-care indicators, while CMS DPP is a hospital IPPS measure formed from two fractions with different denominators. Neither source resolves a comparable patient-mix percentage across Temple, Fox Chase, faculty-practice, and current product boundaries.",
        "penn-medicine": "The exact AHRQ system row supplies only binary high-DSH/uncompensated-care indicators, while CMS DPP is a hospital IPPS measure formed from two fractions with different denominators. Neither source resolves a comparable patient-mix percentage across the faculty-practice and current product boundaries.",
        "cooper-university-health-care": "The exact AHRQ system row supplies only binary high-DSH/uncompensated-care indicators, while CMS DPP is a hospital IPPS measure formed from two fractions with different denominators. Neither source reports a comparable percentage covering Cape Regional or resolves the Children's boundary.",
        "main-line-health": "The exact AHRQ system row supplies only binary high-DSH/uncompensated-care indicators, while CMS DPP is a hospital IPPS measure formed from two fractions with different denominators. Neither source resolves a comparable percentage across acute, rehabilitation, and current system patient boundaries.",
    }
)

__all__ = [
    "AHRQ_HEADER_COLUMNS",
    "COMMON_BLOCKERS",
    "EXTRA_BLOCKERS",
    "FINDINGS",
    "IDENTITY_ROWS",
    "SAFETY_NET_INDICATOR_COLUMNS",
]
