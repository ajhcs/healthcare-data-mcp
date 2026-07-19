"""Exact reviewed no-go declarations for essential-service designation count."""

from __future__ import annotations

from types import MappingProxyType

from shared.acquisition.scale_emergency_department_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    IDENTITY_ROWS,
)

COMMON_BLOCKERS = frozenset(
    {
        "approved_eligible_code_taxonomy_not_frozen",
        "approved_current_facility_to_system_crosswalk_not_receipted",
        "combination_code_expansion_and_dedup_rule_not_approved",
        "common_effective_period_not_receipted",
        "current_product_system_boundary_unresolved",
        "designation_issuer_reconciliation_not_approved",
        "eligible_facility_class_rule_not_approved",
        "expired_and_terminated_record_rule_not_approved",
        "provider_type_is_payment_classification_not_system_count",
        "source_backed_zero_not_available",
        "state_and_federal_designation_mixing_prohibited",
        "system_level_count_not_reported",
    }
)

EXTRA_BLOCKERS = MappingProxyType(
    {
        slug: frozenset({f"{slug}_taxonomy_period_boundary_unresolved"})
        for slug in IDENTITY_ROWS
    }
)

FINDINGS = MappingProxyType(
    {
        slug: (
            "CMS providerType and effective-record context cannot be converted to a "
            "product-system count without an approved eligible-code taxonomy, "
            "effective-period rule, combination-code rule, and current facility crosswalk."
        )
        for slug in IDENTITY_ROWS
    }
)

__all__ = [
    "AHRQ_HEADER_COLUMNS",
    "COMMON_BLOCKERS",
    "EXTRA_BLOCKERS",
    "FINDINGS",
    "IDENTITY_ROWS",
]
