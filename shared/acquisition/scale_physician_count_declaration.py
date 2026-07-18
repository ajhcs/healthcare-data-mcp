"""Exact reviewed physician-count row and conflict declaration."""

from __future__ import annotations

from types import MappingProxyType
from typing import NamedTuple


class PhysicianSystemDeclaration(NamedTuple):
    row_number: int
    source_row_sha256: str
    health_sys_id: str
    health_sys_name: str
    health_sys_city: str
    health_sys_state: str
    total_mds_raw: str
    prim_care_mds_raw: str
    hosp_cnt_raw: str
    acutehosp_cnt_raw: str
    sys_multistate_raw: str


SYSTEM_ROW_DECLARATIONS = MappingProxyType(
    {
        "christianacare": PhysicianSystemDeclaration(110, "sha256:1b6d38a9266101556969e8f00be474811ffc62c6a94216f763eedb777d6a8d46", "HSI00000218", "ChristianaCare", "Wilmington", "DE", "1054", "413", "2", "2", "2"),  # pragma: allowlist secret
        "jefferson-health": PhysicianSystemDeclaration(18, "sha256:6204ec1b304e076d15c96032dfbe732e172e43e293a45abf5eaf4980d6062b54", "HSI00000048", "Jefferson Health", "Philadelphia", "PA", "3811", "1276", "9", "7", "2"),  # pragma: allowlist secret
        "temple-health": PhysicianSystemDeclaration(466, "sha256:4e1ed4e4546f3a6bd74c3e8a9828b13efca87f1b2f2bd72214d63ba9c477e8c4", "HSI00001065", "Temple University Health System", "Philadelphia", "PA", "1281", "368", "4", "3", "1"),  # pragma: allowlist secret
        "penn-medicine": PhysicianSystemDeclaration(361, "sha256:99ffd315130cc094008d33628785b3e5610aaa476de0f7a9da4ef245ac7e2d27", "HSI00000820", "University of Pennsylvania Health System", "Philadelphia", "PA", "4336", "1095", "9", "6", "2"),  # pragma: allowlist secret
        "cooper-university-health-care": PhysicianSystemDeclaration(475, "sha256:7246f2affd1de40325df2add2de4428d0ebad4e0370ef819ec12ea73c004cc1f", "HSI00001079", "Cooper University Health Care", "Camden", "NJ", "1012", "276", "1", "1", "1"),  # pragma: allowlist secret
        "main-line-health": PhysicianSystemDeclaration(268, "sha256:af058d7b4dd8ec51d74d75647b58870266c14aaa6f712f827a512c3f207db185", "HSI00000608", "Main Line Health", "Bryn Mawr", "PA", "1084", "405", "5", "4", "1"),  # pragma: allowlist secret
    }
)

FINDINGS = MappingProxyType(
    {
        "christianacare": "The exact 2023 total_mds candidate is source-local to HSI00000218 at the highest ownership level; it does not establish employed-versus-affiliated basis, active status, deduplication, the current four-facility roster, or post-vintage West Grove membership.",
        "jefferson-health": "The exact 2023 total_mds candidate is source-local to HSI00000048 and predates the frozen 33-facility boundary and later LVHN membership; employed, affiliated, faculty-practice, active-status, specialty, and duplicate treatment are not definitionally receipted.",
        "temple-health": "The exact 2023 total_mds candidate is source-local to HSI00001065; no receipted definition reconciles faculty-practice, employed, affiliated, Fox Chase, active-status, specialty, or duplicate physician bases.",
        "penn-medicine": "The exact 2023 total_mds candidate is source-local to HSI00000820; no receipted definition resolves employed-versus-affiliated scope, faculty-practice membership, active status, deduplication, or post-vintage organizational membership.",
        "cooper-university-health-care": "The exact 2023 total_mds candidate is source-local to HSI00001079 and predates the frozen Cape Regional inclusion; employed, affiliated, medical-staff, Children's boundary, active-status, and duplicate treatment remain unresolved.",
        "main-line-health": "The exact 2023 total_mds candidate is source-local to HSI00000608; no receipted definition reconciles employed, affiliated, medical-staff, rehabilitation, active-status, specialty, or duplicate physician bases.",
    }
)

EXTRA_BLOCKERS = MappingProxyType(
    {
        "christianacare": frozenset(
            {"west_grove_post_vintage_membership", "current_four_facility_boundary_mismatch"}
        ),
        "jefferson-health": frozenset(
            {"lvhn_post_vintage_membership", "frozen_33_facility_boundary_mismatch"}
        ),
        "temple-health": frozenset(
            {"fox_chase_physician_scope_unresolved", "faculty_practice_scope_unresolved"}
        ),
        "penn-medicine": frozenset(
            {"faculty_practice_scope_unresolved", "post_vintage_membership_drift"}
        ),
        "cooper-university-health-care": frozenset(
            {"cape_regional_post_vintage_membership", "childrens_boundary_unresolved"}
        ),
        "main-line-health": frozenset(
            {"medical_staff_basis_unresolved", "rehabilitation_roster_inclusion_mismatch"}
        ),
    }
)

__all__ = ["EXTRA_BLOCKERS", "FINDINGS", "SYSTEM_ROW_DECLARATIONS", "PhysicianSystemDeclaration"]
