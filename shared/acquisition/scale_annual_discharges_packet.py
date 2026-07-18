"""Reviewed all-six annual-discharges tabular acquisition declaration."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Mapping, Self, cast

from pydantic import model_validator

from shared.acquisition.scale_system_roster import SYSTEM_AHRQ_IDENTITIES, SYSTEM_SLUGS
from shared.acquisition.scale_tabular_input_family import (
    COMMON_BLOCKERS,
    ExpectedLinkageRow,
    LinkageContextRow,
    SystemRowExtraction,
    TabularScaleInputFamilyAcquisition,
    build_tabular_acquisition,
    linkage_row_identity,
    verify_tabular_source_bytes,
)

ACQUIRED_AT = "2026-07-18T03:00:00Z"
SYSTEM_ARTIFACT_ID = "artifact:ahrq-compendium:system:2023"
LINKAGE_ARTIFACT_ID = "artifact:ahrq-compendium:hospital-linkage:2023"

LinkageRowDeclaration = tuple[str, int, str, str, str, str, str, str, str]


def _expected_linkage_row(values: LinkageRowDeclaration) -> ExpectedLinkageRow:
    slug, row, digest, hospital_id, ccn, name, acute, sys_id, discharges = values
    return ExpectedLinkageRow(
        system_slug=slug,
        artifact_ref=LINKAGE_ARTIFACT_ID,
        row_number=row,
        source_row_sha256=f"sha256:{digest}",
        compendium_hospital_id=hospital_id,
        ccn=ccn,
        hospital_name=name,
        acutehosp_flag_raw=acute,
        health_sys_id=sys_id,
        health_sys_name=SYSTEM_AHRQ_IDENTITIES[slug].source_name,
        hos_dsch_raw=discharges,
    )

_SYSTEM_ROWS = {
    "christianacare": (110, "1b6d38a9266101556969e8f00be474811ffc62c6a94216f763eedb777d6a8d46", "HSI00000218", "ChristianaCare", "Wilmington", "DE", "71250", "2", "2", "2"),  # pragma: allowlist secret
    "jefferson-health": (18, "6204ec1b304e076d15c96032dfbe732e172e43e293a45abf5eaf4980d6062b54", "HSI00000048", "Jefferson Health", "Philadelphia", "PA", "147361", "9", "7", "2"),  # pragma: allowlist secret
    "temple-health": (466, "4e1ed4e4546f3a6bd74c3e8a9828b13efca87f1b2f2bd72214d63ba9c477e8c4", "HSI00001065", "Temple University Health System", "Philadelphia", "PA", "37387", "4", "3", "1"),  # pragma: allowlist secret
    "penn-medicine": (361, "99ffd315130cc094008d33628785b3e5610aaa476de0f7a9da4ef245ac7e2d27", "HSI00000820", "University of Pennsylvania Health System", "Philadelphia", "PA", "144099", "9", "6", "2"),  # pragma: allowlist secret
    "cooper-university-health-care": (475, "7246f2affd1de40325df2add2de4428d0ebad4e0370ef819ec12ea73c004cc1f", "HSI00001079", "Cooper University Health Care", "Camden", "NJ", "31354", "1", "1", "1"),  # pragma: allowlist secret
    "main-line-health": (268, "af058d7b4dd8ec51d74d75647b58870266c14aaa6f712f827a512c3f207db185", "HSI00000608", "Main Line Health", "Bryn Mawr", "PA", "59916", "5", "4", "1"),  # pragma: allowlist secret
}

_LINKAGE_ROWS = (
    ("jefferson-health", 20, "2b0621faf7f92a13f3b9cf57a582151aed5a6a20ad13823bda85743917816527", "CHSP00000032", "390231", "Abington Memorial Hospital", "1", "HSI00000048", "30683"),  # pragma: allowlist secret
    ("jefferson-health", 21, "d358c32c5dbe88f60af7f7ab884f542934d7663cab5055def7821d10b197bfb6", "CHSP00000033", "390012", "Lansdale Hospital", "1", "HSI00000048", "5120"),  # pragma: allowlist secret
    ("jefferson-health", 154, "d06f82f3559dbdbfde71df131a9f691ccc31bca14c4b98434d6140507fcf7957", "CHSP00000209", "390115", "Aria Health", "1", "HSI00000048", "22180"),  # pragma: allowlist secret
    ("main-line-health", 589, "c27b7a970f6c63f8a70fbd799a2dcda17c80c4595ba936c4468785bb950593b8", "CHSP00000756", "390139", "Bryn Mawr Hospital", "1", "HSI00000608", "14792"),  # pragma: allowlist secret
    ("main-line-health", 590, "949870b53a59b2ee7dc664e7b0fa460bb790de4d8695ab0e2396645709f4361b", "CHSP00000757", "393025", "Bryn Mawr Rehabilitation Hospital", "0", "HSI00000608", "2067"),  # pragma: allowlist secret
    ("temple-health", 870, "f1342be7a822f1f95771b57b0ca15fd97e6a426765639692b13007f6615c154e", "CHSP00001125", "390026", "Chestnut Hill Hospital", "1", "HSI00001065", "6785"),  # pragma: allowlist secret
    ("christianacare", 921, "7422b5d7b7287e3bfc435ee9f50b27eb77fcb60cabfa3a1e6fd54a119f017847", "CHSP00001204", "080001", "Christiana Care Health System", "1", "HSI00000218", "49852"),  # pragma: allowlist secret
    ("cooper-university-health-care", 1110, "b6a64b3593b4d5b515bd2aa991b9000ba6008495626a5b70427a0a07947c8374", "CHSP00001449", "310014", "Cooper University Hospital", "1", "HSI00001079", "31354"),  # pragma: allowlist secret
    ("jefferson-health", 1376, "80ff68adac4f815fe973ba95b8b5ad34344d51b93693fa1c1c7f6b521446e5e2", "CHSP00001815", "390329", "Einstein Medical Center Montgomery", "1", "HSI00000048", "10338"),  # pragma: allowlist secret
    ("jefferson-health", 1377, "290625507fd6f6bd84a285fd1baf89511dd1c6f3025c79ee6fcd60fab0ef049b", "CHSP00001816", "390142", "Albert Einstein Medical Center", "1", "HSI00000048", "21159"),  # pragma: allowlist secret
    ("temple-health", 1574, "d6846615d253d0b1f5325cddd7ca14c3652ef0a909b175369c032c5c1f431d64", "CHSP00002081", "390196", "American Oncologic Hospial", "0", "HSI00001065", "3554"),  # pragma: allowlist secret
    ("penn-medicine", 1708, "255c1ccabbef2209b2b0070727d0ae17f25ad9a4ad1f2e6d0234c6131e1afe46", "CHSP00002249", "392050", "Good Shepherd Penn Partners", "0", "HSI00000820", "144"),  # pragma: allowlist secret
    ("penn-medicine", 2124, "b640397c76e8484a52af122ca80fdc8c4e77ea805ff67ee8e19507d5c2efebbe", "CHSP00002791", "390111", "Hospital Of The Univ Of Penna", "1", "HSI00000820", "41921"),  # pragma: allowlist secret
    ("temple-health", 2264, "7aea7d782cd69af63143b269e8f99ef18a05879a1b8aae9f6135ae13dd311f1c", "CHSP00002972", "390080", "Temple University Hospital Jeanes Campus", "1", "HSI00001065", ""),  # pragma: allowlist secret
    ("jefferson-health", 2396, "aa475ca5b8cd2c7ac7a0d4c1e69e44bbe283c23e9e224b443568c1a8ba3e274e", "CHSP00003141", "310086", "Kennedy University Hospital", "1", "HSI00000048", "24777"),  # pragma: allowlist secret
    ("penn-medicine", 2561, "d84f2a7f8d72f545663388541d753b6fa4c179dbf5fd60885e2090a9a65aa358", "CHSP00003399", "390100", "Lancaster General Hospital", "1", "HSI00000820", "26640"),  # pragma: allowlist secret
    ("penn-medicine", 2562, "6c4e72f3d9ce93622397a3276ba5093399a5cd9fa240a24aa6d7854593487d5e", "CHSP00003401", "393054", "Lancaster Rehabilitation Hospital", "0", "HSI00000820", "1381"),  # pragma: allowlist secret
    ("main-line-health", 2574, "f7727967224e7e723f7274455b8c516a14491d1c9c6a46e98347be8ceee42f4e", "CHSP00003416", "390195", "Lankenau Medical Center", "1", "HSI00000608", "19330"),  # pragma: allowlist secret
    ("jefferson-health", 2763, "7bc05c99943a0373b0b4462327a1049fe7f4302412208e66e7ba8fd74643b9e0", "CHSP00003667", "393038", "Magee Rehabilitation Hospital", "0", "HSI00000048", "1003"),  # pragma: allowlist secret
    ("main-line-health", 3793, "fde2e35b21cb349ce74f8f3a8dce4c289e6ca081b7bf6fb83f74f41325fe4b84", "CHSP00004950", "390153", "Paoli Hospital", "1", "HSI00000608", "15599"),  # pragma: allowlist secret
    ("penn-medicine", 3871, "aaf5a6af61c403badd12cf3972c0be2aac0ef0ae6c29f53ca122932fc392acdc", "CHSP00005051", "390179", "The Chester County Hospital", "1", "HSI00000820", "20573"),  # pragma: allowlist secret
    ("penn-medicine", 3872, "8e72b902e1dcf0ee0c073094490de4bc89306a983c8451110ba8964a6a413024", "CHSP00005052", "390223", "Presbyterian Medical Center", "1", "HSI00000820", "16788"),  # pragma: allowlist secret
    ("penn-medicine", 3876, "650d1a87a905e43b912ec591dc93bc88bde22b01a1928b585db9dbc337d25f7a", "CHSP00005058", "390226", "Pennsylvania Hospital Of Uphs", "1", "HSI00000820", "22700"),  # pragma: allowlist secret
    ("main-line-health", 4204, "960d3148646614cd88beda5e7099e2f4d672a35471ebe33b8e5f97123a36d0c9", "CHSP00005505", "390222", "Riddle Hospital", "1", "HSI00000608", "10195"),  # pragma: allowlist secret
    ("temple-health", 5251, "c8b71bd68058f1f89d4b4d6d72b5074191f70ce10fbd6dbec92cece1b6901fc0", "CHSP00006817", "390027", "Temple University Hospital", "1", "HSI00001065", "30602"),  # pragma: allowlist secret
    ("jefferson-health", 5360, "e9425d632026f1d51c362a068ea2d692c9380182a92c6dc7bf2133fabc378511", "CHSP00006988", "390174", "Thomas Jefferson Univ. Hospital", "1", "HSI00000048", "33104"),  # pragma: allowlist secret
    ("christianacare", 5519, "1579e05ffe32584caca64875fa048fea3a766606eba3e692a289ec92c8e4e161", "CHSP00007214", "210032", "Union Hospital Of Cecil County", "1", "HSI00000218", "21398"),  # pragma: allowlist secret
    ("penn-medicine", 5581, "3c52a89db59ed12a53f542eea2575afc67a76f2f532ffa551f00548a3ceb7ef9", "CHSP00007287", "310010", "Princeton Healthcare System", "1", "HSI00000820", "15477"),  # pragma: allowlist secret
    ("penn-medicine", 6473, "3187c054b98b3cb3bb822ffc82d865537a873c2e714e2c05acf6299b4ef064ac", "CHSP00008463", "394055", "Lancaster Behavioral Health Hospital", "0", "HSI00000820", "3550"),  # pragma: allowlist secret
    ("jefferson-health", 6571, "e5db5391bb5598bf4a8f489b6134ebe02d4b27c6a40728e49687a447e6cfa1d6", "CHSP00008565", "390289", "Albert Einstein Healthcare Network", "0", "HSI00000048", ""),  # pragma: allowlist secret
)

EXPECTED_LINKAGE_ROWS = tuple(_expected_linkage_row(item) for item in _LINKAGE_ROWS)


class AnnualDischargesAcquisition(TabularScaleInputFamilyAcquisition):
    """Annual contract bound to the immutable complete AHRQ linkage slice."""

    @model_validator(mode="after")
    def preserve_complete_annual_linkage_slice(self) -> Self:
        declared = tuple(linkage_row_identity(item) for item in self.linkage_rows)
        expected = tuple(linkage_row_identity(item) for item in EXPECTED_LINKAGE_ROWS)
        if declared != expected:
            raise ValueError("annual acquisition must preserve the exact complete frozen linkage row set")
        return self

_FINDINGS = {
    "christianacare": "The exact 2023 sys_dsch candidate is source-local to HSI00000218 and two linkage rows; it does not establish the current four-facility roster, treatment of the combined 080001 CCN/campuses, or post-vintage West Grove membership.",
    "jefferson-health": "The exact 2023 sys_dsch candidate is source-local to nine linkage rows (seven acute); it predates the frozen 33-facility boundary and later LVHN membership, and its rehabilitation and shared-CCN treatment are not definitionally receipted.",
    "temple-health": "The exact 2023 sys_dsch candidate is source-local to four linkage rows; Jeanes is acute with a blank hos_dsch and Fox Chase is non-acute with a value, so the utilization boundary cannot be reconciled to other admissions/discharges contexts without a technical definition.",
    "penn-medicine": "The exact 2023 sys_dsch candidate is source-local to nine linkage rows; non-acute rehabilitation/behavioral rows, unresolved current facilities, and post-vintage membership make the current product boundary incomparable.",
    "cooper-university-health-care": "The exact 2023 sys_dsch candidate is source-local to Camden's single linkage row; it predates the frozen Cape Regional inclusion and unresolved Children's boundary.",
    "main-line-health": "The exact 2023 sys_dsch candidate is source-local to five linkage rows, including a non-acute Bryn Mawr Rehabilitation row with hos_dsch 2067; without the technical definition, rehabilitation inclusion cannot be inferred or approved.",
}

_EXTRA_BLOCKERS = {
    "christianacare": ["combined_ccn_campus_treatment_unresolved", "west_grove_post_vintage_membership"],
    "jefferson-health": ["lvhn_post_vintage_membership", "frozen_33_facility_boundary_mismatch"],
    "temple-health": ["jeanes_blank_linkage_value", "admissions_discharges_context_unreconciled"],
    "penn-medicine": ["rehabilitation_behavioral_scope_unresolved", "post_vintage_membership_drift"],
    "cooper-university-health-care": ["cape_regional_post_vintage_membership", "childrens_boundary_unresolved"],
    "main-line-health": ["bryn_mawr_rehabilitation_scope_unresolved", "rehabilitation_roster_inclusion_mismatch"],
}


def acquisition() -> AnnualDischargesAcquisition:
    """Return the immutable second-cycle annual-discharges acquisition."""

    system_rows = [_system_row(slug) for slug in SYSTEM_SLUGS]
    linkage_rows = [_linkage_row(item) for item in _LINKAGE_ROWS]
    by_slug = {
        slug: [row for row in linkage_rows if row.system_slug == slug]
        for slug in SYSTEM_SLUGS
    }
    system_by_slug = {row.system_slug: row for row in system_rows}
    candidates = []
    for slug in SYSTEM_SLUGS:
        system = system_by_slug[slug]
        candidates.append(
            {
                "system_slug": slug,
                "input_family": "annual_discharges",
                "candidate_value": int(system.raw_lexical_value),
                "unit": "discharges",
                "source_period": "2023",
                "definition": "AHRQ Compendium 2023 system-file column sys_dsch; technical definition not locally receipted",
                "basis": "source-local AHRQ highest-ownership system row; denominator, setting, payer, rehabilitation, and shared-CCN treatment unapproved",
                "source_artifact_refs": [SYSTEM_ARTIFACT_ID, LINKAGE_ARTIFACT_ID],
                "system_row_ref": f"row:system:{system.health_sys_id}:{system.row_number}",
                "linkage_row_refs": [
                    f"row:linkage:{row.compendium_hospital_id}:{row.row_number}"
                    for row in by_slug[slug]
                ],
                "missingness": "blocked_source_conflict",
                "blocker_codes": sorted(COMMON_BLOCKERS | set(_EXTRA_BLOCKERS[slug])),
                "finding": _FINDINGS[slug],
                "imputed": False,
                "aggregated": False,
                "fabricated_zero": False,
                "approved_for_scale": False,
            }
        )
    return build_annual_discharges_acquisition(
        {
            "schema_version": "ushso.scale-tabular-input-family-acquisition.v2",
            "acquisition_id": "scale-annual-discharges-all-six-2026-07-18",
            "workflow_id": "scale-tabular-input-family-acquisition.v2",
            "input_family": "annual_discharges",
            "systems": list(SYSTEM_SLUGS),
            "acquired_at": ACQUIRED_AT,
            "producer_version": "HDM-d22",
            "cache_receipt": {
                "dataset_id": "ahrq_health_system_compendium",
                "dataset_artifact_id": "ahrq_health_system_compendium-977328e42c6e",
                "run_id": "local-promote-20260615-v040",
                "artifact_role": "silver",
                "cache_status": "ready",
                "retrieved_at": "2026-06-15T20:15:44.083900Z",
                "manifest_sha256": "sha256:0e647fdcd3ec8bf7f95c0936ae214de422132022b6ce53eabe090988eb529bea",
                "manifest_content_length": 2895,
                "validation_status": "pass",
                "loader_version": "cache-manager-v1",
                "validator_version": "cache-manager-v1",
                "etag": "",
                "last_modified": "",
                "source_period_metadata": "Source-declared period, retrieved_at, or modified timestamp.",
            },
            "source_artifacts": [
                {
                    "artifact_id": SYSTEM_ARTIFACT_ID,
                    "relative_path": "ahrq_system_2023.csv",
                    "source_name": "AHRQ Compendium of U.S. Health Systems 2023 system file",
                    "dataset_id": "ahrq_health_system_compendium",
                    "source_url": "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv",
                    "landing_page": "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv",
                    "source_period": "2023",
                    "cache_promoted_at": "2026-06-15T20:15:44.056206Z",
                    "media_type": "text/csv",
                    "encoding": "cp1252",
                    "payload_sha256": "sha256:7bd62db33d2241236c662afdbd0ff9b30032da817f5ec0a2326311f77c5371b6",
                    "content_length": 106647,
                    "row_count": 639,
                    "schema_fingerprint": "sha256:65e32ad895ad8f21964650352978e59c23b3ee739268ab631a9ad487600f487f",
                    "header_sha256": "sha256:9cc022051910c61c2f66e60b81a450985996cca7fa981c85bd38fa8a9853a79f",
                    "validation_status": "pass",
                    "source_quality": "validated_official_tabular_snapshot",
                    "rights_classification": "unknown_review_required",
                },
                {
                    "artifact_id": LINKAGE_ARTIFACT_ID,
                    "relative_path": "ahrq_hospital_linkage_2023.csv",
                    "source_name": "AHRQ Compendium of U.S. Health Systems 2023 hospital linkage file",
                    "dataset_id": "ahrq_health_system_compendium",
                    "source_url": "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
                    "landing_page": "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
                    "source_period": "2023",
                    "cache_promoted_at": "2026-06-15T20:15:44.071838Z",
                    "media_type": "text/csv",
                    "encoding": "cp1252",
                    "payload_sha256": "sha256:a86146f10c8de626fea1da3a24b756e6a68165e449ae3687f1e90d6bdf129727",
                    "content_length": 1528734,
                    "row_count": 6800,
                    "schema_fingerprint": "sha256:fec5c12e352c26259b8691b4fa9568f9c6e3f433b272c58e063b0dab97a4d634",
                    "header_sha256": "sha256:cb0c3cbc25ea68a39c84974ff00c1a6b76ebd9a03b3bd38959ca482f099007af",
                    "validation_status": "pass",
                    "source_quality": "validated_official_tabular_snapshot",
                    "rights_classification": "unknown_review_required",
                },
            ],
            "system_rows": [row.model_dump(mode="json") for row in system_rows],
            "linkage_rows": [row.model_dump(mode="json") for row in linkage_rows],
            "candidates": candidates,
            "technical_definition_receipt": None,
            "technical_definition_custody": "not_locally_receipted",
            "raw_http_receipt_custody": "not_locally_receipted",
            "redistribution_license_receipt": None,
            "redistribution_rights_custody": "unreviewed",
            "prohibited_outputs": [
                "scale_score",
                "component_score",
                "sensitivity_result",
                "projection",
                "adjudication",
                "recommendation",
                "promotion",
            ],
        }
    )


def _system_row(slug: str) -> SystemRowExtraction:
    row, digest, sys_id, name, city, state, value, hospitals, acute, multistate = _SYSTEM_ROWS[slug]
    return SystemRowExtraction(
        system_slug=slug,
        artifact_ref=SYSTEM_ARTIFACT_ID,
        row_number=row,
        source_row_sha256=f"sha256:{digest}",
        health_sys_id=sys_id,
        health_sys_name=name,
        health_sys_city=city,
        health_sys_state=state,
        raw_lexical_value=value,
        hosp_cnt_raw=hospitals,
        acutehosp_cnt_raw=acute,
        sys_multistate_raw=multistate,
    )


def build_annual_discharges_acquisition(
    payload: Mapping[str, object],
) -> AnnualDischargesAcquisition:
    """Build and validate the immutable annual-specific acquisition."""

    generic = build_tabular_acquisition(payload)
    return AnnualDischargesAcquisition.model_validate(generic.model_dump(mode="json"))


def verify_annual_discharges_source_bytes(
    value: AnnualDischargesAcquisition,
    cache_root: Path,
) -> None:
    """Verify bytes and derive the complete pinned-HSI linkage slice."""

    verify_tabular_source_bytes(
        value,
        cache_root,
        expected_linkage_rows=EXPECTED_LINKAGE_ROWS,
    )


def _linkage_row(values: LinkageRowDeclaration) -> LinkageContextRow:
    slug, row, digest, hospital_id, ccn, name, acute, sys_id, discharges = values
    return LinkageContextRow(
        system_slug=slug,
        artifact_ref=LINKAGE_ARTIFACT_ID,
        row_number=row,
        source_row_sha256=f"sha256:{digest}",
        compendium_hospital_id=hospital_id,
        ccn=ccn,
        hospital_name=name,
        acutehosp_flag_raw=cast(Literal["0", "1"], acute),
        health_sys_id=sys_id,
        health_sys_name=SYSTEM_AHRQ_IDENTITIES[slug].source_name,
        hos_dsch_raw=discharges,
    )


__all__ = [
    "AnnualDischargesAcquisition",
    "EXPECTED_LINKAGE_ROWS",
    "acquisition",
    "build_annual_discharges_acquisition",
    "verify_annual_discharges_source_bytes",
]
