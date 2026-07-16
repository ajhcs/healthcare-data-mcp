"""All-six frozen handoff acceptance tests."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from shared.acquisition.scale_roster_bed_packet import CANDIDATES, SYSTEM_IDS, acquisition_spec
from shared.acquisition.scale_roster_beds import build_bundle_input, load_frozen, load_spec
from shared.contracts.public_evidence import PublicEvidenceBundleInput, build_public_evidence_bundle

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "contracts" / "v1" / "fixtures" / "scale-roster-bed-basis-acquisition.json"
FROZEN = ROOT / "contracts" / "v1" / "fixtures" / "scale-roster-bed-basis-frozen.json"
INPUT = ROOT / "contracts" / "v1" / "fixtures" / "scale-roster-bed-basis-input.json"
SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"


def _handoff() -> PublicEvidenceBundleInput:
    return PublicEvidenceBundleInput.model_validate_json(INPUT.read_text(encoding="utf-8"))


def test_checked_in_handoff_is_mechanical_and_deterministic() -> None:
    spec = load_spec(SPEC)
    frozen = load_frozen(FROZEN)
    checked_in = _handoff()

    first = build_bundle_input(spec, frozen)
    second = build_bundle_input(spec, frozen)
    assert first == second == checked_in
    assert spec == acquisition_spec()
    assert checked_in.created_at == frozen.acquired_at
    assert checked_in.request.parameters["acquisition_cutoff"] == frozen.acquired_at.isoformat()

    frozen_payload = json.loads(FROZEN.read_text(encoding="utf-8"))
    assert "local_path" not in json.dumps(frozen_payload)
    assert all(str(item["portable_uri"]).startswith("hc-cache://") for item in frozen_payload["artifacts"])


def test_all_six_rosters_and_bed_coverage_are_complete() -> None:
    bundle_input = _handoff()
    entities = {item.entity_id: item for item in bundle_input.entities}
    observations = {item.observation_id: item for item in bundle_input.observations}
    coverage = list(bundle_input.coverage)

    assert set(bundle_input.scope.systems) == set(SYSTEM_IDS.values())
    assert set(SYSTEM_IDS.values()).issubset(entities)
    assert len(CANDIDATES) == 63

    roster_entities = {
        item.entity_ref for item in observations.values() if item.measure_id == "hospital_roster_disposition"
    }
    candidate_entities = {candidate.entity_id for candidate in CANDIDATES}
    assert roster_entities == candidate_entities
    assert {item.value for item in observations.values() if item.measure_id == "hospital_roster_disposition"} == {
        "included",
        "excluded",
        "unresolved",
    }

    for entity_id in candidate_entities:
        assert any(
            item.entity_ref == entity_id
            and (item.measure_id.startswith("bed_count.") or item.measure_id == "bed_count.declared")
            for item in coverage
        )

    missing = {item.status for item in coverage if item.status != "populated"}
    assert missing == {
        "not_yet_researched",
        "unavailable_public",
        "not_applicable",
        "blocked_source_conflict",
    }
    assert all(not item.observation_refs for item in coverage if item.status != "populated")


def test_handoff_lineage_resolves_and_schema_validates() -> None:
    bundle_input = _handoff()
    bundle = build_public_evidence_bundle(bundle_input)
    receipts = {source.receipt.receipt_id for source in bundle.sources}
    artifacts = {artifact.artifact_id: artifact for artifact in bundle.input_artifacts}

    assert len(bundle.observations) == len(bundle.sources)
    assert all(
        observation.receipt_refs and set(observation.receipt_refs) <= receipts for observation in bundle.observations
    )
    assert all(source.receipt.artifact == artifacts[source.receipt.artifact.artifact_id] for source in bundle.sources)
    assert all(source.content_checksum != source.receipt.artifact.checksum_sha256 for source in bundle.sources)
    assert all(source.receipt.row_locator for source in bundle.sources)
    assert all(source.receipt.artifact.parser_version == "scale-roster-bed-parser.v1" for source in bundle.sources)
    assert all(
        source.receipt.artifact.connector_version == "scale-roster-bed-connector.v1" for source in bundle.sources
    )

    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_handoff_emits_no_rollup_or_scale_calculation() -> None:
    bundle_input = _handoff()
    system_ids = set(SYSTEM_IDS.values())
    forbidden_fragments = ("scale", "score", "hospital_count", "system_total", "rollup")

    assert all(
        not (observation.entity_ref in system_ids and observation.measure_id.startswith("bed_count."))
        for observation in bundle_input.observations
    )
    assert all(
        not any(fragment in observation.measure_id.casefold() for fragment in forbidden_fragments)
        for observation in bundle_input.observations
    )
    assert bundle_input.request.parameters["no_scale_score"] is True
    assert {conflict.status for conflict in bundle_input.conflicts} == {"open"}


def test_state_federal_and_dated_crosschecks_preserve_source_local_bases() -> None:
    bundle_input = _handoff()
    observations = {item.observation_id: item for item in bundle_input.observations}
    conflicts = {item.conflict_id: item for item in bundle_input.conflicts}

    assert observations["observation:md-bed:union:licensed-fy2026"].value == 99
    assert observations["observation:de-bed:christiana-newark:licensed"].value == 1039
    assert observations["observation:cms-pos:390027:bed_cnt"].entity_ref == "data-mcp:facility:ccn:390027"
    assert observations["observation:cms-hcris:080001:number-of-beds"].period.start.isoformat() == "2023-07-01"
    assert observations["observation:ahrq-linkage:390111"].value == "HSI00000820"
    assert observations["observation:ahrq-bed:390111:hos_beds"].measure_id == "bed_count.ahrq_acute"
    assert conflicts["conflict:temple-shared-cms-reporting-entity"].observation_refs


def test_official_rights_remain_review_required_and_government_sources_are_public_domain() -> None:
    spec = load_spec(SPEC)
    by_id = {source.source_id: source for source in spec.sources}
    assert by_id["christianacare-about"].rights_classification == "unknown_review_required"
    assert by_id["jefferson-enterprise-2025"].rights_classification == "unknown_review_required"
    assert by_id["cms-pos-q1-2026"].rights_classification == "public_domain"
    assert by_id["ahrq-compendium-linkage-2023"].rights_classification == "public_domain"
    assert by_id["pa-hospital-report-2024-1a"].rights_classification == "public_domain"
    assert by_id["nj-acute-care-current"].rights_classification == "public_domain"
