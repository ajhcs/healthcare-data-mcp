import json
from pathlib import Path

from hspr_benchmark.cohort import PILOT_IDS, build_cohort
from hspr_benchmark.leakage import audit_registry_packet
from hspr_benchmark.runner import counterbalanced_schedule, observable_events
from hspr_benchmark.scoring import paired_cluster_bootstrap, score_answer

ROOT = Path(__file__).resolve().parents[1]


def test_cohort_is_reproducible_unique_and_disjoint() -> None:
    path = ROOT / "qa/reports/health_system_metrics_reconciliation.csv"
    first = build_cohort(path)
    second = build_cohort(path)
    assert first == second
    systems = first["systems"]
    assert len(systems) == 100
    assert len({row["system_id"] for row in systems}) == 100
    assert not ({row["system_id"] for row in systems} & PILOT_IDS)
    assert [row["selection_group"] for row in systems].count("standard") == 35
    assert [row["selection_group"] for row in systems].count("random") == 50
    assert [row["selection_group"] for row in systems].count("tricky") == 15


def test_identity_packet_passes_and_financial_packet_fails(tmp_path: Path) -> None:
    example = ROOT / "hspr-benchmark-v2/registry/identity-packet.example.json"
    assert audit_registry_packet(example)["passed"]
    leaked = json.loads(example.read_text())
    leaked["systems"][0]["measurements"] = [{"revenue": 123}]
    path = tmp_path / "leaked.json"
    path.write_text(json.dumps(leaked))
    audit = audit_registry_packet(path)
    assert not audit["passed"]
    assert any("forbidden" in item or "unknown" in item for item in audit["findings"])


def test_key_audit_detects_scaled_amount_source_url_and_locator(tmp_path: Path) -> None:
    packet = json.loads((ROOT / "hspr-benchmark-v2/registry/identity-packet.example.json").read_text())
    packet["systems"][0]["ambiguity_warnings"] = [
        "The relevant figure is 22.807769 billion.",
        "Use Consolidated Statements of Operations 2025 column total operating revenues.",
    ]
    packet["systems"][0]["identity_provenance"] = [
        {
            "claim": "identity",
            "source_url": "https://example.org/report.pdf?download=1",
            "accessed": "2026-07-26",
        }
    ]
    key = {
        "records": [
            {
                "question_id": "q",
                "validated_value": 22807769000,
                "primary_source": "https://example.org/report.pdf",
                "exact_locator": "Consolidated Statements of Operations, 2025 column, total operating revenues",
            }
        ]
    }
    packet_path = tmp_path / "packet.json"
    key_path = tmp_path / "gold.json"
    packet_path.write_text(json.dumps(packet))
    key_path.write_text(json.dumps(key))
    findings = audit_registry_packet(packet_path, key_path)["findings"]
    assert any("amount overlap" in item for item in findings)
    assert any("source URL overlap" in item for item in findings)
    assert any("locator n-gram overlap" in item for item in findings)


def test_scoring_and_clustered_pairing() -> None:
    gold = {
        "validated_value": 100,
        "tolerance": {"absolute_usd": 1},
        "requested_metric_available": True,
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31"},
        "units": {"currency": "USD", "normalized_scale": "ones"},
        "entity_perimeter": {
            "description": "Example Health and affiliates, consolidated.",
            "includes": ["Example Health"],
            "excludes": ["one hospital alone"],
            "source_defined_consolidation": True,
        },
        "primary_source": {"url": "https://example.org/u"},
        "exact_locator": "line l",
        "required_caveats": [{"code": "system", "text": "Consolidated system result, not one hospital."}],
        "aggregation_permitted": True,
    }
    answer = {
        "answer_status": "reported",
        "value": 100.5,
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31"},
        "units": "USD",
        "reporting_perimeter": "Example Health and affiliates, consolidated",
        "primary_source_url": "https://example.org/u",
        "exact_locator": "line l",
        "caveats": ["This is the consolidated system result, not one hospital."],
        "aggregated_entities": ["affiliate"],
    }
    assert score_answer(answer, gold)["fully_correct"]
    rows = [
        {"system_id": "a", "arm_id": "x", "m": 1},
        {"system_id": "a", "arm_id": "y", "m": 0},
        {"system_id": "b", "arm_id": "x", "m": 0},
        {"system_id": "b", "arm_id": "y", "m": 0},
    ]
    result = paired_cluster_bootstrap(rows, "m", "x", "y", draws=100)
    assert result["paired_difference"] == 0.5
    assert result["system_clusters"] == 2


def test_schedule_is_deterministic_and_balanced() -> None:
    first = counterbalanced_schedule(["q1", "q2"], ["a", "b", "c", "d"], 3, "seed")
    assert first == counterbalanced_schedule(["q1", "q2"], ["a", "b", "c", "d"], 3, "seed")
    assert len(first) == 24
    assert {row["arm_id"] for row in first} == {"a", "b", "c", "d"}


def test_runtime_events_never_invent_timestamps_or_authority() -> None:
    events = observable_events(
        '{"type":"web_search","query":"audited 10-k"}\n{"type":"web_open","timestamp":"2026-07-26T17:00:00Z"}\n'
    )
    assert events[0]["runtime_timestamp"] is None
    assert events[0]["timing_available"] is False
    assert events[1]["runtime_timestamp"] == "2026-07-26T17:00:00Z"
    assert not any("authoritative_financial_evidence" in event["event"] for event in events)
