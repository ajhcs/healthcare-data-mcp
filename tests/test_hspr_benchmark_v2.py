import json
from pathlib import Path

from hspr_benchmark.cohort import PILOT_IDS, build_cohort
from hspr_benchmark.container_launcher import (
    assemble_answer_packet,
    codex_docker_command,
    docker_command,
    stage_runtime,
    summarize_codex_trace,
    write_new_text,
)
from hspr_benchmark.leakage import audit_registry_packet
from hspr_benchmark import runner
from hspr_benchmark.runner import counterbalanced_schedule, observable_events
from hspr_benchmark.scoring import paired_cluster_bootstrap, score_answer
from hspr_benchmark.trial_executor import _validate_answer_shape, run_trial, trial_prompt

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


def test_scoring_ignores_schema_required_null_period_placeholders() -> None:
    gold = {
        "validated_value": 100,
        "tolerance": {"absolute_usd": 0},
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31"},
        "units": {"currency": "USD"},
        "entity_perimeter": "Example Health",
        "primary_source": {"url": "https://example.org/report"},
    }
    answer = {
        "answer_status": "reported",
        "value": 100,
        "period": {
            "type": "fiscal_year",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "date": None,
        },
        "units": "USD",
        "reporting_perimeter": "Example Health",
        "primary_source_url": "https://example.org/report",
        "exact_locator": "",
        "caveats": [],
        "aggregated_entities": [],
    }
    assert score_answer(answer, gold)["financial_correct"]


def test_schedule_is_deterministic_and_balanced() -> None:
    first = counterbalanced_schedule(["q1", "q2"], ["a", "b", "c", "d"], 3, "seed")
    assert first == counterbalanced_schedule(["q1", "q2"], ["a", "b", "c", "d"], 3, "seed")
    assert len(first) == 24
    assert {row["arm_id"] for row in first} == {"a", "b", "c", "d"}


def test_batch_executor_writes_immutable_separate_trial_outputs(tmp_path: Path, monkeypatch) -> None:
    sealed = tmp_path / ".benchmark-sealed"
    sealed.mkdir()
    gold = sealed / "gold.json"
    gold.write_text("{}")
    credential = tmp_path / "auth.json"
    credential.write_text('{"fake":"credential-material"}')
    calls = []

    def fake_run_trial(**kwargs):
        calls.append(kwargs)
        kwargs["output_dir"].mkdir(mode=0o700)
        for name in ("native-trace.json", "answer.json", "observable-summary.json"):
            (kwargs["output_dir"] / name).write_text("{}")
        return {"trace": {"duration_ns": 5}}

    monkeypatch.setattr(runner, "audit_registry_packet", lambda *_: {"passed": True, "findings": []})
    monkeypatch.setattr(
        runner,
        "_validate_sealed_gold",
        lambda *_: {"adjudicated_gold_sha256": "a" * 64, "adjudicated_records": 12},
    )
    monkeypatch.setattr(runner, "_build_input_manifest", lambda **_: {"git_revision": "frozen"})
    monkeypatch.setattr(runner, "run_trial", fake_run_trial)
    arguments = {
        "questions_path": ROOT / "hspr-benchmark-v2/public/pilot_questions.json",
        "arms_path": ROOT / "hspr-benchmark-v2/config/arms.json",
        "registry_path": ROOT / "hspr-benchmark-v2/registry/pilot.identity.json",
        "sealed_gold": gold,
        "response_schema": ROOT / "hspr-benchmark-v2/config/response-schema.json",
        "runtime_source": ROOT / "hspr-benchmark-v2/runtime",
        "codex_package_dir": tmp_path / "codex",
        "credential_path": credential,
        "output_root": tmp_path / "runs",
    }
    result = runner.execute_batch(
        **arguments,
        start=1,
        trial_count=2,
    )
    assert len(result["completed"]) == len(calls) == 2
    assert all(call["allow_live_credential"] for call in calls)
    assert all(call["sealed_gold"] == gold for call in calls)
    assert len(list((tmp_path / "runs").glob("trial-*/trial-metadata.json"))) == 2
    resumed = runner.execute_batch(**arguments, start=1, trial_count=2)
    assert len(resumed["completed"]) == 2
    assert len(calls) == 2
    try:
        runner.execute_batch(**arguments, start=1, trial_count=1, attempt=2)
    except ValueError as error:
        assert "already has a completed attempt" in str(error)
    else:
        raise AssertionError("a completed schedule position must not allow another attempt")

    trial_dirs = sorted((tmp_path / "runs").glob("trial-*"))
    (trial_dirs[0] / "answer.json").write_text('{"tampered":true}')
    try:
        runner.execute_batch(**arguments, start=1, trial_count=1)
    except ValueError as error:
        assert "artifact digest mismatch" in str(error)
    else:
        raise AssertionError("tampered completed artifacts must not be accepted")
    (trial_dirs[0] / "answer.json").write_text("{}")

    metadata_path = trial_dirs[1] / "trial-metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["question_id"] = "wrong"
    metadata_path.write_text(json.dumps(metadata))
    try:
        runner.execute_batch(**arguments, start=2, trial_count=1)
    except ValueError as error:
        assert "metadata identity mismatch" in str(error)
    else:
        raise AssertionError("mismatched completed metadata must not be accepted")


def test_batch_executor_preserves_failure_and_requires_new_attempt(tmp_path: Path, monkeypatch) -> None:
    gold = tmp_path / "gold.json"
    gold.write_text("{}")
    credential = tmp_path / "auth.json"
    credential.write_text('{"fake":"credential-material"}')
    monkeypatch.setattr(runner, "audit_registry_packet", lambda *_: {"passed": True, "findings": []})
    monkeypatch.setattr(runner, "_validate_sealed_gold", lambda *_: {"adjudicated_records": 12})
    manifest = {"git_revision": "frozen"}
    monkeypatch.setattr(runner, "_build_input_manifest", lambda **_: manifest)

    def fail_trial(**kwargs):
        kwargs["output_dir"].mkdir(mode=0o700)
        (kwargs["output_dir"] / "native-trace.json").write_text("{}")
        raise RuntimeError("controlled failure")

    monkeypatch.setattr(runner, "run_trial", fail_trial)
    arguments = {
        "questions_path": ROOT / "hspr-benchmark-v2/public/pilot_questions.json",
        "arms_path": ROOT / "hspr-benchmark-v2/config/arms.json",
        "registry_path": ROOT / "hspr-benchmark-v2/registry/pilot.identity.json",
        "sealed_gold": gold,
        "response_schema": ROOT / "hspr-benchmark-v2/config/response-schema.json",
        "runtime_source": ROOT / "hspr-benchmark-v2/runtime",
        "codex_package_dir": tmp_path / "codex",
        "credential_path": credential,
        "output_root": tmp_path / "runs",
        "start": 1,
        "trial_count": 1,
    }
    try:
        runner.execute_batch(**arguments)
    except RuntimeError as error:
        assert "controlled failure" in str(error)
    else:
        raise AssertionError("controlled failure must propagate")
    assert len(list((tmp_path / "runs").glob("in-progress-*"))) == 1
    try:
        runner.execute_batch(**arguments)
    except FileExistsError as error:
        assert "higher --attempt" in str(error)
    else:
        raise AssertionError("partial attempt must not be overwritten")

    def succeed_trial(**kwargs):
        kwargs["output_dir"].mkdir(mode=0o700)
        for name in ("native-trace.json", "answer.json", "observable-summary.json"):
            (kwargs["output_dir"] / name).write_text("{}")
        return {"trace": {"duration_ns": 5}}

    monkeypatch.setattr(runner, "run_trial", succeed_trial)
    result = runner.execute_batch(**arguments, attempt=2)
    assert len(result["completed"]) == 1
    assert len(list((tmp_path / "runs").glob("trial-*-a2"))) == 1

    manifest["git_revision"] = "drifted"
    try:
        runner.execute_batch(**{**arguments, "start": 2}, attempt=1)
    except ValueError as error:
        assert "input-manifest.json" in str(error)
    else:
        raise AssertionError("input drift must stop resumed execution")


def test_runtime_events_never_invent_timestamps_or_authority() -> None:
    events = observable_events(
        '{"type":"web_search","query":"audited 10-k"}\n{"type":"web_open","timestamp":"2026-07-26T17:00:00Z"}\n'
    )
    assert events[0]["runtime_timestamp"] is None
    assert events[0]["timing_available"] is False
    assert events[1]["runtime_timestamp"] == "2026-07-26T17:00:00Z"
    assert not any("authoritative_financial_evidence" in event["event"] for event in events)


def test_packet_allowlist_and_docker_boundary(tmp_path: Path) -> None:
    packet = assemble_answer_packet(
        tmp_path / "packet",
        question={"question_id": "q"},
        response_schema=ROOT / "hspr-benchmark-v2/config/response-schema.json",
        registry_packet=ROOT / "hspr-benchmark-v2/registry/pilot.identity.json",
    )
    assert {path.name for path in packet.iterdir()} == {
        "question.json",
        "response-schema.json",
        "hspr-identity.json",
        "hspr-materialization-timing.json",
    }
    output = tmp_path / "output"
    output.mkdir()
    runtime = stage_runtime(ROOT / "hspr-benchmark-v2/runtime", tmp_path / "runtime")
    command = docker_command(
        image="node:22-alpine",
        packet_dir=packet,
        output_dir=output,
        runtime_dir=runtime,
        runtime_command=("node", "/runtime/isolation-probe.mjs"),
        network="none",
    )
    rendered = " ".join(command)
    assert "--read-only" in command and "--cap-drop ALL" in rendered
    assert "/var/run/docker.sock" not in rendered
    assert ".benchmark-sealed" not in rendered
    assert str(ROOT) not in rendered


def test_answer_packet_rejects_any_sealed_named_source(tmp_path: Path) -> None:
    sealed = tmp_path / ".benchmark-sealed"
    sealed.mkdir()
    forbidden = sealed / "gold.json"
    forbidden.write_text("{}")
    try:
        assemble_answer_packet(
            tmp_path / "packet",
            question={"question_id": "q"},
            response_schema=ROOT / "hspr-benchmark-v2/config/response-schema.json",
            registry_packet=forbidden,
        )
    except ValueError as error:
        assert "sealed files are forbidden" in str(error)
    else:
        raise AssertionError("sealed-named source must never enter an answer packet")


def test_codex_command_has_stdin_supervisor_not_credential_mount(tmp_path: Path) -> None:
    for name in ("packet", "output", "runtime"):
        (tmp_path / name).mkdir()
    codex = tmp_path / "@openai/codex"
    (codex / "bin").mkdir(parents=True)
    (codex / "bin/codex.js").write_text("// test")
    (codex / "package.json").write_text('{"name":"@openai/codex","version":"0.145.0"}')
    command = codex_docker_command(
        packet_dir=tmp_path / "packet",
        output_dir=tmp_path / "output",
        runtime_dir=tmp_path / "runtime",
        codex_package_dir=codex,
        model="gpt-5.6-luna",
        reasoning="medium",
        prompt="answer the allowlisted question",
    )
    rendered = " ".join(command)
    assert "--interactive" in command
    assert "/runtime/credential-supervisor.mjs" in command
    assert "dst=/auth" not in rendered
    assert "/auth:rw,noexec,nosuid,nodev" in rendered
    assert "OPENAI_API_KEY" not in rendered
    assert "dst=/output" not in rendered
    assert "--dangerously-bypass-approvals-and-sandbox" not in command
    assert "use_legacy_landlock" in command
    assert "node@sha256:" in rendered


def test_mount_and_host_output_guards_reject_broad_or_linked_paths(tmp_path: Path) -> None:
    packet = tmp_path / "packet"
    runtime = tmp_path / "runtime"
    output = tmp_path / "output"
    for path in (packet, runtime, output):
        path.mkdir()
    try:
        docker_command(
            image="node:22-alpine",
            packet_dir=Path("/"),
            runtime_dir=runtime,
            output_dir=output,
            runtime_command=("true",),
            network="none",
        )
    except ValueError as error:
        assert "unsafe mount" in str(error)
    else:
        raise AssertionError("filesystem root mount must be rejected")
    target = tmp_path / "target"
    target.write_text("untouched")
    link = tmp_path / "result.json"
    link.symlink_to(target)
    try:
        write_new_text(link, "overwrite")
    except FileExistsError:
        pass
    else:
        raise AssertionError("host result writer must not follow a symlink")
    assert target.read_text() == "untouched"


def test_native_trace_summary_uses_capture_times_only() -> None:
    trace = {
        "events": [
            {"event": "run_start", "monotonic_ns": 1},
            {
                "event": "codex_native_event",
                "monotonic_ns": 2,
                "payload": {
                    "type": "item.started",
                    "item": {
                        "id": "w",
                        "type": "web_search",
                        "action": "open https://example.org/a",
                    },
                },
            },
            {
                "event": "codex_native_event",
                "monotonic_ns": 3,
                "payload": {
                    "type": "item.completed",
                    "item": {
                        "id": "f",
                        "type": "agent_message",
                        "text": "https://example.org/a",
                    },
                },
            },
            {
                "event": "codex_native_event",
                "monotonic_ns": 4,
                "payload": {"type": "turn.completed", "usage": {"input_tokens": 9}},
            },
            {"event": "run_end", "monotonic_ns": 5, "exit_code": 0},
        ]
    }
    summary = summarize_codex_trace(trace, authoritative_urls=["https://example.org/a"])
    assert summary["first_authoritative_evidence_monotonic_ns"] == 2
    assert summary["last_authoritative_evidence_monotonic_ns"] == 3
    assert summary["final_answer_monotonic_ns"] == 3
    assert summary["usage"] == {"input_tokens": 9}
    assert summary["cost"] is None


def test_trial_prompt_differs_only_for_registry_instruction() -> None:
    hspr = trial_prompt(True)
    native = trial_prompt(False)
    assert "/input/hspr-identity.json" in hspr
    assert "/input/hspr-identity.json" not in native
    shared = "Retrieve the financial result now from a live authoritative primary source"
    assert shared in hspr and shared in native


def test_trial_executor_requires_explicit_live_credential_opt_in(tmp_path: Path) -> None:
    try:
        run_trial(
            question={"question_id": "q"},
            arm={"hspr_available": False, "reasoning": "medium"},
            registry_packet=tmp_path / "registry.json",
            sealed_gold=tmp_path / "gold.json",
            response_schema=tmp_path / "schema.json",
            runtime_source=tmp_path / "runtime",
            codex_package_dir=tmp_path / "codex",
            output_dir=tmp_path / "output",
            credential_json=b"{}",
        )
    except PermissionError as error:
        assert "risk-aware" in str(error)
    else:
        raise AssertionError("live credential execution must fail closed")


def test_host_answer_validation_enforces_nested_schema() -> None:
    schema = ROOT / "hspr-benchmark-v2/config/response-schema.json"
    answer = {
        "question_id": "q",
        "answer_status": "reported",
        "reporting_perimeter": "Example Health",
        "metric_label": "Revenue",
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31", "date": None},
        "units": "USD",
        "value": 100,
        "primary_source_url": "https://example.org/report",
        "exact_locator": "page 1",
        "caveats": [],
        "aggregated_entities": [],
        "clarification_needed": False,
        "closest_reported_subtotal": None,
    }
    _validate_answer_shape(answer, "q", schema)
    for field, invalid in (("answer_status", "maybe"), ("period", {"type": "fiscal_year"})):
        malformed = {**answer, field: invalid}
        try:
            _validate_answer_shape(malformed, "q", schema)
        except ValueError as error:
            assert "locked response schema" in str(error)
        else:
            raise AssertionError(f"invalid {field} must be rejected by the host")
