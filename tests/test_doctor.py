"""Tests for the hc-mcp doctor readiness report."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from shared.utils.doctor import PRIORITY_EVIDENCE_CONTRACTS, build_doctor_report, format_doctor_report, print_doctor
import shared.utils.doctor as doctor


def test_doctor_report_includes_operator_readiness_sections(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    report = build_doctor_report(cache_root=tmp_path)

    assert report["package"]["name"] == "healthcare-data-mcp"
    assert report["python"]["version"]
    assert report["summary"]["server_count"] >= 18
    assert "public-records" in {server["server_id"] for server in report["servers"]}
    assert "compliance_exclusion_screening" in {workflow["workflow"] for workflow in report["workflows"]}
    compliance = next(workflow for workflow in report["workflows"] if workflow["workflow"] == "compliance_exclusion_screening")
    assert compliance["status"] == "needs_inputs"
    assert compliance["missing_inputs"]
    assert compliance["step_status_counts"]
    assert compliance["contract_validation"]["status"] == "ok"
    assert compliance["tool_reference_validation"]["status"] == "ok"
    assert compliance["plan_command"] == "hc-mcp workflow compliance_exclusion_screening --json"
    assert compliance["source_resolution"]["status"] == "ok"
    assert compliance["source_resolution"]["source_count"] >= 1
    assert compliance["source_resolution"]["status_counts"]["registry_dataset"] >= 1
    system_reconciliation = next(
        workflow for workflow in report["workflows"] if workflow["workflow"] == "system_reconciliation"
    )
    aliases = {
        alias["source_id"]: alias
        for alias in system_reconciliation["source_resolution"]["aliases"]
    }
    assert aliases["public_web"]["canonical_dataset_ids"] == ["web_intelligence"]
    assert report["workflow_contract_validation"]["status"] == "ok"
    assert report["workflow_tool_reference_validation"]["status"] == "ok"
    assert report["summary"]["workflow_validation_issues"] == 0
    assert report["evidence_contract_validation"]["status"] == "ok"
    assert report["evidence_contract_validation"]["method"] == "priority_evidence_contract_static"
    assert report["summary"]["evidence_contract_issues"] == 0
    assert report["live_gateway_policy_validation"]["status"] == "ok"
    assert report["live_gateway_policy_validation"]["method"] == "live_gateway_static_policy_ast"
    assert report["live_gateway_policy_validation"]["tool_count"] >= 50
    assert report["live_gateway_policy_validation"]["bulk_tool_count"] >= 2
    assert report["live_gateway_policy_validation"]["provenance_required_tool_count"] == (
        report["live_gateway_policy_validation"]["tool_count"]
    )
    assert {"standard", "bulk"} <= set(report["live_gateway_policy_validation"]["rate_limit_classes"])
    assert "mcp:read+mcp:bulk" in report["live_gateway_policy_validation"]["scope_sets"]
    shared_evidence_validation = report["live_gateway_policy_validation"]["shared_evidence_validation"]
    assert shared_evidence_validation["status"] == "ok"
    assert shared_evidence_validation["call_count"] >= 1
    assert shared_evidence_validation["helper"] == "shared.utils.mcp_response.evidence_receipt_validation_summary"
    assert report["summary"]["live_gateway_policy_issues"] == 0
    evidence_surfaces = {
        surface["surface"]: surface
        for surface in report["evidence_contract_validation"]["surfaces"]
    }
    assert {
        "hospital-quality",
        "provider-enrollment",
        "health-system-profiler",
        "financial-intelligence",
        "workforce-analytics",
        "public-records-cyber-breach",
        "public-records-exclusions",
        "web-intelligence",
        "research-trials",
        "community-health",
        "claims-analytics",
        "physician-referral-network",
        "cms-facility",
        "geo-demographics",
        "drive-time",
        "service-area",
        "price-transparency",
        "public-records-federal-regulatory",
        "public-records-phc4",
    } <= set(evidence_surfaces)
    assert evidence_surfaces["hospital-quality"]["strict_validation_count"] >= 1
    assert evidence_surfaces["research-trials"]["strict_validation_count"] >= 1
    assert "get_quality_scores" in evidence_surfaces["hospital-quality"]["tools"]
    assert "get_cyber_incident_profile" in evidence_surfaces["public-records-cyber-breach"]["tools"]
    assert "screen_leie_batch" in evidence_surfaces["public-records-exclusions"]["tools"]
    assert "screen_sam_exclusions_batch" in evidence_surfaces["public-records-exclusions"]["tools"]
    assert "profile_research_activity" in evidence_surfaces["research-trials"]["tools"]
    assert "detect_leakage" in evidence_surfaces["physician-referral-network"]["tools"]
    assert "search_npi" in evidence_surfaces["cms-facility"]["tools"]
    assert "get_negotiated_rates" in evidence_surfaces["price-transparency"]["tools"]
    assert "search_usaspending" in evidence_surfaces["public-records-federal-regulatory"]["tools"]
    assert report["registry_artifacts"]["status"] == "current"
    assert report["summary"]["registry_artifact_drift"] == 0
    assert report["metadata_catalog_validation"]["status"] == "ok"
    assert report["metadata_catalog_validation"]["method"] == "registry_metadata_catalog_contracts"
    assert report["metadata_catalog_validation"]["issue_count"] == 0
    metadata_checks = {check["name"]: check for check in report["metadata_catalog_validation"]["checks"]}
    assert metadata_checks["discovery_dataset_catalog"]["method"] == "registry_discovery_dataset_catalog"
    assert metadata_checks["discovery_dataset_catalog"]["dataset_count"] >= 1
    assert metadata_checks["gateway_dataset_contracts"]["method"] == "registry_gateway_dataset_ast"
    assert metadata_checks["gateway_dataset_contracts"]["dataset_count"] >= 1
    assert report["summary"]["metadata_catalog_issues"] == 0
    assert report["distribution"]["status"] == "ok"
    assert report["summary"]["distribution_issues"] == 0
    distribution_checks = {check["name"]: check for check in report["distribution"]["checks"]}
    assert distribution_checks["python_package_metadata"]["status"] == "ok"
    assert distribution_checks["console_entry_points"]["status"] == "ok"
    assert distribution_checks["wheel_force_include_registry_aliases"]["status"] == "ok"
    assert distribution_checks["versioned_container_metadata"]["status"] == "ok"
    assert distribution_checks["read_only_onboarding_scripts"]["status"] == "ok"
    assert distribution_checks["ci_product_readiness_gates"]["status"] == "ok"
    artifact_names = {artifact["name"] for artifact in report["registry_artifacts"]["artifacts"]}
    assert ".env.example" in artifact_names
    assert "docker-compose.zero-config.yml" in artifact_names
    assert "desktop-extension/manifest.json" in artifact_names
    assert "docs:workflow-catalog" in artifact_names
    assert report["client_config_hints"]
    assert report["remote_gateway"]["metadata_gateway"] == "search/fetch metadata only"


def test_priority_evidence_contracts_cover_live_gateway_provenance_tools() -> None:
    from servers.live_gateway import server

    covered_tools = {
        tool
        for contract in PRIORITY_EVIDENCE_CONTRACTS
        for tool in contract["tools"]
    }
    routed_provenance_tools = {
        spec.tool_name
        for spec in server.LIVE_TOOL_SPECS
        if spec.require_provenance
    }

    assert routed_provenance_tools - covered_tools == set()


def test_evidence_contract_requires_each_priority_tool_in_strict_receipt_test(
    tmp_path,
    monkeypatch,
) -> None:
    test_dir = tmp_path / "tests"
    test_dir.mkdir()
    test_file = test_dir / "test_fake_surface.py"
    test_file.write_text(
        '''
def test_tool_with_receipt():
    response = call_tool_with_receipt()
    validate_evidence_receipt(response["evidence"], require_content=True)


def test_tool_without_receipt_mentions_name_only():
    assert "tool_without_receipt"
''',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        doctor,
        "PRIORITY_EVIDENCE_CONTRACTS",
        (
            {
                "surface": "fake-surface",
                "server_id": "fake-server",
                "module": "fake.module",
                "tools": ("tool_with_receipt", "tool_without_receipt"),
                "test_paths": ("tests/test_fake_surface.py",),
            },
        ),
    )
    monkeypatch.setattr(
        doctor,
        "_module_fastmcp_tools",
        lambda module_name: {
            "status": "ok",
            "tools": ["tool_with_receipt", "tool_without_receipt"],
        },
    )

    report = doctor._evidence_contract_validation(repo_root=tmp_path)

    assert report["status"] == "issues_found"
    assert report["surfaces"][0]["strict_tested_tools"] == ["tool_with_receipt"]
    assert report["issues"] == [
        {
            "surface": "fake-surface",
            "status": "priority_tool_strict_test_missing",
            "tools": ["tool_without_receipt"],
            "test_paths": ["tests/test_fake_surface.py"],
        }
    ]


def test_doctor_report_formats_as_actionable_text(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    report = build_doctor_report(cache_root=tmp_path)
    text = format_doctor_report(report)

    assert "Healthcare Data MCP doctor" in text
    assert "Server importability and ports:" in text
    assert "Workflow readiness:" in text
    assert "planner contracts: ok (0 issues)" in text
    assert "tool references: ok (0 issues)" in text
    assert "validation: contract=ok, tools=ok" in text
    assert "source resolution:" in text
    assert "source aliases: public_web->web_intelligence" in text
    assert "Evidence contract readiness:" in text
    assert "priority_evidence_contract_static" not in text
    assert "hospital-quality: ok" in text
    assert "Registry-rendered artifacts:" in text
    assert "status: current" in text
    assert "Metadata catalog validation:" in text
    assert "discovery_dataset_catalog: ok" in text
    assert "gateway_dataset_contracts: ok" in text
    assert "Distribution readiness:" in text
    assert "python_package_metadata: ok" in text
    assert "ci_product_readiness_gates: ok" in text
    assert "Live-gateway policy validation:" in text
    assert "rate-limit classes: bulk, standard" in text
    assert "step readiness:" in text
    assert "hc-mcp workflow compliance_exclusion_screening --json" in text
    assert "hc-mcp public-records" in text
    json.dumps(report)


def test_print_doctor_returns_structured_report(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    report = print_doctor(json_output=True, cache_root=tmp_path)
    output = capsys.readouterr().out

    assert report["status"] == "ready"
    assert json.loads(output)["status"] == "ready"


def test_hc_mcp_doctor_check_exits_nonzero_when_action_needed(tmp_path) -> None:
    empty_env = tmp_path / "empty.env"
    empty_env.write_text("", encoding="utf-8")
    env = os.environ.copy()
    env["SEC_USER_AGENT"] = "healthcare-data-mcp tests@example.com"

    ready = subprocess.run(
        [
            sys.executable,
            "-m",
            "servers._launcher",
            "doctor",
            "--env-file",
            str(empty_env),
            "--check",
            "--json",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
    )
    ready_payload = json.loads(ready.stdout)

    assert ready.returncode == 0
    assert ready_payload["status"] == "ready"

    env.pop("SEC_USER_AGENT", None)
    action_needed = subprocess.run(
        [
            sys.executable,
            "-m",
            "servers._launcher",
            "doctor",
            "--env-file",
            str(empty_env),
            "--check",
            "--json",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
    )
    action_needed_payload = json.loads(action_needed.stdout)

    assert action_needed.returncode == 1
    assert action_needed_payload["status"] == "action_needed"
    assert "SEC_USER_AGENT" in action_needed_payload["summary"]["missing_required_env"]


def test_doctor_report_uses_workflow_required_env_readiness(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    report = build_doctor_report(cache_root=tmp_path)
    hospital = next(workflow for workflow in report["workflows"] if workflow["workflow"] == "hospital_competitive_profile")

    assert "SEC_USER_AGENT" in hospital["missing_required_env"]
    assert "env:SEC_USER_AGENT" in hospital["missing_requirements"]


def test_doctor_report_surfaces_workflow_contract_drift(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    def fake_contract_validation() -> dict:
        return {
            "status": "issues_found",
            "issue_count": 1,
            "issues": [
                {
                    "workflow_id": "compliance_exclusion_screening",
                    "status": "evidence_path_not_in_step_contract",
                }
            ],
            "workflows": {
                "compliance_exclusion_screening": {
                    "status": "issues_found",
                    "issue_count": 1,
                    "fact_row_count": 1,
                    "step_count": 1,
                }
            },
            "method": "workflow_report_contract_static",
        }

    monkeypatch.setattr("shared.utils.doctor.validate_workflow_contracts", fake_contract_validation)

    report = build_doctor_report(cache_root=tmp_path)
    compliance = next(workflow for workflow in report["workflows"] if workflow["workflow"] == "compliance_exclusion_screening")

    assert report["status"] == "action_needed"
    assert report["summary"]["workflow_validation_issues"] == 1
    assert compliance["contract_validation"]["status"] == "issues_found"
    assert "workflow_contract:issues_found" in compliance["missing_requirements"]
    assert "Run pytest tests/test_workflows.py" in format_doctor_report(report)


def test_doctor_report_surfaces_registry_artifact_drift(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    def fake_artifact_checks() -> dict:
        return {
            "status": "action_needed",
            "checked_count": 1,
            "drift_count": 1,
            "not_checked_count": 0,
            "artifacts": [
                {
                    "name": ".env.example",
                    "status": "drifted",
                    "path": ".env.example",
                    "regenerate": "python scripts/render_env_example.py > .env.example",
                }
            ],
        }

    monkeypatch.setattr("shared.utils.doctor._registry_artifact_checks", fake_artifact_checks)

    report = build_doctor_report(cache_root=tmp_path)

    assert report["status"] == "action_needed"
    assert report["summary"]["registry_artifact_drift"] == 1
    assert report["registry_artifacts"]["artifacts"][0]["status"] == "drifted"
    assert "Regenerate stale registry-backed artifacts" in format_doctor_report(report)


def test_doctor_report_surfaces_distribution_readiness_issues(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    def fake_distribution_report() -> dict:
        return {
            "status": "action_needed",
            "issue_count": 1,
            "package_name": "healthcare-data-mcp",
            "package_version": "0.1.2",
            "checks": [
                {
                    "name": "versioned_container_metadata",
                    "status": "action_needed",
                    "message": "package/container version mismatch",
                }
            ],
        }

    monkeypatch.setattr("shared.utils.doctor._distribution_report", fake_distribution_report)

    report = build_doctor_report(cache_root=tmp_path)

    assert report["status"] == "action_needed"
    assert report["summary"]["distribution_issues"] == 1
    assert report["distribution"]["checks"][0]["name"] == "versioned_container_metadata"
    assert "Re-check pyproject package metadata" in format_doctor_report(report)


def test_ci_product_readiness_gate_check_reports_missing_source_gates(tmp_path: Path) -> None:
    ci_path = tmp_path / ".github" / "workflows" / "ci.yml"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text("name: incomplete\n", encoding="utf-8")

    check = doctor._ci_product_readiness_gate_check(tmp_path)

    assert check["status"] == "action_needed"
    assert check["name"] == "ci_product_readiness_gates"
    assert "hc-mcp doctor --check --json" in check["missing_snippets"]


def test_onboarding_script_contract_check_reports_missing_registry_and_read_only_guards(tmp_path: Path) -> None:
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    (tmp_path / "install.sh").write_text("#!/usr/bin/env bash\nload_server_registry\n", encoding="utf-8")
    (scripts_dir / "register-codex.sh").write_text("#!/usr/bin/env bash\n--dry-run\n", encoding="utf-8")
    (scripts_dir / "setup.sh").write_text("#!/usr/bin/env bash\nhc-mcp-setup\n", encoding="utf-8")

    check = doctor._onboarding_script_contract_check(tmp_path)

    assert check["status"] == "action_needed"
    assert check["name"] == "read_only_onboarding_scripts"
    assert "install.sh" in check["missing_snippets"]
    assert "scripts/register-codex.sh" in check["missing_snippets"]
    assert "scripts/setup.sh" in check["missing_snippets"]
    assert "from shared.utils.server_registry import SERVER_REGISTRY" in check["missing_snippets"]["install.sh"]
    assert "Dry run: no Codex config changes will be made." in check["missing_snippets"]["scripts/register-codex.sh"]
    assert "intentionally read-only when run without arguments" in check["missing_snippets"]["scripts/setup.sh"]


def test_doctor_live_gateway_policy_validation_rejects_unknown_and_missing_read_scopes(
    monkeypatch,
) -> None:
    def fake_live_tool_specs_from_tree(_tree) -> list[dict]:
        return [
            {
                "server": "provider-enrollment",
                "module": "servers.provider_enrollment.server",
                "tool_name": "search_provider_enrollment",
                "category": "provider_enrollment",
                "scopes": ("mcp:admin",),
                "request_size_limit_bytes": 32768,
                "result_size_limit_bytes": 262144,
                "result_limit": 100,
                "rate_limit_class": "standard",
                "source_caveat_class": "public_source",
                "require_provenance": True,
            }
        ]

    monkeypatch.setattr(doctor, "_live_tool_specs_from_tree", fake_live_tool_specs_from_tree)

    validation = doctor._live_gateway_policy_validation()
    statuses = {issue["status"] for issue in validation["issues"]}

    assert validation["status"] == "issues_found"
    assert "unknown_scope" in statuses
    assert "missing_baseline_read_scope" in statuses


def test_doctor_report_surfaces_live_gateway_policy_drift(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    def fake_live_gateway_policy_validation() -> dict:
        return {
            "status": "issues_found",
            "method": "live_gateway_static_policy_ast",
            "tool_count": 1,
            "live_server_count": 1,
            "bulk_tool_count": 0,
            "provenance_required_tool_count": 1,
            "rate_limit_classes": ["standard"],
            "source_caveat_classes": ["public_source"],
            "scope_sets": ["mcp:read"],
            "issues": [
                {
                    "status": "server_not_live_exposed",
                    "tool": "search_provider_enrollment",
                    "server": "provider-enrollment",
                }
            ],
            "issue_count": 1,
        }

    monkeypatch.setattr("shared.utils.doctor._live_gateway_policy_validation", fake_live_gateway_policy_validation)

    report = build_doctor_report(cache_root=tmp_path)

    assert report["status"] == "action_needed"
    assert report["summary"]["live_gateway_policy_issues"] == 1
    assert report["live_gateway_policy_validation"]["issues"][0]["status"] == "server_not_live_exposed"
    assert "Review live-gateway LIVE_TOOL_SPECS" in format_doctor_report(report)


def test_doctor_report_surfaces_metadata_catalog_drift(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    def fake_metadata_catalog_validation() -> dict:
        return {
            "status": "issues_found",
            "method": "registry_metadata_catalog_contracts",
            "check_count": 2,
            "issue_count": 1,
            "checks": [
                {
                    "name": "discovery_dataset_catalog",
                    "status": "issues_found",
                    "method": "registry_discovery_dataset_catalog",
                    "dataset_count": 1,
                    "issue_count": 1,
                    "issues": [{"status": "declared_dataset_missing_from_catalog", "dataset_id": "missing"}],
                },
                {
                    "name": "gateway_dataset_contracts",
                    "status": "ok",
                    "method": "registry_gateway_dataset_ast",
                    "dataset_count": 1,
                    "issue_count": 0,
                    "issues": [],
                },
            ],
            "issues": [
                {
                    "name": "discovery_dataset_catalog",
                    "status": "declared_dataset_missing_from_catalog",
                    "dataset_id": "missing",
                }
            ],
        }

    monkeypatch.setattr("shared.utils.doctor._metadata_catalog_validation", fake_metadata_catalog_validation)

    report = build_doctor_report(cache_root=tmp_path)
    text = format_doctor_report(report)

    assert report["status"] == "action_needed"
    assert report["summary"]["metadata_catalog_issues"] == 1
    assert "Metadata catalog validation:" in text
    assert "discovery_dataset_catalog: issues_found" in text
    assert "Reconcile discovery/gateway dataset metadata" in text
