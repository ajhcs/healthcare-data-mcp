"""Tests for registry-rendered docs and Python distribution artifacts."""

from __future__ import annotations

import re
import json
import os
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

from scripts.render_registry_docs import (
    render_env_catalog,
    render_http_client_catalog,
    render_live_gateway_catalog,
    render_preset_catalog,
    render_server_catalog,
    render_workflow_catalog,
)
from scripts.security_gate import validate_detect_secrets_baseline
from scripts.mcp_smoke import structured_path_exists, structured_path_exists_for_all
from shared.utils.server_registry import CURATED_PRESETS, SERVER_REGISTRY, WORKFLOW_PRESETS
from shared.utils.workflows import WORKFLOW_SOURCE_ALIASES

REPO_ROOT = Path(__file__).resolve().parents[1]


def _venv_script_path(venv_dir: Path, script_name: str) -> Path:
    suffix = ".exe" if sys.platform.startswith("win") else ""
    bin_dir = "Scripts" if sys.platform.startswith("win") else "bin"
    return venv_dir / bin_dir / f"{script_name}{suffix}"


def test_readme_server_catalog_matches_registry_renderer() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    match = re.search(
        r"## Server Catalog\n\n(?P<table>\| Server \| Port \| Domain \| Dataset IDs \|\n(?:\|.*\|\n)+)",
        readme,
    )

    assert match, "README server catalog table not found"
    assert match.group("table").strip() == render_server_catalog().strip()
    assert "Dataset IDs" in match.group("table")
    assert "`cms_hospital_quality`" in match.group("table")


def test_preset_catalog_renderer_covers_curated_presets() -> None:
    table = render_preset_catalog()

    for preset_id in CURATED_PRESETS:
        assert f"`{preset_id}`" in table
    for workflow_id in {workflow_id for preset in CURATED_PRESETS.values() for workflow_id in preset.workflow_ids}:
        assert f"`{workflow_id}`" in table


def test_readme_preset_catalog_matches_registry_renderer() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    try:
        section = readme.split("## Preset Catalog\n\n", 1)[1].split("\n\n## Why Use It?", 1)[0]
    except IndexError:
        raise AssertionError("README preset catalog table not found") from None
    table = section.split("\n\n", 1)[1]

    assert table.strip() == render_preset_catalog().strip()


def test_task_workflow_catalog_matches_registry_renderer() -> None:
    task_docs = (REPO_ROOT / "docs" / "TASK_WORKFLOWS.md").read_text(encoding="utf-8")
    try:
        section = task_docs.split("## Other Canonical Workflow Presets\n\n", 1)[1].split(
            "\n\nUse `hc-mcp doctor --json`", 1
        )[0]
    except IndexError:
        raise AssertionError("TASK_WORKFLOWS workflow catalog table not found") from None
    table = section.split("\n\n", 1)[1]

    assert table.strip() == render_workflow_catalog().strip()
    for workflow_id in WORKFLOW_PRESETS:
        assert f"`{workflow_id}`" in table


def test_task_workflow_docs_explain_source_resolution_aliases() -> None:
    task_docs = (REPO_ROOT / "docs" / "TASK_WORKFLOWS.md").read_text(encoding="utf-8")
    discovery_docs = (REPO_ROOT / "docs" / "DISCOVERY_SERVER.md").read_text(encoding="utf-8")

    required_terms = (
        "`required_sources`",
        "`source_resolution`",
        "`registry_dataset`",
        "`workflow_alias`",
        "substituting adjacent public",
        "records for exact source-backed facts",
    )
    for term in required_terms:
        assert term in task_docs

    preset_terms = (
        "`workflow_summaries`",
        "`hc-mcp workflow <workflow_id> --json` plan command",
        "`discovery.get_preset_plan`",
    )
    for term in preset_terms:
        assert term in task_docs
    for term in ("`get_preset_plan`", "`workflow_summaries`", "source aliases"):
        assert term in discovery_docs

    documented_aliases = ("public_web", "public_financial_health", "routing", "nppes")
    assert set(documented_aliases) <= set(WORKFLOW_SOURCE_ALIASES)
    for alias in documented_aliases:
        alias_spec = WORKFLOW_SOURCE_ALIASES[alias]
        assert f"`{alias}`" in task_docs
        for dataset_id in alias_spec["canonical_dataset_ids"]:
            assert f"`{dataset_id}`" in task_docs


def test_readme_env_catalog_matches_registry_renderer() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    match = re.search(
        r"Registry-backed environment key catalog:\n\n(?P<table>\| Key \| Required \| Servers \| Purpose \|\n(?:\|.*\|\n?)+)",
        readme,
    )

    assert match, "README environment key catalog table not found"
    assert match.group("table").strip() == render_env_catalog().strip()


def test_remote_gateway_live_catalog_matches_registry_renderer() -> None:
    remote_gateway = (REPO_ROOT / "docs" / "REMOTE_GATEWAY.md").read_text(encoding="utf-8")
    try:
        table = remote_gateway.split("Registry-backed live-gateway server catalog:\n\n", 1)[1].split(
            "\n\n## Security Configuration",
            1,
        )[0]
    except IndexError:
        raise AssertionError("REMOTE_GATEWAY live-gateway catalog table not found") from None

    assert table.strip() == render_live_gateway_catalog().strip()
    live_server_ids = {
        spec.server_id
        for spec in SERVER_REGISTRY
        if spec.server_id != "live-gateway" and "live" in spec.gateway_exposure
    }
    for server_id in live_server_ids:
        assert f"`{server_id}`" in table


def test_mcp_clients_http_catalog_matches_registry_renderer() -> None:
    client_docs = (REPO_ROOT / "docs" / "MCP_CLIENTS.md").read_text(encoding="utf-8")
    try:
        table = client_docs.split("Local HTTP ports are rendered into `configs/http-clients.json`:\n\n", 1)[1].split(
            "\n\nRegenerate checked-in client configs",
            1,
        )[0]
    except IndexError:
        raise AssertionError("MCP_CLIENTS local HTTP catalog table not found") from None

    assert table.strip() == render_http_client_catalog().strip()
    for spec in SERVER_REGISTRY:
        assert f"`{spec.server_id}`" in table
        assert f"`http://localhost:{spec.port}/mcp`" in table


def test_ci_checks_live_gateway_registry_docs_renderer() -> None:
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "python scripts/render_registry_docs.py live-gateway-catalog --check" in ci
    assert "python scripts/render_registry_docs.py source-ledger-registry --check" in ci


def test_ci_product_readiness_gates_cover_security_distribution_and_runtime_smoke() -> None:
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    required_snippets = (
        "python -m pip install detect-secrets pip-audit",
        "bash install.sh --dry-run --no-register",
        "bash scripts/register-codex.sh --dry-run --http",
        "bash scripts/setup.sh --help",
        "detect-secrets-hook --baseline .secrets.baseline",
        "python scripts/security_gate.py --baseline .secrets.baseline",
        "pip-audit . --strict",
        "python -m compileall -q servers shared scripts tests",
        "python scripts/render_compose.py full --check",
        "python scripts/render_compose.py zero-config --check",
        "python scripts/render_client_configs.py codex --check",
        "pytest tests/test_client_packaging.py tests/test_distribution_artifacts.py",
        "hc-mcp doctor --check --json",
        "hc-mcp workflow quality_measure_lookup --json",
        "hc-mcp preset metadata-only --json",
        "python scripts/mcp_smoke.py --server live-gateway --expect-tool list_live_tools --call-tool list_live_tools",
        "--expect-structured-path-all tools[].allowed_scopes",
        "--expect-structured-path-all tools[].request_size_limit_bytes",
        "--expect-structured-path-all tools[].result_size_limit_bytes",
        "--expect-structured-path-all tools[].rate_limit_class",
        "--expect-structured-path-all tools[].source_caveat_class",
        "--expect-structured-path-all tools[].requires_provenance",
        "python scripts/mcp_smoke.py --server discovery --expect-tool get_workflow_plan --call-tool get_workflow_plan",
        "python scripts/mcp_smoke.py --server discovery --expect-tool list_presets --expect-resource healthcare-data://presets/catalog --call-tool list_presets",
        "python scripts/mcp_smoke.py --server discovery --expect-tool get_preset_plan --call-tool get_preset_plan",
        '"workflow_id":"system_reconciliation"',
        '"query":"Jefferson Health"',
        '"system_slug":"jefferson-health"',
        "--expect-structured-key report_ingest_contract",
        "--expect-structured-key workflow_summaries",
        "--expect-structured-path-all workflows[].identity_join_keys",
        "--expect-structured-path-all workflows[].source_resolution",
        "--expect-structured-path identity_map.join_keys",
        "--expect-structured-path-all identity_map.resolution_plan[].qualified_tool",
        "--expect-structured-path-all identity_map.resolution_plan[].merge_action",
        "--expect-structured-path-all steps[].identity_contract",
        "--expect-structured-path-all steps[].source_resolution",
        "--expect-structured-path-all report_ingest_contract.fact_rows[].evidence_path",
        "--expect-structured-path-all report_ingest_contract.fact_rows[].source_metadata_path",
        "--expect-structured-path-all report_ingest_contract.fact_rows[].identity_path",
        "--expect-structured-path-all report_ingest_contract.fact_rows[].identity_map_path",
        "--expect-structured-path-all report_ingest_contract.fact_rows[].source_claim_path_contract",
        "--expect-structured-path report_ingest_contract.source_claim_path_validation.final_report",
        "--expect-structured-path-all workflow_summaries[].identity_join_keys",
        "--expect-structured-path-all workflow_summaries[].source_resolution",
        "scripts/mcp_inspector_smoke.sh",
        "docker compose -f docker-compose.zero-config.yml config",
        "python scripts/build_mcpb.py --check",
        "python scripts/build_mcpb.py --skip-dependency-install --force",
        "python -m build --sdist --wheel --outdir dist/python-package",
        "python -m twine check dist/python-package/*",
        "docker compose -f docker-compose.zero-config.yml up -d --build --wait",
        "docker compose -f docker-compose.zero-config.yml down -v",
    )

    for snippet in required_snippets:
        assert snippet in ci


def test_detect_secrets_baseline_policy_allows_only_test_fixture_suppressions() -> None:
    result = validate_detect_secrets_baseline(REPO_ROOT / ".secrets.baseline")

    assert result["status"] == "ok"
    assert result["finding_count"] >= 1
    assert result["allowed_prefixes"] == ["tests/"]


def test_detect_secrets_baseline_policy_rejects_source_suppressions(tmp_path: Path) -> None:
    baseline = {
        "filters_used": [
            {
                "path": "detect_secrets.filters.regex.should_exclude_file",
                "pattern": ["(^\\.git/|^\\.venv/|^build/|^dist/|^\\.pytest_cache/|^\\.ruff_cache/|^\\.secrets\\.baseline$)"],
            }
        ],
        "results": {
            "servers/live_gateway/server.py": [
                {
                    "type": "Secret Keyword",
                    "filename": "servers/live_gateway/server.py",
                    "hashed_secret": "abc123",  # pragma: allowlist secret
                    "is_verified": False,
                    "line_number": 1,
                }
            ]
        },
    }
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(baseline), encoding="utf-8")

    result = validate_detect_secrets_baseline(path)

    assert result["status"] == "error"
    assert any("test fixture" in error for error in result["errors"])


def test_registry_renderers_have_read_only_artifact_checks() -> None:
    commands = [
        [sys.executable, "scripts/render_registry_docs.py", "server-catalog", "--check"],
        [sys.executable, "scripts/render_registry_docs.py", "preset-catalog", "--check"],
        [sys.executable, "scripts/render_registry_docs.py", "workflow-catalog", "--check"],
        [sys.executable, "scripts/render_registry_docs.py", "live-gateway-catalog", "--check"],
        [sys.executable, "scripts/render_registry_docs.py", "source-ledger-registry", "--check"],
        [sys.executable, "scripts/render_registry_docs.py", "http-client-catalog", "--check"],
        [sys.executable, "scripts/render_registry_docs.py", "env-catalog", "--check"],
        [sys.executable, "scripts/render_env_example.py", "--check"],
        [sys.executable, "scripts/render_compose.py", "full", "--check"],
        [sys.executable, "scripts/render_compose.py", "zero-config", "--check"],
        [sys.executable, "scripts/render_client_configs.py", "codex", "--check"],
        [sys.executable, "scripts/render_client_configs.py", "http-clients", "--check"],
        [sys.executable, "scripts/render_client_configs.py", "project-mcp", "--check"],
        [sys.executable, "scripts/render_client_configs.py", "claude-desktop-stdio", "--check"],
        [sys.executable, "scripts/render_client_configs.py", "claude-desktop", "--check"],
    ]

    for command in commands:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        assert "is current" in result.stdout


def test_mcp_smoke_runner_validates_executable_workflow_plan() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/mcp_smoke.py",
            "--server",
            "discovery",
            "--expect-tool",
            "get_workflow_plan",
            "--call-tool",
            "get_workflow_plan",
            "--tool-args",
            '{"workflow_id":"quality_measure_lookup","inputs":{"ccn":"390223","measure":"clabsi_sir"}}',
            "--expect-structured-key",
            "workflow_id",
            "--expect-structured-key",
            "steps",
            "--expect-structured-key",
            "report_ingest_contract",
            "--expect-structured-path",
            "identity_map.join_keys",
            "--expect-structured-path-all",
            "steps[].identity_contract",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].evidence_path",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].source_metadata_path",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].identity_path",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].identity_map_path",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].source_claim_path_contract",
            "--expect-structured-path",
            "report_ingest_contract.source_claim_path_validation.final_report",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["server"] == "discovery"
    assert payload["called_tool"] == "get_workflow_plan"
    assert {"workflow_id", "steps", "report_ingest_contract"} <= set(payload["structured_keys"])
    assert "identity_map.join_keys" in payload["structured_paths"]
    assert {
        "steps[].identity_contract",
        "report_ingest_contract.fact_rows[].evidence_path",
        "report_ingest_contract.fact_rows[].source_metadata_path",
        "report_ingest_contract.fact_rows[].identity_path",
        "report_ingest_contract.fact_rows[].identity_map_path",
        "report_ingest_contract.fact_rows[].source_claim_path_contract",
    } <= set(payload["structured_paths_all"])
    assert "report_ingest_contract.source_claim_path_validation.final_report" in payload["structured_paths"]


def test_mcp_smoke_runner_validates_cross_server_identity_workflow() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/mcp_smoke.py",
            "--server",
            "discovery",
            "--expect-tool",
            "get_workflow_plan",
            "--call-tool",
            "get_workflow_plan",
            "--tool-args",
            '{"workflow_id":"system_reconciliation","inputs":{"query":"Jefferson Health","system_slug":"jefferson-health"}}',
            "--expect-structured-key",
            "workflow_id",
            "--expect-structured-key",
            "identity_map",
            "--expect-structured-key",
            "steps",
            "--expect-structured-key",
            "report_ingest_contract",
            "--expect-structured-path",
            "identity_map.join_keys",
            "--expect-structured-path-all",
            "identity_map.resolution_plan[].qualified_tool",
            "--expect-structured-path-all",
            "identity_map.resolution_plan[].merge_action",
            "--expect-structured-path-all",
            "steps[].identity_contract",
            "--expect-structured-path-all",
            "steps[].source_resolution",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].evidence_path",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].identity_map_path",
            "--expect-structured-path-all",
            "report_ingest_contract.fact_rows[].source_claim_path_contract",
            "--expect-structured-path",
            "report_ingest_contract.source_claim_path_validation.final_report",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["server"] == "discovery"
    assert payload["called_tool"] == "get_workflow_plan"
    assert {"workflow_id", "identity_map", "steps", "report_ingest_contract"} <= set(payload["structured_keys"])
    assert "identity_map.join_keys" in payload["structured_paths"]
    assert {
        "identity_map.resolution_plan[].qualified_tool",
        "identity_map.resolution_plan[].merge_action",
        "steps[].identity_contract",
        "steps[].source_resolution",
        "report_ingest_contract.fact_rows[].evidence_path",
        "report_ingest_contract.fact_rows[].identity_map_path",
        "report_ingest_contract.fact_rows[].source_claim_path_contract",
    } <= set(payload["structured_paths_all"])
    assert "report_ingest_contract.source_claim_path_validation.final_report" in payload["structured_paths"]


def test_mcp_smoke_structured_path_all_requires_every_list_item() -> None:
    payload = {
        "tools": [
            {"name": "a", "allowed_scopes": ["mcp:read"]},
            {"name": "b", "allowed_scopes": ["mcp:read", "mcp:bulk"]},
        ],
        "workflows": [
            {"steps": [{"identity_contract": {"consumes": ["ccn"]}}]},
            {"steps": [{"identity_contract": {"consumes": ["npi"]}}]},
        ],
    }
    drifted = {
        "tools": [
            {"name": "a", "allowed_scopes": ["mcp:read"]},
            {"name": "b"},
        ]
    }

    assert structured_path_exists(drifted, "tools[].allowed_scopes")
    assert not structured_path_exists_for_all(drifted, "tools[].allowed_scopes")
    assert structured_path_exists_for_all(payload, "tools[].allowed_scopes")
    assert structured_path_exists_for_all(payload, "workflows[].steps[].identity_contract")


def test_mcp_smoke_runner_validates_curated_preset_catalog() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/mcp_smoke.py",
            "--server",
            "discovery",
            "--expect-tool",
            "list_presets",
            "--expect-resource",
            "healthcare-data://presets/catalog",
            "--call-tool",
            "list_presets",
            "--expect-structured-key",
            "presets",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["server"] == "discovery"
    assert payload["called_tool"] == "list_presets"
    assert "presets" in payload["structured_keys"]


def test_mcp_smoke_runner_validates_curated_preset_plan() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "scripts/mcp_smoke.py",
            "--server",
            "discovery",
            "--expect-tool",
            "get_preset_plan",
            "--call-tool",
            "get_preset_plan",
            "--tool-args",
            '{"preset_id":"market-strategy"}',
            "--expect-structured-key",
            "preset_id",
            "--expect-structured-key",
            "workflow_summaries",
            "--expect-structured-path-all",
            "workflow_summaries[].identity_join_keys",
            "--expect-structured-path-all",
            "workflow_summaries[].source_resolution",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["server"] == "discovery"
    assert payload["called_tool"] == "get_preset_plan"
    assert {"preset_id", "workflow_summaries"} <= set(payload["structured_keys"])
    assert {
        "workflow_summaries[].identity_join_keys",
        "workflow_summaries[].source_resolution",
    } <= set(payload["structured_paths_all"])


def test_installer_dry_run_uses_registry_without_writing_install_dir(tmp_path: Path) -> None:
    install_dir = tmp_path / "install-target"
    env = {
        **os.environ,
        "HEALTHCARE_MCP_DIR": str(install_dir),
    }

    result = subprocess.run(
        ["bash", "install.sh", "--dry-run", "--no-register"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert f"Server registry entries: {len(SERVER_REGISTRY)}" in result.stdout
    assert "Dry run completed without cloning, installing, writing config, or registering clients" in result.stdout
    assert not install_dir.exists()


def test_codex_register_dry_run_enumerates_registry_servers() -> None:
    result = subprocess.run(
        ["bash", "scripts/register-codex.sh", "--dry-run", "--http"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Dry run: no Codex config changes will be made." in result.stdout
    assert f"Registry entries: {len(SERVER_REGISTRY)}" in result.stdout
    for spec in SERVER_REGISTRY:
        expected = f"would add: codex mcp add hc-{spec.server_id} --url http://localhost:{spec.port}/mcp"
        assert expected in result.stdout


def test_readme_documentation_links_are_durable() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    local_doc_links = re.findall(r"\[[^\]]+\]\((docs/[^)]+\.md)\)", readme)

    assert "docs/SOURCE_CAPABILITY_LEDGER.md" in local_doc_links
    assert "docs/USER_ISSUE_REMEDIATION_PLAN.md" not in local_doc_links
    assert not (REPO_ROOT / "docs" / "USER_ISSUE_REMEDIATION_PLAN.md").exists()
    for link in local_doc_links:
        assert (REPO_ROOT / link).exists(), f"README links to missing documentation: {link}"


def test_distribution_markdown_code_fences_are_balanced() -> None:
    docs = [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").rglob("*.md"))]

    for path in docs:
        fence_count = sum(
            1
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.lstrip().startswith("```")
        )
        assert fence_count % 2 == 0, f"Unbalanced fenced code block in {path.relative_to(REPO_ROOT)}"


def test_source_capability_ledger_covers_known_source_boundaries() -> None:
    ledger = (REPO_ROOT / "docs" / "SOURCE_CAPABILITY_LEDGER.md").read_text(encoding="utf-8")

    required_terms = (
        "MORT_30_AMI",
        "READM_30_HOSP_WIDE",
        "HAI_1_SIR",
        "HHS OIG LEIE",
        "SAM.gov Exclusions",
        "Cybersecurity attestation status",
        "Clinical trial sponsor/site inventory",
        "Do not substitute",
    )
    for term in required_terms:
        assert term in ledger
    assert "Registry-rendered server/source coverage:" in ledger
    assert "| `public-records` |" in ledger
    assert "`hhs_oig_leie`" in ledger
    assert "`sam_gov_exclusions`" in ledger


def test_project_metadata_and_docs_support_versioned_tool_installs() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = pyproject["project"]
    scripts = project["scripts"]
    optional_deps = project["optional-dependencies"]
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    client_packaging = (REPO_ROOT / "docs" / "CLIENT_PACKAGING.md").read_text(encoding="utf-8")

    assert project["name"] == "healthcare-data-mcp"
    assert project["version"]
    assert project["readme"] == "README.md"
    assert project["requires-python"] == ">=3.11"
    assert project["license"]["text"] == "MIT"
    assert project["urls"]["Repository"].endswith("/healthcare-data-mcp")
    assert {"mcp", "healthcare", "public-data"} <= set(project["keywords"])
    assert "Development Status :: 3 - Alpha" in project["classifiers"]
    assert "Intended Audience :: Healthcare Industry" in project["classifiers"]
    assert "Topic :: Scientific/Engineering :: Medical Science Apps." in project["classifiers"]
    assert scripts["hc-mcp"] == "servers._launcher:main"
    assert scripts["hc-mcp-setup"] == "shared.setup_wizard:main"
    assert "build>=1.2.0" in optional_deps["dev"]
    assert "twine>=5.0.0" in optional_deps["dev"]

    install_section = readme.split("## Installation", 1)[1].split("## Registry-Backed Metadata", 1)[0]
    assert install_section.index("### Versioned Python Tools") < install_section.index("### Local Python Development")
    assert install_section.index("hc-mcp doctor") < install_section.index("hc-mcp-setup --interactive")
    assert "pipx install git+https://github.com/ajhcs/healthcare-data-mcp@<tag>" in readme

    for text in (readme, client_packaging):
        assert "pipx install healthcare-data-mcp" in text
        assert "uvx --from healthcare-data-mcp hc-mcp doctor" in text
        assert "hc-mcp --version" in text


def test_python_package_force_include_aliases_cover_registry_server_modules() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    force_include = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]

    expected_aliases: dict[str, str] = {}
    for spec in SERVER_REGISTRY:
        module_parts = spec.module.split(".")
        assert module_parts[0] == "servers"
        package_name = module_parts[1]
        source_dir = REPO_ROOT / "servers" / spec.server_id
        if source_dir.exists() and spec.server_id != package_name:
            expected_aliases[f"servers/{spec.server_id}"] = f"servers/{package_name}"

    assert expected_aliases
    for source, destination in expected_aliases.items():
        assert force_include.get(source) == destination

    assert set(force_include) <= set(expected_aliases)


def test_python_distribution_artifacts_build_and_include_runtime_modules(tmp_path: Path) -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version = pyproject["project"]["version"]

    subprocess.run(
        [sys.executable, "-m", "build", "--sdist", "--wheel", "--outdir", str(tmp_path)],
        cwd=REPO_ROOT,
        check=True,
    )

    wheels = sorted(tmp_path.glob("*.whl"))
    sdists = sorted(tmp_path.glob("*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1
    subprocess.run(
        [sys.executable, "-m", "twine", "check", str(wheels[0]), str(sdists[0])],
        cwd=REPO_ROOT,
        check=True,
    )

    with zipfile.ZipFile(wheels[0]) as wheel:
        names = set(wheel.namelist())
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        entry_points_name = next(name for name in names if name.endswith(".dist-info/entry_points.txt"))
        metadata = wheel.read(metadata_name).decode()
        entry_points = wheel.read(entry_points_name).decode()

    for module_path in (
        "shared/utils/server_registry.py",
        "shared/utils/doctor.py",
        "shared/utils/workflows.py",
        "shared/utils/presets.py",
        "shared/utils/healthcare_identity.py",
        "servers/_launcher.py",
        "servers/provider_enrollment/server.py",
        "servers/live_gateway/server.py",
    ):
        assert module_path in names

    for spec in SERVER_REGISTRY:
        module_path = spec.module.replace(".", "/") + ".py"
        assert module_path in names, module_path

    assert "Name: healthcare-data-mcp" in metadata
    assert "Requires-Python: >=3.11" in metadata
    assert "hc-mcp = servers._launcher:main" in entry_points
    assert "hc-mcp-setup = shared.setup_wizard:main" in entry_points

    venv_dir = tmp_path / "wheel-install-smoke"
    subprocess.run(
        [sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)],
        cwd=REPO_ROOT,
        check=True,
    )
    venv_python = _venv_script_path(venv_dir, "python")
    hc_mcp = _venv_script_path(venv_dir, "hc-mcp")
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "--no-deps", str(wheels[0])],
        cwd=REPO_ROOT,
        check=True,
    )
    version_result = subprocess.run(
        [str(hc_mcp), "--version"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    list_result = subprocess.run(
        [str(hc_mcp), "--list"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    workflow_result = subprocess.run(
        [
            str(hc_mcp),
            "workflow",
            "quality_measure_lookup",
            "--input",
            "ccn=390223",
            "--inputs-json",
            '{"measure":"clabsi_sir"}',
            "--json",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    doctor_result = subprocess.run(
        [str(hc_mcp), "doctor", "--json"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    preset_result = subprocess.run(
        [str(hc_mcp), "preset", "market-strategy", "--json"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    system_reconciliation_result = subprocess.run(
        [
            str(hc_mcp),
            "workflow",
            "system_reconciliation",
            "--input",
            "query=Jefferson Health",
            "--input",
            "system_slug=jefferson-health",
            "--json",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    workflow_plan = json.loads(workflow_result.stdout)
    doctor_report = json.loads(doctor_result.stdout)
    preset_plan = json.loads(preset_result.stdout)
    system_reconciliation_plan = json.loads(system_reconciliation_result.stdout)
    assert version_result.stdout.strip() == f"healthcare-data-mcp {version}"
    assert "hospital-quality" in list_result.stdout
    assert workflow_plan["workflow_id"] == "quality_measure_lookup"
    assert workflow_plan["steps"][1]["mcp_call"]["qualified_tool"] == "hospital-quality.get_quality_measure_rows"
    assert doctor_report["package"]["name"] == "healthcare-data-mcp"
    assert preset_plan["preset_id"] == "market-strategy"
    assert "system_reconciliation" in preset_plan["workflow_ids"]
    assert system_reconciliation_plan["workflow_id"] == "system_reconciliation"
    resolution_plan = system_reconciliation_plan["identity_map"]["resolution_plan"]
    assert resolution_plan
    assert {step["merge_action"] for step in resolution_plan} >= {
        "merge_on_exact_identifier",
        "record_candidate_alias_requires_source_review",
    }

    with tarfile.open(sdists[0], "r:gz") as sdist:
        sdist_names = set(sdist.getnames())
    assert any(name.endswith("README.md") for name in sdist_names)
    assert any(name.endswith("shared/utils/server_registry.py") for name in sdist_names)


def test_docker_distribution_metadata_is_versioned() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version = pyproject["project"]["version"]
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    zero_config_compose = (REPO_ROOT / "docker-compose.zero-config.yml").read_text(encoding="utf-8")
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/docker_image_tags.py",
            "--format",
            "json",
            "--image",
            "ghcr.io/ajhcs/healthcare-data-mcp",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    tag_payload = json.loads(result.stdout)
    version_result = subprocess.run(
        [sys.executable, "scripts/docker_image_tags.py", "--format", "version"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert tag_payload["version"] == version
    assert version_result.stdout.strip() == version
    assert tag_payload["tags"] == [
        f"ghcr.io/ajhcs/healthcare-data-mcp:{version}",
        "ghcr.io/ajhcs/healthcare-data-mcp:latest",
    ]
    assert f"ARG VERSION={version}" in dockerfile
    compose_image_line = 'image: "$' + f'{{HC_MCP_IMAGE:-healthcare-data-mcp:{version}}}"'
    assert compose_image_line in compose
    assert compose_image_line in zero_config_compose
    assert "org.opencontainers.image.version" in dockerfile
    assert "org.opencontainers.image.source" in dockerfile
    assert "scripts/docker_image_tags.py --format docker-build-args" in ci
    assert "scripts/docker_image_tags.py --format version" in ci
    assert "docker run --rm" in ci
