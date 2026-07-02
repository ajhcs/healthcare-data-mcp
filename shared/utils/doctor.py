"""Read-only product readiness checks for the hc-mcp CLI."""

from __future__ import annotations

import ast
import importlib
import importlib.metadata
import importlib.util
import json
import os
import socket
import sys
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from shared.utils.server_registry import SERVER_BY_ID, SERVER_REGISTRY, WORKFLOW_PRESETS, ServerCapability
from shared.utils.source_status import normalize_source_status
from shared.utils.workflows import build_workflow_plan, validate_workflow_contracts, validate_workflow_tool_references


PRIORITY_EVIDENCE_CONTRACTS: tuple[dict[str, Any], ...] = (
    {
        "surface": "hospital-quality",
        "server_id": "hospital-quality",
        "module": "servers.hospital_quality.server",
        "tools": (
            "get_quality_scores",
            "get_readmission_data",
            "get_safety_scores",
            "get_patient_experience",
            "get_financial_profile",
            "get_quality_measure_rows",
            "compare_hospitals",
        ),
        "test_paths": ("tests/servers/hospital_quality/test_server.py",),
    },
    {
        "surface": "provider-enrollment",
        "server_id": "provider-enrollment",
        "module": "servers.provider_enrollment.server",
        "tools": (
            "search_provider_enrollment",
            "get_provider_enrollment_detail",
            "get_facility_ownership",
            "trace_owner_network",
            "search_change_of_ownership",
            "profile_provider_control",
        ),
        "test_paths": ("tests/servers/provider_enrollment/test_server.py",),
    },
    {
        "surface": "health-system-profiler",
        "server_id": "health-system-profiler",
        "module": "servers.health_system_profiler.server",
        "tools": (
            "search_health_systems",
            "get_system_profile",
            "reconcile_system_facilities",
            "get_system_facilities",
        ),
        "test_paths": (
            "tests/servers/health_system_profiler/test_server.py",
            "tests/servers/health_system_profiler/test_generic_reconciliation.py",
            "tests/servers/health_system_profiler/test_jefferson_resolver.py",
        ),
    },
    {
        "surface": "financial-intelligence",
        "server_id": "financial-intelligence",
        "module": "servers.financial_intelligence.server",
        "tools": (
            "search_form990",
            "get_form990_details",
            "search_sec_filings",
            "get_sec_filing",
            "search_muni_bonds",
            "get_muni_bond_details",
            "parse_audited_financial_pdf",
            "get_public_financial_health_profile",
            "get_uncompensated_care_profile",
            "get_charity_care_profile",
            "get_bad_debt_profile",
        ),
        "test_paths": ("tests/servers/financial_intelligence/test_server.py",),
    },
    {
        "surface": "workforce-analytics",
        "server_id": "workforce-analytics",
        "module": "servers.workforce_analytics.server",
        "tools": (
            "get_bls_employment",
            "get_hrsa_workforce",
            "get_gme_profile",
            "get_acgme_source_status",
            "get_acgme_program",
            "search_acgme_programs",
            "search_union_activity",
            "get_staffing_benchmarks",
            "get_cost_report_staffing",
            "resolve_hospital_beds",
            "get_hospital_staffing_productivity",
            "compare_hospital_staffing_productivity",
            "get_snf_nursing_hprd",
            "get_teaching_intensity",
            "get_public_throughput_profile",
            "compare_public_throughput",
            "get_ed_volume_profile",
            "get_or_procedure_volume_profile",
        ),
        "test_paths": ("tests/servers/workforce_analytics/test_workforce_data.py",),
    },
    {
        "surface": "public-records-cyber-breach",
        "server_id": "public-records",
        "module": "servers.public_records.server",
        "tools": (
            "get_breach_history",
            "search_ocr_enforcement_actions",
            "search_sec_cyber_disclosures",
            "get_state_ag_breach_notice_sources",
            "get_cyber_attestation_source_status",
            "get_cisa_kev_context_status",
            "search_state_breach_notices",
            "get_cyber_incident_profile",
        ),
        "test_paths": ("tests/servers/public_records/test_cyber_enrichment.py",),
    },
    {
        "surface": "public-records-exclusions",
        "server_id": "public-records",
        "module": "servers.public_records.server",
        "tools": (
            "check_leie_npi",
            "search_leie_individual",
            "search_leie_entity",
            "screen_leie_batch",
            "get_leie_metadata",
            "search_sam_exclusions",
            "check_sam_exclusion_identifier",
            "screen_sam_exclusions_batch",
            "get_sam_exclusions_metadata",
        ),
        "test_paths": (
            "tests/servers/public_records/test_leie_server.py",
            "tests/servers/public_records/test_sam_exclusions_server.py",
        ),
    },
    {
        "surface": "web-intelligence",
        "server_id": "web-intelligence",
        "module": "servers.web_intelligence.server",
        "tools": (
            "search_web",
            "fetch_web_page",
            "scrape_system_profile",
            "detect_ehr_vendor",
            "get_executive_profiles",
            "monitor_newsroom",
            "detect_gpo_affiliation",
        ),
        "test_paths": ("tests/servers/web_intelligence/test_search_client.py",),
    },
    {
        "surface": "research-trials",
        "server_id": "research-trials",
        "module": "servers.research_trials.server",
        "tools": (
            "search_nih_projects",
            "get_nih_project",
            "profile_research_funding",
            "search_clinical_trials",
            "get_clinical_trial",
            "inventory_clinical_trial_sponsors",
            "inventory_clinical_trial_sites",
            "profile_research_activity",
        ),
        "test_paths": ("tests/servers/research_trials/test_server.py",),
    },
    {
        "surface": "community-health",
        "server_id": "community-health",
        "module": "servers.community_health.server",
        "tools": (
            "list_places_measures",
            "search_places",
            "get_places_profile",
            "compare_places",
            "get_market_community_profile",
        ),
        "test_paths": ("tests/servers/community_health/test_server.py",),
    },
    {
        "surface": "claims-analytics",
        "server_id": "claims-analytics",
        "module": "servers.claims_analytics.server",
        "tools": (
            "get_inpatient_volumes",
            "get_outpatient_volumes",
            "trend_service_lines",
            "compute_case_mix",
            "analyze_market_volumes",
        ),
        "test_paths": ("tests/servers/claims_analytics/test_server.py",),
    },
    {
        "surface": "physician-referral-network",
        "server_id": "physician-referral-network",
        "module": "servers.physician_referral_network.server",
        "tools": (
            "search_physicians",
            "get_physician_profile",
            "load_docgraph_cache",
            "map_referral_network",
            "analyze_physician_mix",
            "detect_leakage",
        ),
        "test_paths": ("tests/servers/physician_referral_network/test_server.py",),
    },
    {
        "surface": "cms-facility",
        "server_id": "cms-facility",
        "module": "servers.cms_facility.server",
        "tools": (
            "search_facilities",
            "get_facility",
            "search_npi",
            "get_facility_financials",
            "get_hospital_info",
        ),
        "test_paths": ("tests/servers/cms_facility/test_server.py",),
    },
    {
        "surface": "geo-demographics",
        "server_id": "geo-demographics",
        "module": "servers.geo_demographics.server",
        "tools": (
            "get_zcta_demographics",
            "get_zcta_demographics_batch",
            "get_zcta_adjacency",
            "get_medicare_enrollment",
            "get_geographic_variation",
            "crosswalk_zip",
        ),
        "test_paths": ("tests/servers/geo_demographics/test_server.py",),
    },
    {
        "surface": "drive-time",
        "server_id": "drive-time",
        "module": "servers.drive_time.server",
        "tools": (
            "compute_drive_time",
            "compute_drive_time_matrix",
            "generate_isochrone",
            "find_competing_facilities",
            "compute_accessibility_score",
        ),
        "test_paths": ("tests/servers/drive_time/test_drive_time_server.py",),
    },
    {
        "surface": "service-area",
        "server_id": "service-area",
        "module": "servers.service_area.server",
        "tools": (
            "compute_service_area",
            "get_market_share",
            "get_hsa_hrr_mapping",
            "compare_to_dartmouth",
        ),
        "test_paths": ("tests/servers/service_area/test_server.py",),
    },
    {
        "surface": "price-transparency",
        "server_id": "price-transparency",
        "module": "servers.price_transparency.server",
        "tools": (
            "search_mrf_index",
            "get_negotiated_rates",
            "compute_rate_dispersion",
            "compare_rates_system",
            "benchmark_rates",
        ),
        "test_paths": ("tests/servers/price_transparency/test_server.py",),
    },
    {
        "surface": "public-records-federal-regulatory",
        "server_id": "public-records",
        "module": "servers.public_records.server",
        "tools": (
            "search_usaspending",
            "search_sam_gov",
            "get_accreditation",
            "get_interop_status",
        ),
        "test_paths": (
            "tests/servers/public_records/test_federal_search_records.py",
            "tests/servers/public_records/test_regulatory_records.py",
        ),
    },
    {
        "surface": "public-records-phc4",
        "server_id": "public-records",
        "module": "servers.public_records.server",
        "tools": (
            "search_phc4_public_reports",
            "get_phc4_hospital_performance",
            "get_phc4_financial_analysis",
            "get_phc4_common_procedure_profile",
        ),
        "test_paths": ("tests/servers/public_records/test_phc4_server.py",),
    },
)


def build_doctor_report(*, cache_root: str | Path | None = None) -> dict[str, Any]:
    """Return a structured, read-only readiness report."""

    installed_version = package_version()
    dependencies = {name: _import_status(name) for name in ("mcp", "pandas", "duckdb", "pydantic")}
    server_reports = [_server_report(spec) for spec in SERVER_REGISTRY]
    cache_report = _cache_report(cache_root)
    workflow_contract_validation = validate_workflow_contracts()
    workflow_tool_reference_validation = validate_workflow_tool_references()
    evidence_contract_validation = _evidence_contract_validation()
    registry_artifacts = _registry_artifact_checks()
    metadata_catalog_validation = _metadata_catalog_validation()
    distribution_report = _distribution_report()
    live_gateway_policy = _live_gateway_policy_validation()
    workflow_reports = _workflow_reports(
        server_reports,
        cache_report,
        workflow_contract_validation=workflow_contract_validation,
        workflow_tool_reference_validation=workflow_tool_reference_validation,
    )
    env_report = _env_report()
    conflicts = [item for item in server_reports if item["port_status"] == "in_use"]
    import_failures = [item for item in server_reports if item["import_status"] != "ok"]
    missing_required_env = [
        entry
        for entry in env_report["required"]
        if not entry["configured"]
    ]

    status = "ready"
    workflow_validation_issues = int(workflow_contract_validation.get("issue_count", 0)) + int(
        workflow_tool_reference_validation.get("issue_count", 0)
    )
    artifact_drift_count = int(registry_artifacts.get("drift_count", 0))
    metadata_catalog_issue_count = int(metadata_catalog_validation.get("issue_count", 0))
    distribution_issue_count = int(distribution_report.get("issue_count", 0))
    evidence_contract_issue_count = int(evidence_contract_validation.get("issue_count", 0))
    live_gateway_policy_issue_count = int(live_gateway_policy.get("issue_count", 0))
    if (
        import_failures
        or missing_required_env
        or workflow_validation_issues
        or evidence_contract_issue_count
        or artifact_drift_count
        or metadata_catalog_issue_count
        or distribution_issue_count
        or live_gateway_policy_issue_count
    ):
        status = "action_needed"

    return {
        "status": status,
        "package": {
            "name": "healthcare-data-mcp",
            "version": installed_version,
        },
        "python": {
            "version": sys.version.split()[0],
            "executable": sys.executable,
            "prefix": sys.prefix,
            "base_prefix": sys.base_prefix,
            "in_virtualenv": sys.prefix != sys.base_prefix,
        },
        "environment": env_report,
        "dependencies": dependencies,
        "servers": server_reports,
        "port_conflicts": conflicts,
        "cache": cache_report,
        "workflows": workflow_reports,
        "workflow_contract_validation": workflow_contract_validation,
        "workflow_tool_reference_validation": workflow_tool_reference_validation,
        "evidence_contract_validation": evidence_contract_validation,
        "registry_artifacts": registry_artifacts,
        "metadata_catalog_validation": metadata_catalog_validation,
        "distribution": distribution_report,
        "live_gateway_policy_validation": live_gateway_policy,
        "client_config_hints": _client_config_hints(),
        "remote_gateway": _remote_gateway_posture(),
        "summary": {
            "server_count": len(server_reports),
            "import_failures": len(import_failures),
            "ports_in_use": len(conflicts),
            "missing_required_env": [entry["name"] for entry in missing_required_env],
            "workflow_validation_issues": workflow_validation_issues,
            "evidence_contract_issues": evidence_contract_issue_count,
            "registry_artifact_drift": artifact_drift_count,
            "metadata_catalog_issues": metadata_catalog_issue_count,
            "distribution_issues": distribution_issue_count,
            "live_gateway_policy_issues": live_gateway_policy_issue_count,
        },
    }


def format_doctor_report(report: dict[str, Any]) -> str:
    """Format the doctor report for non-software operators."""

    lines: list[str] = []
    status = report["status"]
    lines.append("Healthcare Data MCP doctor")
    lines.append(f"Status: {status}")
    lines.append("")
    lines.append(f"Package: {report['package']['name']} {report['package']['version']}")
    lines.append(
        "Python: "
        f"{report['python']['version']} at {report['python']['executable']} "
        f"({'venv' if report['python']['in_virtualenv'] else 'system/interpreter'})"
    )

    lines.append("")
    lines.append("Dependencies:")
    for name, dep in report["dependencies"].items():
        marker = "OK" if dep["status"] == "ok" else "FAIL"
        lines.append(f"  {marker} {name}: {dep.get('version') or dep.get('error')}")

    lines.append("")
    lines.append("Server importability and ports:")
    for server in report["servers"]:
        import_marker = "OK" if server["import_status"] == "ok" else "FAIL"
        port_note = "free" if server["port_status"] == "free" else "in use"
        lines.append(f"  {import_marker} {server['server_id']:<28} port {server['port']} {port_note}")
        if server["import_status"] != "ok":
            lines.append(f"      {server['import_error']}")

    lines.append("")
    lines.append("API keys:")
    for entry in report["environment"]["required"]:
        marker = "OK" if entry["configured"] else "MISSING"
        lines.append(f"  {marker} required {entry['name']}: {entry['description']}")
    for entry in report["environment"]["optional"]:
        marker = "OK" if entry["configured"] else "optional"
        lines.append(f"  {marker} {entry['name']}: {entry['description']}")

    cache = report["cache"]
    lines.append("")
    lines.append(f"Cache root: {cache['cache_root']}")
    summary = cache.get("summary", {})
    if summary:
        lines.append(
            "Cache readiness: "
            f"{summary.get('ready', 0)} ready, {summary.get('stale', 0)} stale, "
            f"{summary.get('missing', 0)} missing"
        )
    for item in cache.get("not_ready_examples", []):
        validation = item.get("validation_status") or "not_validated"
        period = item.get("source_period") or "source_period_unknown"
        eligible = "report eligible" if item.get("report_eligible") else "not report eligible"
        lines.append(
            f"  {item['status']} {item['relative_path']} ({item['dataset_id']}); "
            f"validation={validation}; period={period}; {eligible}"
        )
        if item.get("next_action"):
            lines.append(f"      next: {item['next_action']}")

    lines.append("")
    lines.append("Workflow readiness:")
    contract_validation = report.get("workflow_contract_validation", {})
    tool_reference_validation = report.get("workflow_tool_reference_validation", {})
    if contract_validation or tool_reference_validation:
        lines.append(
            "  planner contracts: "
            f"{contract_validation.get('status', 'unknown')} "
            f"({contract_validation.get('issue_count', 0)} issues)"
        )
        lines.append(
            "  tool references: "
            f"{tool_reference_validation.get('status', 'unknown')} "
            f"({tool_reference_validation.get('issue_count', 0)} issues)"
        )
    for workflow in report["workflows"]:
        marker = "READY" if workflow["ready"] else "CHECK"
        missing = ", ".join(workflow["missing_requirements"]) or "none"
        lines.append(f"  {marker} {workflow['workflow']}: {workflow.get('status', 'unknown')}; missing {missing}")
        lines.append(
            "      validation: "
            f"contract={workflow.get('contract_validation', {}).get('status', 'unknown')}, "
            f"tools={workflow.get('tool_reference_validation', {}).get('status', 'unknown')}"
        )
        step_counts = workflow.get("step_status_counts", {})
        if step_counts:
            counts = ", ".join(f"{status}={count}" for status, count in sorted(step_counts.items()))
            lines.append(f"      step readiness: {counts}")
        source_resolution = workflow.get("source_resolution", {})
        source_counts = source_resolution.get("status_counts", {}) if isinstance(source_resolution, dict) else {}
        if source_counts:
            counts = ", ".join(f"{status}={count}" for status, count in sorted(source_counts.items()))
            lines.append(f"      source resolution: {counts}")
        aliases = source_resolution.get("aliases", []) if isinstance(source_resolution, dict) else []
        if aliases:
            alias_text = ", ".join(
                f"{alias.get('source_id')}->{'+'.join(alias.get('canonical_dataset_ids', []))}"
                for alias in aliases[:3]
            )
            lines.append(f"      source aliases: {alias_text}")
        if workflow.get("optional_unavailable"):
            optional = ", ".join(
                f"{item['server']}.{item['tool']}" for item in workflow["optional_unavailable"][:3]
            )
            lines.append(f"      optional unavailable: {optional}")
        lines.append(f"      plan: {workflow['plan_command']}")

    evidence_contracts = report.get("evidence_contract_validation", {})
    lines.append("")
    lines.append("Evidence contract readiness:")
    lines.append(
        f"  status: {evidence_contracts.get('status', 'unknown')}; "
        f"surfaces {evidence_contracts.get('surface_count', 0)}, "
        f"tools {evidence_contracts.get('tool_count', 0)}, "
        f"issues {evidence_contracts.get('issue_count', 0)}"
    )
    for surface in evidence_contracts.get("surfaces", []):
        marker = "OK" if surface["status"] == "ok" else ("SKIP" if surface["status"] == "not_checked" else "CHECK")
        lines.append(
            f"  {marker} {surface['surface']}: {surface['status']} - "
            f"{surface['tool_count']} tools; strict tests {surface.get('strict_validation_count', 0)}"
        )

    artifacts = report.get("registry_artifacts", {})
    lines.append("")
    lines.append("Registry-rendered artifacts:")
    lines.append(
        f"  status: {artifacts.get('status', 'unknown')}; "
        f"checked {artifacts.get('checked_count', 0)}, drift {artifacts.get('drift_count', 0)}, "
        f"not checked {artifacts.get('not_checked_count', 0)}"
    )
    for item in artifacts.get("artifacts", []):
        marker = "OK" if item["status"] == "current" else ("SKIP" if item["status"] == "not_checked" else "CHECK")
        lines.append(f"  {marker} {item['name']}: {item['status']} ({item.get('path', '')})")

    metadata_catalogs = report.get("metadata_catalog_validation", {})
    lines.append("")
    lines.append("Metadata catalog validation:")
    lines.append(
        f"  status: {metadata_catalogs.get('status', 'unknown')}; "
        f"checks {metadata_catalogs.get('check_count', 0)}, "
        f"issues {metadata_catalogs.get('issue_count', 0)}"
    )
    for check in metadata_catalogs.get("checks", []):
        marker = "OK" if check["status"] == "ok" else ("SKIP" if check["status"] == "not_checked" else "CHECK")
        lines.append(
            f"  {marker} {check['name']}: {check['status']} - "
            f"{check.get('dataset_count', check.get('checked_count', 0))} datasets; "
            f"{check.get('issue_count', 0)} issues"
        )
    for issue in metadata_catalogs.get("issues", [])[:5]:
        lines.append(f"  CHECK {issue.get('name', 'metadata')}: {issue.get('status')}")

    distribution = report.get("distribution", {})
    lines.append("")
    lines.append("Distribution readiness:")
    lines.append(
        f"  status: {distribution.get('status', 'unknown')}; "
        f"issues {distribution.get('issue_count', 0)}"
    )
    for check in distribution.get("checks", []):
        marker = "OK" if check["status"] == "ok" else ("SKIP" if check["status"] == "not_checked" else "CHECK")
        lines.append(f"  {marker} {check['name']}: {check['status']} - {check.get('message', '')}")

    live_policy = report.get("live_gateway_policy_validation", {})
    lines.append("")
    lines.append("Live-gateway policy validation:")
    lines.append(
        f"  status: {live_policy.get('status', 'unknown')}; "
        f"tools {live_policy.get('tool_count', 0)}, "
        f"servers {live_policy.get('live_server_count', 0)}, "
        f"bulk tools {live_policy.get('bulk_tool_count', 0)}, "
        f"issues {live_policy.get('issue_count', 0)}"
    )
    if live_policy.get("rate_limit_classes"):
        lines.append("  rate-limit classes: " + ", ".join(live_policy["rate_limit_classes"]))
    if live_policy.get("scope_sets"):
        lines.append("  scope sets: " + "; ".join(live_policy["scope_sets"]))
    for issue in live_policy.get("issues", [])[:5]:
        lines.append(f"  CHECK {issue.get('tool', issue.get('server', 'policy'))}: {issue.get('status')}")

    lines.append("")
    lines.append("Client config hints:")
    for hint in report["client_config_hints"]:
        lines.append(f"  - {hint}")

    gateway = report["remote_gateway"]
    lines.append("")
    lines.append("Remote gateway posture:")
    lines.append(f"  metadata gateway: {gateway['metadata_gateway']}")
    lines.append(f"  live gateway: {gateway['live_gateway']}")
    for warning in gateway["warnings"]:
        lines.append(f"  WARN {warning}")

    if status != "ready":
        lines.append("")
        lines.append("Next steps:")
        if report["summary"]["missing_required_env"]:
            lines.append("  - Set missing required environment variables in .env or HC_MCP_ENV_FILE.")
        if report["summary"]["import_failures"]:
            lines.append('  - Reinstall with python -m pip install -e ".[dev]" and rerun hc-mcp doctor.')
        if summary.get("missing", 0):
            lines.append("  - Run hc-mcp-setup --cache-status or hc-mcp-setup --acquire-public-caches.")
        if report["summary"].get("workflow_validation_issues"):
            lines.append("  - Run pytest tests/test_workflows.py and inspect hc-mcp workflow <name> --json.")
        if report["summary"].get("evidence_contract_issues"):
            lines.append(
                "  - Run priority evidence tests and confirm receipts pass "
                "validate_evidence_receipt(..., require_content=True)."
            )
        if report["summary"].get("registry_artifact_drift"):
            lines.append("  - Regenerate stale registry-backed artifacts with scripts/render_* --check guidance.")
        if report["summary"].get("metadata_catalog_issues"):
            lines.append("  - Reconcile discovery/gateway dataset metadata with shared.utils.server_registry.")
        if report["summary"].get("distribution_issues"):
            lines.append("  - Re-check pyproject package metadata, Docker tags, and MCPB packaging before release.")
        if report["summary"].get("live_gateway_policy_issues"):
            lines.append("  - Review live-gateway LIVE_TOOL_SPECS, registry exposure, scopes, limits, and caveat classes.")

    return "\n".join(lines) + "\n"


def print_doctor(*, json_output: bool = False, cache_root: str | Path | None = None) -> dict[str, Any]:
    report = build_doctor_report(cache_root=cache_root)
    if json_output:
        print(json.dumps(report, indent=2, sort_keys=True))
        return report
    print(format_doctor_report(report), end="")
    return report


def package_version() -> str:
    """Return the installed package version used by doctor and CLI metadata."""

    try:
        return importlib.metadata.version("healthcare-data-mcp")
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def _import_status(module_name: str) -> dict[str, Any]:
    try:
        module = importlib.import_module(module_name)
        version = getattr(module, "__version__", "")
        if not version:
            try:
                version = importlib.metadata.version(module_name.replace("_", "-"))
            except importlib.metadata.PackageNotFoundError:
                version = "importable"
        return {"status": "ok", "version": str(version)}
    except Exception as exc:
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}


def _server_report(spec: ServerCapability) -> dict[str, Any]:
    try:
        importlib.import_module(spec.module)
        import_status = "ok"
        import_error = ""
    except Exception as exc:
        import_status = "error"
        import_error = f"{type(exc).__name__}: {exc}"

    return {
        "server_id": spec.server_id,
        "module": spec.module,
        "port": spec.port,
        "description": spec.description,
        "import_status": import_status,
        "import_error": import_error,
        "port_status": "in_use" if _port_in_use(spec.port) else "free",
        "zero_config": spec.zero_config,
        "gateway_exposure": list(spec.gateway_exposure),
        "profiles": list(spec.profiles),
        "safety_notes": list(spec.safety_notes),
    }


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def _env_report() -> dict[str, Any]:
    required: dict[str, dict[str, Any]] = {}
    optional: dict[str, dict[str, Any]] = {}
    for spec in SERVER_REGISTRY:
        for key in spec.required_env:
            required[key.name] = {
                "name": key.name,
                "configured": bool(os.environ.get(key.name)),
                "description": key.description,
                "servers": sorted({*required.get(key.name, {}).get("servers", []), spec.server_id}),
            }
        for key in spec.optional_env:
            optional[key.name] = {
                "name": key.name,
                "configured": bool(os.environ.get(key.name)),
                "description": key.description,
                "servers": sorted({*optional.get(key.name, {}).get("servers", []), spec.server_id}),
            }
    return {
        "required": sorted(required.values(), key=lambda item: item["name"]),
        "optional": sorted(optional.values(), key=lambda item: item["name"]),
        "env_file_hint": os.environ.get("HC_MCP_ENV_FILE", "./.env"),
    }


def _cache_report(cache_root: str | Path | None) -> dict[str, Any]:
    try:
        from servers.discovery import server as discovery_server

        payload = discovery_server.cache_status_payload(cache_root=Path(cache_root) if cache_root else None)
        entries = payload.get("entries", [])
        not_ready = [
            {
                "dataset_id": str(entry.get("dataset_id", "")),
                "relative_path": str(entry.get("relative_path", "")),
                "status": str(entry.get("readiness_status") or entry.get("status", "")),
                "validation_status": str(entry.get("validation_status", "")),
                "source_period": str(entry.get("source_period", "")),
                "report_eligible": bool(entry.get("report_eligible", False)),
                "next_action": str(entry.get("next_action", "")),
                "ttl_days": entry.get("ttl_days"),
                "source_status": normalize_source_status(
                    entry,
                    retrieval_method="cache",
                    caveat=str(entry.get("next_action") or "Cache/source status is reported by discovery metadata."),
                ),
            }
            for entry in entries
            if (entry.get("readiness_status") or entry.get("status")) != "ready"
        ][:10]
        return {
            "status": "ok",
            "cache_root": payload.get("cache_root", ""),
            "summary": payload.get("summary", {}),
            "entries": payload.get("entries", []),
            "datasets": payload.get("datasets", []),
            "readiness_model": payload.get("readiness_model", ""),
            "allowed_states": payload.get("allowed_states", []),
            "not_ready_examples": not_ready,
        }
    except Exception as exc:
        return {
            "status": "error",
            "cache_root": str(cache_root or Path.home() / ".healthcare-data-mcp" / "cache"),
            "summary": {},
            "entries": [],
            "not_ready_examples": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def _workflow_reports(
    server_reports: list[dict[str, Any]],
    cache_report: dict[str, Any],
    *,
    workflow_contract_validation: dict[str, Any],
    workflow_tool_reference_validation: dict[str, Any],
) -> list[dict[str, Any]]:
    by_server = {entry["server_id"]: entry for entry in server_reports}
    contract_by_workflow = workflow_contract_validation.get("workflows", {})
    reports: list[dict[str, Any]] = []
    for workflow, server_ids in WORKFLOW_PRESETS.items():
        plan = build_workflow_plan(workflow, cache_status=cache_report)
        readiness = plan.get("readiness", {}) if isinstance(plan, dict) else {}
        contract_status = contract_by_workflow.get(workflow, {"status": "not_checked", "issue_count": 0})
        tool_reference_issues = [
            issue
            for issue in workflow_tool_reference_validation.get("issues", [])
            if issue.get("workflow_id") == workflow
        ]
        tool_reference_status = {
            "status": "ok" if not tool_reference_issues else "issues_found",
            "issue_count": len(tool_reference_issues),
        }
        missing: list[str] = [
            f"input:{item}" for item in readiness.get("missing_inputs", [])
        ]
        missing.extend(f"env:{item}" for item in readiness.get("missing_required_env", []))
        missing.extend(f"cache:{item}" for item in readiness.get("missing_caches", []))
        if contract_status.get("status") != "ok":
            missing.append(f"workflow_contract:{contract_status.get('status', 'unknown')}")
        if tool_reference_status["status"] != "ok":
            missing.append(f"workflow_tools:{tool_reference_status['status']}")
        for server_id in server_ids:
            report = by_server.get(server_id)
            if report is None or report["import_status"] != "ok":
                missing.append(f"{server_id}: importable")
        step_status_counts: dict[str, int] = {}
        for step in plan.get("steps", []) if isinstance(plan, dict) else []:
            status = str(step.get("execution_readiness", {}).get("status", "unknown"))
            step_status_counts[status] = step_status_counts.get(status, 0) + 1
        source_resolution = _workflow_source_resolution_summary(plan if isinstance(plan, dict) else {})
        reports.append(
            {
                "workflow": workflow,
                "servers": list(server_ids),
                "status": readiness.get("status", "unknown"),
                "ready": not any(not item.startswith("input:") for item in missing),
                "missing_requirements": missing,
                "missing_inputs": readiness.get("missing_inputs", []),
                "missing_required_env": readiness.get("missing_required_env", []),
                "missing_caches": readiness.get("missing_caches", []),
                "source_resolution": source_resolution,
                "optional_unavailable": readiness.get("optional_unavailable", []),
                "step_status_counts": step_status_counts,
                "contract_validation": contract_status,
                "tool_reference_validation": tool_reference_status,
                "plan_command": f"hc-mcp workflow {workflow} --json",
            }
        )
    return reports


def _workflow_source_resolution_summary(plan: dict[str, Any]) -> dict[str, Any]:
    """Summarize workflow source resolution for doctor readiness output."""

    rows = plan.get("source_resolution", [])
    if not isinstance(rows, list):
        rows = []
    by_status: dict[str, int] = {}
    aliases: list[dict[str, Any]] = []
    registry_datasets: list[str] = []
    unresolved: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "unknown")
        source_id = str(row.get("source_id") or "")
        by_status[status] = by_status.get(status, 0) + 1
        if status == "alias":
            aliases.append(
                {
                    "source_id": source_id,
                    "canonical_dataset_ids": list(row.get("canonical_dataset_ids") or []),
                    "source_type": row.get("source_type", ""),
                    "caveat": row.get("caveat", ""),
                }
            )
        elif status == "registry_dataset" and source_id:
            registry_datasets.append(source_id)
        elif source_id:
            unresolved.append(source_id)

    return {
        "status": "ok" if not unresolved else "issues_found",
        "source_count": len(rows),
        "status_counts": by_status,
        "registry_dataset_ids": registry_datasets,
        "aliases": aliases,
        "unresolved_source_ids": unresolved,
    }


def _evidence_contract_validation(*, repo_root: Path | None = None) -> dict[str, Any]:
    """Validate priority provenance surfaces for doctor readiness output.

    This is intentionally static and read-only. Runtime source facts are still
    validated by the owning server tests and by live-gateway before routing.
    Doctor uses this check to catch obvious adoption drift: a priority tool is
    removed from a server module, or the source checkout no longer contains
    strict report-ready receipt tests.
    """

    root = repo_root or Path(__file__).resolve().parents[2]
    surfaces: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for contract in PRIORITY_EVIDENCE_CONTRACTS:
        surface = str(contract["surface"])
        module_name = str(contract["module"])
        expected_tools = tuple(str(tool) for tool in contract["tools"])
        surface_issues: list[dict[str, Any]] = []

        module_tools_result = _module_fastmcp_tools(module_name)
        module_tools = set(module_tools_result.get("tools", []))
        if module_tools_result.get("status") != "ok":
            surface_issues.append(
                {
                    "surface": surface,
                    "status": "module_not_checked",
                    "module": module_name,
                    "error": module_tools_result.get("error", ""),
                }
            )
        else:
            missing_tools = sorted(set(expected_tools) - module_tools)
            for tool_name in missing_tools:
                surface_issues.append(
                    {
                        "surface": surface,
                        "status": "priority_tool_missing",
                        "module": module_name,
                        "tool": tool_name,
                    }
                )

        test_paths = [root / str(path) for path in contract.get("test_paths", ())]
        existing_test_paths = [path for path in test_paths if path.exists()]
        strict_validation_count = 0
        strict_tested_tools: list[str] = []
        if existing_test_paths:
            test_text_by_path = {
                path: path.read_text(encoding="utf-8")
                for path in existing_test_paths
            }
            test_text = "\n".join(test_text_by_path.values())
            strict_validation_count = test_text.count("require_content=True")
            strict_tested_tools = _strict_receipt_tested_tools(
                expected_tools,
                test_text_by_path,
            )
            if "validate_evidence_receipt" not in test_text:
                surface_issues.append(
                    {
                        "surface": surface,
                        "status": "strict_receipt_validator_missing",
                        "test_paths": [str(path.relative_to(root)) for path in existing_test_paths],
                    }
                )
            if strict_validation_count < 1:
                surface_issues.append(
                    {
                        "surface": surface,
                        "status": "strict_receipt_content_tests_missing",
                        "test_paths": [str(path.relative_to(root)) for path in existing_test_paths],
                    }
                )
            missing_test_tool_mentions = sorted(set(expected_tools) - set(strict_tested_tools))
            if missing_test_tool_mentions:
                surface_issues.append(
                    {
                        "surface": surface,
                        "status": "priority_tool_strict_test_missing",
                        "tools": missing_test_tool_mentions,
                        "test_paths": [str(path.relative_to(root)) for path in existing_test_paths],
                    }
                )
        else:
            surface_issues.append(
                {
                    "surface": surface,
                    "status": "strict_tests_not_checked",
                    "test_paths": [str(path) for path in test_paths],
                    "severity": "not_checked",
                }
            )

        issue_count = sum(1 for issue in surface_issues if issue.get("severity") != "not_checked")
        status = "ok" if issue_count == 0 else "issues_found"
        if not existing_test_paths and not issue_count:
            status = "not_checked"
        surfaces.append(
            {
                "surface": surface,
                "server_id": contract["server_id"],
                "module": module_name,
                "status": status,
                "tool_count": len(expected_tools),
                "tools": list(expected_tools),
                "test_paths": [str(path.relative_to(root)) for path in existing_test_paths],
                "strict_validation_count": strict_validation_count,
                "strict_tested_tool_count": len(strict_tested_tools),
                "strict_tested_tools": strict_tested_tools,
                "issues": surface_issues,
            }
        )
        issues.extend(issue for issue in surface_issues if issue.get("severity") != "not_checked")

    return {
        "status": "ok" if not issues else "issues_found",
        "method": "priority_evidence_contract_static",
        "surface_count": len(surfaces),
        "tool_count": sum(int(surface["tool_count"]) for surface in surfaces),
        "issue_count": len(issues),
        "surfaces": surfaces,
        "issues": issues,
    }


def _strict_receipt_tested_tools(
    expected_tools: tuple[str, ...],
    test_text_by_path: dict[Path, str],
) -> list[str]:
    """Return tools mentioned in a test function that also performs strict receipt validation."""

    strict_tools: set[str] = set()
    for path, test_text in test_text_by_path.items():
        try:
            tree = ast.parse(test_text, filename=str(path))
        except SyntaxError:
            continue
        strict_helper_names = _strict_receipt_helper_names(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
                continue
            if not _function_has_strict_receipt_validation(node, strict_helper_names=strict_helper_names):
                continue
            source = ast.get_source_segment(test_text, node) or ""
            strict_tools.update(tool for tool in expected_tools if tool in source)
    return [tool for tool in expected_tools if tool in strict_tools]


def _strict_receipt_helper_names(tree: ast.AST) -> set[str]:
    """Return helper functions that perform strict receipt validation, directly or transitively."""

    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
    }
    strict_helpers = {
        name
        for name, node in functions.items()
        if _function_has_direct_strict_receipt_validation(node)
    }
    changed = True
    while changed:
        changed = False
        for name, node in functions.items():
            if name in strict_helpers:
                continue
            if _called_function_names(node) & strict_helpers:
                strict_helpers.add(name)
                changed = True
    return strict_helpers


def _function_has_strict_receipt_validation(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
    *,
    strict_helper_names: set[str] | None = None,
) -> bool:
    if _function_has_direct_strict_receipt_validation(node):
        return True
    return bool(_called_function_names(node) & (strict_helper_names or set()))


def _function_has_direct_strict_receipt_validation(node: ast.AsyncFunctionDef | ast.FunctionDef) -> bool:
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        candidate = child.func.attr if isinstance(child.func, ast.Attribute) else getattr(child.func, "id", "")
        if candidate != "validate_evidence_receipt":
            continue
        for keyword in child.keywords:
            if (
                keyword.arg == "require_content"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value is True
            ):
                return True
    return False


def _called_function_names(node: ast.AsyncFunctionDef | ast.FunctionDef) -> set[str]:
    names: set[str] = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        if isinstance(child.func, ast.Name):
            names.add(child.func.id)
        elif isinstance(child.func, ast.Attribute):
            names.add(child.func.attr)
    return names


def _ast_call_count(tree: ast.AST, function_name: str) -> int:
    count = 0
    for child in ast.walk(tree):
        if not isinstance(child, ast.Call):
            continue
        candidate = child.func.attr if isinstance(child.func, ast.Attribute) else getattr(child.func, "id", "")
        if candidate == function_name:
            count += 1
    return count


def _module_fastmcp_tools(module_name: str) -> dict[str, Any]:
    spec = importlib.util.find_spec(module_name)
    if spec is None or not spec.origin:
        return {"status": "error", "tools": [], "error": f"module source not found: {module_name}"}
    path = Path(spec.origin)
    if not path.exists():
        return {"status": "error", "tools": [], "error": f"module source not found: {path}"}
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"status": "error", "tools": [], "error": f"{type(exc).__name__}: {exc}"}

    tools: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and _has_fastmcp_tool_decorator(node):
            tools.append(node.name)
    return {"status": "ok", "path": str(path), "tools": sorted(tools)}


def _has_fastmcp_tool_decorator(node: ast.AsyncFunctionDef | ast.FunctionDef) -> bool:
    for decorator in node.decorator_list:
        candidate = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(candidate, ast.Attribute) and candidate.attr == "tool":
            return True
    return False


def _client_config_hints() -> list[str]:
    return [
        "List servers with hc-mcp --list.",
        "Run local stdio servers as hc-mcp <server>, for example hc-mcp public-records.",
        "For GUI clients launched outside the repo, set HC_MCP_ENV_FILE=/absolute/path/to/.env.",
        "Use hc-mcp gateway for metadata-only remote discovery and hc-mcp live-gateway only with auth.",
    ]


def _remote_gateway_posture() -> dict[str, Any]:
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    warnings: list[str] = []
    if host in {"0.0.0.0", "::"}:
        warnings.append("MCP_HOST exposes HTTP servers on all interfaces; use only inside Docker or behind trusted HTTPS/auth.")
    if os.environ.get("MCP_LIVE_GATEWAY_AUTH_REQUIRED", "true").strip().lower() in {"0", "false", "no", "off"}:
        warnings.append("Live gateway auth is disabled in environment; do not expose live-gateway over HTTP/SSE.")
    return {
        "metadata_gateway": "search/fetch metadata only",
        "live_gateway": "allowlisted live tools with bearer-token auth required for HTTP/SSE",
        "warnings": warnings,
    }


def _live_gateway_policy_validation() -> dict[str, Any]:
    """Statically validate live-gateway policy wiring without starting the gateway."""

    module_name = "servers.live_gateway.server"
    policy_runner_module_name = "servers.live_gateway.policy_runner"
    module_spec = importlib.util.find_spec(module_name)
    issues: list[dict[str, Any]] = []
    if module_spec is None or not module_spec.origin:
        return {
            "status": "issues_found",
            "method": "live_gateway_static_policy_ast",
            "tool_count": 0,
            "live_server_count": 0,
            "bulk_tool_count": 0,
            "provenance_required_tool_count": 0,
            "rate_limit_classes": [],
            "source_caveat_classes": [],
            "scope_sets": [],
            "shared_evidence_validation": {"status": "not_checked", "call_count": 0},
            "issues": [
                {
                    "status": "module_not_found",
                    "module": module_name,
                    "message": "Could not find live-gateway module source.",
                }
            ],
            "issue_count": 1,
        }

    path = Path(module_spec.origin)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except Exception as exc:
        return {
            "status": "issues_found",
            "method": "live_gateway_static_policy_ast",
            "tool_count": 0,
            "live_server_count": 0,
            "bulk_tool_count": 0,
            "provenance_required_tool_count": 0,
            "rate_limit_classes": [],
            "source_caveat_classes": [],
            "scope_sets": [],
            "shared_evidence_validation": {"status": "not_checked", "call_count": 0},
            "issues": [
                {
                    "status": "module_parse_failed",
                    "module": module_name,
                    "message": f"{type(exc).__name__}: {exc}",
                }
            ],
            "issue_count": 1,
        }

    policy_runner_tree: ast.AST | None = None
    policy_runner_spec = importlib.util.find_spec(policy_runner_module_name)
    if policy_runner_spec is None or not policy_runner_spec.origin:
        issues.append(
            {
                "status": "policy_runner_module_not_found",
                "module": policy_runner_module_name,
                "message": "Could not find live-gateway policy runner module source.",
            }
        )
    else:
        policy_runner_path = Path(policy_runner_spec.origin)
        try:
            policy_runner_tree = ast.parse(
                policy_runner_path.read_text(encoding="utf-8"),
                filename=str(policy_runner_path),
            )
        except Exception as exc:
            issues.append(
                {
                    "status": "policy_runner_module_parse_failed",
                    "module": policy_runner_module_name,
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )

    specs = _live_tool_specs_from_tree(tree)
    rate_limit_policies = _literal_assignment(tree, "_RATE_LIMIT_POLICIES", default={})
    allowed_live_scopes = _literal_assignment(tree, "_ALLOWED_LIVE_SCOPES", default={"mcp:read", "mcp:bulk"})
    policy_tree = policy_runner_tree or tree
    source_caveat_classes = _literal_assignment(policy_tree, "SOURCE_CAVEAT_CLASSES", default={})
    category_caveat_class = _literal_assignment(policy_tree, "_CATEGORY_CAVEAT_CLASS", default={})
    shared_evidence_validation_call_count = _ast_call_count(tree, "evidence_receipt_validation_summary")
    if policy_runner_tree is not None:
        shared_evidence_validation_call_count += _ast_call_count(
            policy_runner_tree,
            "evidence_receipt_validation_summary",
        )
    shared_evidence_validation = {
        "status": "ok" if shared_evidence_validation_call_count else "missing",
        "call_count": shared_evidence_validation_call_count,
        "helper": "shared.utils.mcp_response.evidence_receipt_validation_summary",
    }
    if not shared_evidence_validation_call_count:
        issues.append(
            {
                "status": "live_gateway_shared_evidence_validation_missing",
                "module": module_name,
                "message": (
                    "live-gateway must use the shared nested evidence receipt validator "
                    "instead of a private provenance traversal."
                ),
            }
        )
    rate_limit_names = set(rate_limit_policies) if isinstance(rate_limit_policies, dict) else set()
    allowed_scope_names = set(allowed_live_scopes) if isinstance(allowed_live_scopes, set | list | tuple) else set()
    source_caveat_names = set(source_caveat_classes) if isinstance(source_caveat_classes, dict) else set()
    category_caveats = category_caveat_class if isinstance(category_caveat_class, dict) else {}
    covered_evidence_tools = {
        tool
        for contract in PRIORITY_EVIDENCE_CONTRACTS
        for tool in contract["tools"]
    }

    seen_tools: set[str] = set()
    module_tools_cache: dict[str, set[str]] = {}
    live_servers: set[str] = set()
    scope_sets: set[str] = set()
    bulk_tool_count = 0
    provenance_required_tool_count = 0
    for spec in specs:
        tool_name = str(spec.get("tool_name", ""))
        server_id = str(spec.get("server", ""))
        module = str(spec.get("module", ""))
        category = str(spec.get("category", ""))
        scopes = tuple(str(scope) for scope in spec.get("scopes", ("mcp:read",)))
        rate_limit_class = str(spec.get("rate_limit_class", "standard"))
        source_caveat_class = str(
            category_caveats.get(category, spec.get("source_caveat_class", "public_source"))
        )
        require_provenance = bool(spec.get("require_provenance", True))
        request_size_limit = int(spec.get("request_size_limit_bytes", 0) or 0)
        result_size_limit = int(spec.get("result_size_limit_bytes", 0) or 0)
        result_limit = int(spec.get("result_limit", 0) or 0)

        live_servers.add(server_id)
        scope_sets.add("+".join(scopes))
        if "mcp:bulk" in scopes:
            bulk_tool_count += 1
        if require_provenance:
            provenance_required_tool_count += 1

        if tool_name in seen_tools:
            issues.append({"status": "duplicate_tool", "tool": tool_name})
        seen_tools.add(tool_name)

        registry_spec = SERVER_BY_ID.get(server_id)
        if registry_spec is None:
            issues.append({"status": "unknown_registry_server", "tool": tool_name, "server": server_id})
            continue
        if module != registry_spec.module:
            issues.append(
                {
                    "status": "module_mismatch",
                    "tool": tool_name,
                    "server": server_id,
                    "module": module,
                    "registry_module": registry_spec.module,
                }
            )
        if "live" not in registry_spec.gateway_exposure:
            issues.append({"status": "server_not_live_exposed", "tool": tool_name, "server": server_id})
        if not registry_spec.dataset_ids:
            issues.append({"status": "live_server_missing_dataset_ids", "tool": tool_name, "server": server_id})
        if not scopes:
            issues.append({"status": "missing_scopes", "tool": tool_name, "server": server_id})
        unknown_scopes = sorted(set(scopes) - allowed_scope_names)
        if unknown_scopes:
            issues.append(
                {
                    "status": "unknown_scope",
                    "tool": tool_name,
                    "server": server_id,
                    "scopes": unknown_scopes,
                }
            )
        if scopes and "mcp:read" not in scopes:
            issues.append({"status": "missing_baseline_read_scope", "tool": tool_name, "server": server_id})
        if request_size_limit <= 0 or result_size_limit <= 0 or result_limit <= 0:
            issues.append({"status": "non_positive_policy_limit", "tool": tool_name, "server": server_id})
        if rate_limit_class not in rate_limit_names:
            issues.append(
                {
                    "status": "unknown_rate_limit_class",
                    "tool": tool_name,
                    "server": server_id,
                    "rate_limit_class": rate_limit_class,
                }
            )
        if source_caveat_class not in source_caveat_names:
            issues.append(
                {
                    "status": "unknown_source_caveat_class",
                    "tool": tool_name,
                    "server": server_id,
                    "source_caveat_class": source_caveat_class,
                }
            )
        if require_provenance and tool_name not in covered_evidence_tools:
            issues.append({"status": "provenance_tool_not_in_priority_evidence_contract", "tool": tool_name})

        module_tools = module_tools_cache.get(module)
        if module_tools is None:
            module_tools_result = _module_fastmcp_tools(module)
            module_tools = set(module_tools_result.get("tools", []))
            module_tools_cache[module] = module_tools
            if module_tools_result.get("status") != "ok":
                issues.append(
                    {
                        "status": "owning_module_not_checked",
                        "tool": tool_name,
                        "module": module,
                        "message": str(module_tools_result.get("error") or ""),
                    }
                )
                continue
        if tool_name not in module_tools:
            issues.append({"status": "allowlisted_tool_missing_from_module", "tool": tool_name, "module": module})

    return {
        "status": "ok" if not issues else "issues_found",
        "method": "live_gateway_static_policy_ast",
        "module": module_name,
        "policy_runner_module": policy_runner_module_name,
        "tool_count": len(specs),
        "live_server_count": len(live_servers),
        "bulk_tool_count": bulk_tool_count,
        "provenance_required_tool_count": provenance_required_tool_count,
        "rate_limit_classes": sorted(rate_limit_names),
        "source_caveat_classes": sorted(source_caveat_names),
        "scope_sets": sorted(scope_sets),
        "shared_evidence_validation": shared_evidence_validation,
        "issues": issues,
        "issue_count": len(issues),
    }


def _live_tool_specs_from_tree(tree: ast.AST) -> list[dict[str, Any]]:
    assignment = _find_assignment(tree, "LIVE_TOOL_SPECS")
    if assignment is None or not isinstance(assignment.value, ast.Tuple):
        return []

    specs: list[dict[str, Any]] = []
    defaults = {
        "scopes": ("mcp:read",),
        "request_size_limit_bytes": 32768,
        "result_size_limit_bytes": 262144,
        "result_limit": 100,
        "rate_limit_class": "standard",
        "source_caveat_class": "public_source",
        "require_provenance": True,
    }
    field_names = ("server", "module", "tool_name", "category")
    for element in assignment.value.elts:
        if not isinstance(element, ast.Call):
            continue
        function_name = element.func.id if isinstance(element.func, ast.Name) else ""
        if function_name != "LiveToolSpec":
            continue
        spec = dict(defaults)
        for index, field_name in enumerate(field_names):
            if index < len(element.args):
                spec[field_name] = _literal_node_value(element.args[index], default="")
        for keyword in element.keywords:
            if keyword.arg:
                spec[keyword.arg] = _literal_node_value(keyword.value, default=spec.get(keyword.arg))
        specs.append(spec)
    return specs


def _literal_assignment(tree: ast.AST, name: str, *, default: Any) -> Any:
    assignment = _find_assignment(tree, name)
    if assignment is None:
        return default
    return _literal_node_value(assignment.value, default=default)


def _find_assignment(tree: ast.AST, name: str) -> ast.Assign | ast.AnnAssign | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return node
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == name:
            return node
    return None


def _literal_node_value(node: ast.AST, *, default: Any) -> Any:
    try:
        return ast.literal_eval(node)
    except Exception:
        return default


def _registry_artifact_checks() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    artifact_specs = _registry_artifact_specs(repo_root)
    artifacts: list[dict[str, Any]] = []
    for spec in artifact_specs:
        name = spec["name"]
        path = spec["path"]
        if not isinstance(path, Path):
            continue
        if not path.exists():
            artifacts.append(
                {
                    "name": name,
                    "status": "not_checked",
                    "path": str(path),
                    "reason": "checked-in artifact not present in this install context",
                }
            )
            continue
        try:
            expected = spec["renderer"]()
            current_reader = spec.get("current_reader") or (lambda current_path: current_path.read_text(encoding="utf-8"))
            current = current_reader(path)
        except Exception as exc:
            artifacts.append(
                {
                    "name": name,
                    "status": "not_checked",
                    "path": str(path),
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        artifacts.append(
            {
                "name": name,
                "status": "current" if current == expected else "drifted",
                "path": str(path),
                "regenerate": spec["regenerate"],
            }
        )

    checked_count = sum(1 for item in artifacts if item["status"] in {"current", "drifted"})
    drift_count = sum(1 for item in artifacts if item["status"] == "drifted")
    not_checked_count = sum(1 for item in artifacts if item["status"] == "not_checked")
    return {
        "status": "current" if drift_count == 0 else "action_needed",
        "checked_count": checked_count,
        "drift_count": drift_count,
        "not_checked_count": not_checked_count,
        "artifacts": artifacts,
    }


def _metadata_catalog_validation() -> dict[str, Any]:
    """Validate registry-backed discovery and metadata-gateway catalogs."""

    checks: list[dict[str, Any]] = []
    checks.append(
        _metadata_catalog_check(
            name="discovery_dataset_catalog",
            function_path="servers.discovery.server.validate_dataset_catalog_contracts",
            validator=lambda: _call_metadata_validator(
                "servers.discovery.server",
                "validate_dataset_catalog_contracts",
            ),
        )
    )
    checks.append(
        _metadata_catalog_check(
            name="gateway_dataset_contracts",
            function_path="servers.gateway.server.validate_gateway_dataset_contracts",
            validator=lambda: _call_metadata_validator(
                "servers.gateway.server",
                "validate_gateway_dataset_contracts",
            ),
        )
    )
    issues: list[dict[str, Any]] = []
    for check in checks:
        for issue in check.get("issues", []):
            scoped_issue = dict(issue)
            scoped_issue.setdefault("name", check["name"])
            issues.append(scoped_issue)
        if check["status"] not in {"ok", "not_checked"} and not check.get("issues"):
            issues.append(
                {
                    "name": check["name"],
                    "status": check["status"],
                    "message": check.get("message", ""),
                }
            )
    issue_count = len(issues)
    return {
        "status": "ok" if issue_count == 0 else "issues_found",
        "method": "registry_metadata_catalog_contracts",
        "check_count": len(checks),
        "issue_count": issue_count,
        "checks": checks,
        "issues": issues,
    }


def _metadata_catalog_check(
    *,
    name: str,
    function_path: str,
    validator: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    try:
        result = validator()
    except Exception as exc:
        return {
            "name": name,
            "status": "issues_found",
            "function": function_path,
            "message": f"{type(exc).__name__}: {exc}",
            "issue_count": 1,
            "issues": [
                {
                    "status": "metadata_catalog_validator_failed",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            ],
        }
    payload = dict(result)
    payload["name"] = name
    payload["function"] = function_path
    return payload


def _call_metadata_validator(module_name: str, function_name: str) -> dict[str, Any]:
    module = importlib.import_module(module_name)
    validator = getattr(module, function_name)
    result = validator()
    if not isinstance(result, dict):
        raise TypeError(f"{module_name}.{function_name} returned {type(result).__name__}, expected dict")
    return result


def _distribution_report() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    pyproject_path = repo_root / "pyproject.toml"
    dockerfile_path = repo_root / "Dockerfile"
    compose_paths = (
        repo_root / "docker-compose.yml",
        repo_root / "docker-compose.zero-config.yml",
    )
    checks: list[dict[str, Any]] = []

    if not pyproject_path.exists():
        return {
            "status": "not_checked",
            "issue_count": 0,
            "checks": [
                {
                    "name": "python_package_metadata",
                    "status": "not_checked",
                    "message": "pyproject.toml is not present in this install context.",
                }
            ],
        }

    try:
        pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "action_needed",
            "issue_count": 1,
            "checks": [
                {
                    "name": "python_package_metadata",
                    "status": "error",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            ],
        }

    project = pyproject.get("project", {})
    version = str(project.get("version") or "")
    name = str(project.get("name") or "")
    checks.append(
        _distribution_check(
            name="python_package_metadata",
            ok=(
                name == "healthcare-data-mcp"
                and bool(version)
                and project.get("readme") == "README.md"
                and project.get("requires-python") == ">=3.11"
            ),
            message=f"{name or '<missing>'} {version or '<missing>'}",
        )
    )

    scripts = project.get("scripts", {}) if isinstance(project.get("scripts"), dict) else {}
    checks.append(
        _distribution_check(
            name="console_entry_points",
            ok=scripts.get("hc-mcp") == "servers._launcher:main"
            and scripts.get("hc-mcp-setup") == "shared.setup_wizard:main",
            message="hc-mcp and hc-mcp-setup console scripts",
        )
    )

    force_include = (
        pyproject.get("tool", {})
        .get("hatch", {})
        .get("build", {})
        .get("targets", {})
        .get("wheel", {})
        .get("force-include", {})
    )
    if not isinstance(force_include, dict):
        force_include = {}
    expected_aliases = _expected_force_include_aliases(repo_root)
    missing_aliases = {
        source: destination
        for source, destination in expected_aliases.items()
        if force_include.get(source) != destination
    }
    extra_aliases = sorted(set(force_include) - set(expected_aliases))
    checks.append(
        _distribution_check(
            name="wheel_force_include_registry_aliases",
            ok=not missing_aliases and not extra_aliases,
            message=(
                f"{len(expected_aliases)} registry module alias(es); "
                f"missing={len(missing_aliases)} extra={len(extra_aliases)}"
            ),
        )
    )

    if dockerfile_path.exists() and all(path.exists() for path in compose_paths):
        compose_image_line = 'image: "$' + f'{{HC_MCP_IMAGE:-healthcare-data-mcp:{version}}}"'
        dockerfile = dockerfile_path.read_text(encoding="utf-8")
        compose_texts = [path.read_text(encoding="utf-8") for path in compose_paths]
        checks.append(
            _distribution_check(
                name="versioned_container_metadata",
                ok=(
                    bool(version)
                    and f"ARG VERSION={version}" in dockerfile
                    and "org.opencontainers.image.version" in dockerfile
                    and all(compose_image_line in text for text in compose_texts)
                ),
                message=f"package/container version {version or '<missing>'}",
            )
        )
    else:
        checks.append(
            {
                "name": "versioned_container_metadata",
                "status": "not_checked",
                "message": "Dockerfile or Compose files are not present in this install context.",
            }
        )

    checks.append(_onboarding_script_contract_check(repo_root))
    checks.append(_ci_product_readiness_gate_check(repo_root))

    issue_count = sum(1 for check in checks if check["status"] in {"error", "action_needed"})
    return {
        "status": "ok" if issue_count == 0 else "action_needed",
        "issue_count": issue_count,
        "package_name": name,
        "package_version": version,
        "checks": checks,
    }


def _onboarding_script_contract_check(repo_root: Path) -> dict[str, Any]:
    script_snippets = {
        "install.sh": (
            "from shared.utils.server_registry import SERVER_REGISTRY",
            "load_server_registry",
            "load_zero_config_compose_registry",
            "--dry-run",
            "Dry run completed without cloning, installing, writing config, or registering clients",
            "Unknown installer option",
            "Registry-defined environment keys enable optional and key-required tools",
            "--env \"HC_MCP_ENV_FILE=$ENV_FILE\"",
            'server_id="${SERVER_IDS[$name]:-${name#hc-}}"',
            'servers._launcher "$server_id"',
        ),
        "scripts/register-codex.sh": (
            "from shared.utils.server_registry import SERVER_REGISTRY",
            "--dry-run",
            "Dry run: no Codex config changes will be made.",
            "Registry entries:",
            "--url http://localhost:$port/mcp",
            "HC_MCP_ENV_FILE=$ENV_FILE",
            "servers._launcher",
            "Use --dry-run to preview registry-backed registrations without Codex installed.",
        ),
        "scripts/setup.sh": (
            "intentionally read-only when run without arguments",
            "run_doctor",
            "hc-mcp doctor",
            "hc-mcp-setup",
            "-m servers._launcher doctor",
            "-m shared.setup_wizard",
        ),
    }

    missing_files: list[str] = []
    missing_snippets: dict[str, list[str]] = {}
    for relative_path, snippets in script_snippets.items():
        path = repo_root / relative_path
        if not path.exists():
            missing_files.append(relative_path)
            continue
        text = path.read_text(encoding="utf-8")
        missing = [snippet for snippet in snippets if snippet not in text]
        if missing:
            missing_snippets[relative_path] = missing

    total_snippets = sum(len(snippets) for snippets in script_snippets.values())
    missing_count = sum(len(snippets) for snippets in missing_snippets.values())
    check = _distribution_check(
        name="read_only_onboarding_scripts",
        ok=not missing_files and not missing_snippets,
        message=(
            f"{len(script_snippets) - len(missing_files)}/{len(script_snippets)} scripts present; "
            f"{total_snippets - missing_count}/{total_snippets} onboarding contract snippets present"
        ),
    )
    if missing_files:
        check["missing_files"] = missing_files
    if missing_snippets:
        check["missing_snippets"] = missing_snippets
    return check


def _ci_product_readiness_gate_check(repo_root: Path) -> dict[str, Any]:
    ci_path = repo_root / ".github" / "workflows" / "ci.yml"
    if not ci_path.exists():
        return {
            "name": "ci_product_readiness_gates",
            "status": "not_checked",
            "message": "CI workflow is not present in this install context.",
        }

    ci = ci_path.read_text(encoding="utf-8")
    expected = _ci_product_readiness_gate_snippets()
    missing = [snippet for snippet in expected if snippet not in ci]
    check = _distribution_check(
        name="ci_product_readiness_gates",
        ok=not missing,
        message=f"{len(expected) - len(missing)}/{len(expected)} expected CI gate snippets present",
    )
    if missing:
        check["missing_snippets"] = missing
    return check


def _ci_product_readiness_gate_snippets() -> tuple[str, ...]:
    return (
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
        "python scripts/mcp_smoke.py --server discovery --expect-tool list_workflows --expect-resource healthcare-data://workflows/catalog --call-tool list_workflows",
        "python scripts/mcp_smoke.py --server discovery --expect-tool get_workflow_plan --call-tool get_workflow_plan",
        '"workflow_id":"system_reconciliation"',
        '"query":"Jefferson Health"',
        '"system_slug":"jefferson-health"',
        "python scripts/mcp_smoke.py --server discovery --expect-tool list_presets --expect-resource healthcare-data://presets/catalog --call-tool list_presets",
        "python scripts/mcp_smoke.py --server discovery --expect-tool get_preset_plan --call-tool get_preset_plan",
        "--expect-structured-key report_ingest_contract",
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
        "--expect-structured-key workflow_summaries",
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


def _expected_force_include_aliases(repo_root: Path) -> dict[str, str]:
    expected_aliases: dict[str, str] = {}
    for spec in SERVER_REGISTRY:
        module_parts = spec.module.split(".")
        if len(module_parts) < 2 or module_parts[0] != "servers":
            continue
        package_name = module_parts[1]
        source_dir = repo_root / "servers" / spec.server_id
        if source_dir.exists() and spec.server_id != package_name:
            expected_aliases[f"servers/{spec.server_id}"] = f"servers/{package_name}"
    return expected_aliases


def _distribution_check(*, name: str, ok: bool, message: str) -> dict[str, Any]:
    return {
        "name": name,
        "status": "ok" if ok else "action_needed",
        "message": message,
    }


def _registry_artifact_specs(repo_root: Path) -> list[dict[str, Any]]:
    try:
        from scripts.render_client_configs import (
            render_claude_desktop_config,
            render_claude_desktop_stdio_example,
            render_codex_config,
            render_http_clients_config,
            render_project_mcp_config,
        )
        from scripts.build_mcpb import validate_manifest_registry_sync
        from scripts.render_compose import render_compose
        from scripts.render_env_example import render_env_example
        from scripts.render_registry_docs import checked_in_snippet, render_snippet
    except Exception as exc:
        reason = f"registry renderer imports unavailable: {type(exc).__name__}: {exc}"
        return [
            {
                "name": "registry renderers",
                "path": repo_root,
                "renderer": _not_checked_renderer(reason),
                "regenerate": "",
            }
        ]

    specs = [
        {
            "name": ".env.example",
            "path": repo_root / ".env.example",
            "renderer": render_env_example,
            "regenerate": "python scripts/render_env_example.py > .env.example",
        },
        {
            "name": "docker-compose.yml",
            "path": repo_root / "docker-compose.yml",
            "renderer": lambda: render_compose(zero_config_only=False),
            "regenerate": "python scripts/render_compose.py full > docker-compose.yml",
        },
        {
            "name": "docker-compose.zero-config.yml",
            "path": repo_root / "docker-compose.zero-config.yml",
            "renderer": lambda: render_compose(zero_config_only=True),
            "regenerate": "python scripts/render_compose.py zero-config > docker-compose.zero-config.yml",
        },
        {
            "name": ".mcp.json",
            "path": repo_root / ".mcp.json",
            "renderer": render_project_mcp_config,
            "regenerate": "python scripts/render_client_configs.py project-mcp > .mcp.json",
        },
        {
            "name": "examples/codex-config.toml",
            "path": repo_root / "examples" / "codex-config.toml",
            "renderer": render_codex_config,
            "regenerate": "python scripts/render_client_configs.py codex > examples/codex-config.toml",
        },
        {
            "name": "examples/claude-desktop-stdio.json",
            "path": repo_root / "examples" / "claude-desktop-stdio.json",
            "renderer": render_claude_desktop_stdio_example,
            "regenerate": "python scripts/render_client_configs.py claude-desktop-stdio > examples/claude-desktop-stdio.json",
        },
        {
            "name": "configs/http-clients.json",
            "path": repo_root / "configs" / "http-clients.json",
            "renderer": render_http_clients_config,
            "regenerate": "python scripts/render_client_configs.py http-clients > configs/http-clients.json",
        },
        {
            "name": "configs/claude-desktop.json",
            "path": repo_root / "configs" / "claude-desktop.json",
            "renderer": render_claude_desktop_config,
            "regenerate": "python scripts/render_client_configs.py claude-desktop > configs/claude-desktop.json",
        },
        {
            "name": "desktop-extension/manifest.json",
            "path": repo_root / "desktop-extension" / "manifest.json",
            "renderer": lambda: "",
            "current_reader": lambda path: "\n".join(
                validate_manifest_registry_sync(json.loads(path.read_text(encoding="utf-8")), "cms-facility")
            ),
            "regenerate": "python scripts/build_mcpb.py --check",
        },
    ]
    for snippet in (
        "server-catalog",
        "preset-catalog",
        "workflow-catalog",
        "live-gateway-catalog",
        "source-ledger-registry",
        "http-client-catalog",
        "env-catalog",
    ):
        try:
            path, _current = checked_in_snippet(snippet)
            specs.append(
                {
                    "name": f"docs:{snippet}",
                    "path": path,
                    "renderer": lambda snippet=snippet: render_snippet(snippet).strip(),
                    "current_reader": lambda _path, snippet=snippet: checked_in_snippet(snippet)[1].strip(),
                    "regenerate": f"python scripts/render_registry_docs.py {snippet}",
                }
            )
        except Exception as exc:
            specs.append(
                {
                    "name": f"docs:{snippet}",
                    "path": repo_root,
                    "renderer": _not_checked_renderer(f"{type(exc).__name__}: {exc}"),
                    "regenerate": f"python scripts/render_registry_docs.py {snippet}",
                }
            )
    return specs


def _not_checked_renderer(reason: str) -> Callable[[], str]:
    def _raise() -> str:
        raise RuntimeError(reason)

    return _raise


__all__ = ["build_doctor_report", "format_doctor_report", "print_doctor"]
