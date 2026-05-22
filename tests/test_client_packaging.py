from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from zipfile import ZipFile

from scripts.build_mcpb import (
    DEFAULT_MANIFEST,
    MCPB_EXCLUDED_ENV_KEYS,
    SERVER_CHOICE_VALUES,
    SERVER_NAMES,
    load_manifest,
    project_version,
    registry_env_keys_for_mcpb,
    server_choice_description,
    validate_manifest_registry_sync,
)
from scripts.render_client_configs import (
    codex_key,
    render_claude_desktop_config,
    render_claude_desktop_stdio_example,
    render_codex_config,
    render_http_clients_config,
    render_project_mcp_config,
)
from scripts.render_compose import compose_image_reference, render_compose
from scripts.render_env_example import expected_env_names, render_env_example
from servers._launcher import SERVERS
from shared.utils.server_registry import SERVER_BY_ID, SERVER_REGISTRY


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_desktop_extension_manifest_server_choices_match_registry() -> None:
    manifest = json.loads((REPO_ROOT / "desktop-extension" / "manifest.json").read_text(encoding="utf-8"))
    server_config = manifest["user_config"]["server_name"]

    assert server_config["enum"] == list(SERVER_CHOICE_VALUES)
    assert server_config["description"] == server_choice_description()
    assert server_config["default"] == "cms-facility"
    for spec in SERVER_REGISTRY:
        assert f"{spec.server_id}: {spec.description}" in server_config["description"]


def test_desktop_extension_manifest_env_fields_match_registry() -> None:
    manifest = load_manifest(DEFAULT_MANIFEST, "cms-facility")
    user_config = manifest["user_config"]
    mcp_env = manifest["server"]["mcp_config"]["env"]
    win_env = manifest["server"]["mcp_config"]["platform_overrides"]["win32"]["env"]
    env_names = {key.name for key in registry_env_keys_for_mcpb()}
    registry_names = {
        key.name
        for spec in SERVER_REGISTRY
        for key in (*spec.required_env, *spec.optional_env)
        if key.name not in MCPB_EXCLUDED_ENV_KEYS
    }

    assert env_names == registry_names
    assert env_names <= set(user_config)
    assert env_names <= set(mcp_env)
    assert env_names <= set(win_env)
    for env_name in env_names:
        assert mcp_env[env_name] == f"${{user_config.{env_name}}}"
        assert win_env[env_name] == f"${{user_config.{env_name}}}"
        assert user_config[env_name]["description"]
        assert user_config[env_name]["required"] is False
    assert "MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND" not in user_config


def test_desktop_extension_source_manifest_is_registry_current() -> None:
    manifest = json.loads(DEFAULT_MANIFEST.read_text(encoding="utf-8"))

    assert validate_manifest_registry_sync(manifest, "cms-facility") == []
    assert manifest["version"] == project_version()


def test_desktop_extension_manifest_check_rejects_package_version_drift() -> None:
    manifest = json.loads(DEFAULT_MANIFEST.read_text(encoding="utf-8"))
    manifest["version"] = "0.0.0"

    errors = validate_manifest_registry_sync(manifest, "cms-facility")

    assert any("does not match pyproject version" in error for error in errors)


def test_mcpb_manifest_check_is_read_only_and_registry_backed() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/build_mcpb.py", "--check"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Manifest registry fields are current" in result.stdout


def test_mcpb_builder_accepts_all_launcher_servers() -> None:
    assert SERVER_NAMES == set(SERVERS)


def test_mcpb_builder_rewrites_staged_manifest_choices_from_registry() -> None:
    manifest = load_manifest(DEFAULT_MANIFEST, "public-records")
    server_config = manifest["user_config"]["server_name"]

    assert server_config["default"] == "public-records"
    assert server_config["enum"] == list(SERVER_CHOICE_VALUES)
    assert server_config["description"] == server_choice_description()


def test_mcpb_skeleton_archive_contains_registry_rewritten_manifest() -> None:
    output = REPO_ROOT / "dist" / "test-client-packaging.mcpb"
    stage_dir = REPO_ROOT / "build" / "test-client-packaging-mcpb"
    try:
        subprocess.run(
            [
                sys.executable,
                "scripts/build_mcpb.py",
                "--skip-dependency-install",
                "--force",
                "--server-name",
                "public-records",
                "--stage-dir",
                str(stage_dir),
                "--output",
                str(output),
            ],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        with ZipFile(output) as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
            launcher = archive.read("server/launcher.py").decode("utf-8")

        assert {"manifest.json", "server/launcher.py"} <= names
        assert all("__pycache__" not in name for name in names)
        assert manifest["name"] == "healthcare-data-mcp"
        assert manifest["server"]["mcp_config"]["args"][-1] == "${user_config.server_name}"
        server_config = manifest["user_config"]["server_name"]
        assert server_config["default"] == "public-records"
        assert server_config["enum"] == list(SERVER_CHOICE_VALUES)
        assert server_config["description"] == server_choice_description()
        assert "OSRM_BASE_URL" in manifest["user_config"]
        assert "DOCGRAPH_CSV_PATH" in manifest["server"]["mcp_config"]["env"]
        assert "from servers._launcher import main as launcher_main" in launcher
    finally:
        output.unlink(missing_ok=True)
        shutil.rmtree(stage_dir, ignore_errors=True)


def test_codex_example_includes_new_servers_and_http_entries() -> None:
    config_text = (REPO_ROOT / "examples" / "codex-config.toml").read_text(encoding="utf-8")
    config = tomllib.loads(config_text)
    mcp_servers = config["mcp_servers"]

    assert config_text == render_codex_config()
    assert set(mcp_servers) == {
        *(codex_key(spec.server_id) for spec in SERVER_REGISTRY),
        *(codex_key(spec.server_id, http=True) for spec in SERVER_REGISTRY),
    }
    for spec in SERVER_REGISTRY:
        assert mcp_servers[codex_key(spec.server_id)]["args"] == [spec.server_id]
        assert mcp_servers[codex_key(spec.server_id, http=True)]["url"] == f"http://127.0.0.1:{spec.port}/mcp"


def test_claude_desktop_stdio_example_is_valid_json_and_includes_env_pointer() -> None:
    config_text = (REPO_ROOT / "examples" / "claude-desktop-stdio.json").read_text(encoding="utf-8")
    config = json.loads(config_text)
    mcp_servers = config["mcpServers"]

    assert config_text == render_claude_desktop_stdio_example()
    assert set(mcp_servers) == set(SERVERS)
    assert mcp_servers["provider-enrollment"]["args"] == ["provider-enrollment"]
    assert mcp_servers["community-health"]["args"] == ["community-health"]
    assert mcp_servers["research-trials"]["args"] == ["research-trials"]
    assert mcp_servers["live-gateway"]["args"] == ["live-gateway"]
    assert "HC_MCP_ENV_FILE" in mcp_servers["public-records"]["env"]


def test_project_mcp_json_matches_registry_renderer() -> None:
    config_text = (REPO_ROOT / ".mcp.json").read_text(encoding="utf-8")
    config = json.loads(config_text)
    mcp_servers = config["mcpServers"]

    assert config_text == render_project_mcp_config()
    assert set(mcp_servers) == set(SERVER_BY_ID)
    for server_id, entry in mcp_servers.items():
        assert entry["type"] == "stdio"
        assert entry["command"] == "hc-mcp"
        assert entry["args"] == [server_id]


def test_shared_claude_desktop_config_uses_launcher_for_all_registry_servers() -> None:
    config_text = (REPO_ROOT / "configs" / "claude-desktop.json").read_text(encoding="utf-8")
    config = json.loads(config_text)
    mcp_servers = config["mcpServers"]

    assert config_text == render_claude_desktop_config()
    assert set(mcp_servers) == {f"hc-{server_id}" for server_id in SERVER_BY_ID}
    for server_id in SERVER_BY_ID:
        entry = mcp_servers[f"hc-{server_id}"]
        assert entry["command"] == "hc-mcp"
        assert entry["args"] == [server_id]
        assert "HC_MCP_ENV_FILE" in entry["env"]


def test_http_client_config_matches_registry_ports() -> None:
    config_text = (REPO_ROOT / "configs" / "http-clients.json").read_text(encoding="utf-8")
    config = json.loads(config_text)
    mcp_servers = config["mcpServers"]

    assert config_text == render_http_clients_config()
    assert set(mcp_servers) == {f"hc-{server_id}" for server_id in SERVER_BY_ID}
    for server_id, spec in SERVER_BY_ID.items():
        assert mcp_servers[f"hc-{server_id}"]["url"] == f"http://localhost:{spec.port}/mcp"


def test_docker_compose_publishes_ports_on_localhost() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert compose == render_compose()
    for spec in SERVERS.values():
        assert f'"127.0.0.1:{spec.port}:{spec.port}"' in compose
        assert f'"{spec.port}:{spec.port}"' not in compose


def test_docker_compose_services_match_registry_modules_and_ports() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    services = _compose_service_blocks(compose)
    image_line = f'image: "{compose_image_reference()}"'

    assert set(services) == set(SERVER_BY_ID)
    for server_id, spec in SERVER_BY_ID.items():
        block = services[server_id]
        assert "build: ." in block
        assert image_line in block
        assert "pull_policy: missing" in block
        assert f"command: python -m {spec.module}" in block
        assert f'"127.0.0.1:{spec.port}:{spec.port}"' in block
        assert f"MCP_PORT={spec.port}" in block
        for env_key in spec.all_env_names:
            assert f"{env_key}=" in block


def test_live_gateway_compose_inherits_live_routed_server_env_keys() -> None:
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    live_gateway_block = _compose_service_blocks(compose)["live-gateway"]
    routed_env_names = {
        env_key.name
        for spec in SERVER_REGISTRY
        if spec.server_id != "live-gateway" and "live" in spec.gateway_exposure
        for env_key in (*spec.required_env, *spec.optional_env)
    }

    assert "SEC_USER_AGENT" in routed_env_names
    assert "SAM_GOV_API_KEY" in routed_env_names
    for env_name in routed_env_names:
        assert f"{env_name}=" in live_gateway_block


def test_zero_config_compose_matches_registry_zero_config_subset() -> None:
    compose = (REPO_ROOT / "docker-compose.zero-config.yml").read_text(encoding="utf-8")
    services = _compose_service_blocks(compose)
    expected = {spec.server_id for spec in SERVER_REGISTRY if spec.zero_config}

    assert compose == render_compose(zero_config_only=True)
    assert set(services) == expected
    for server_id in expected:
        spec = SERVER_BY_ID[server_id]
        block = services[server_id]
        assert f'image: "{compose_image_reference()}"' in block
        assert "pull_policy: missing" in block
        assert f"command: python -m {spec.module}" in block
        assert f'"127.0.0.1:{spec.port}:{spec.port}"' in block
        assert "MCP_HOST=0.0.0.0" in block


def test_register_codex_script_reads_canonical_registry() -> None:
    script = (REPO_ROOT / "scripts" / "register-codex.sh").read_text(encoding="utf-8")

    assert "from shared.utils.server_registry import SERVER_REGISTRY" in script
    assert "--dry-run" in script
    assert "Dry run: no Codex config changes will be made." in script
    assert "Use --dry-run to preview registry-backed registrations without Codex installed." in script
    assert "servers.service_area.server:8002" not in script
    assert 'ENV_FILE="${HC_MCP_ENV_FILE:-$PROJECT_DIR/.env}"' in script
    assert '--env "HC_MCP_ENV_FILE=$ENV_FILE"' in script
    assert 'servers._launcher "$server_id"' in script


def test_register_codex_dry_run_is_read_only_and_registry_backed() -> None:
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "register-codex.sh"), "--dry-run", "--http"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Dry run: no Codex config changes will be made." in result.stdout
    assert f"Registry entries: {len(SERVER_REGISTRY)}" in result.stdout
    assert "Mode: --http" in result.stdout
    assert "codex CLI not found" not in result.stdout
    for spec in SERVER_REGISTRY:
        assert f"would add: codex mcp add hc-{spec.server_id} --url http://localhost:{spec.port}/mcp" in result.stdout

    remove_result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "register-codex.sh"), "--dry-run", "--remove"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Dry run: no Codex config changes will be made." in remove_result.stdout
    assert "would remove: codex mcp remove hc-public-records" in remove_result.stdout


def test_installer_server_arrays_match_registry() -> None:
    install = (REPO_ROOT / "install.sh").read_text(encoding="utf-8")

    assert "from shared.utils.server_registry import SERVER_REGISTRY" in install
    assert "load_server_registry" in install
    assert "load_zero_config_compose_registry" in install
    assert "env_servers = defaultdict(list)" in install
    assert "Registry-defined environment keys enable optional and key-required tools" in install
    assert "prompt_registry_keys" in install
    assert '--env "HC_MCP_ENV_FILE=$ENV_FILE"' in install
    assert "declare -A SERVER_IDS=()" in install
    assert 'print(f"SERVER\\thc-{spec.server_id}\\t{spec.server_id}\\t{spec.module}\\t{spec.port}\\t{int(spec.zero_config)}")' in install
    assert 'server_id="${SERVER_IDS[$name]:-${name#hc-}}"' in install
    assert 'servers._launcher "$server_id"' in install
    assert "https://api.census.gov/data/key_signup.html" not in install
    assert "Google Custom Search API (web intelligence)" not in install
    assert "Servers with key-enhanced tools" in install
    assert "Run hc-mcp doctor for registry-backed environment guidance" in install
    for spec in SERVER_BY_ID.values():
        assert f'[{spec.server_id}]="{spec.module}"' not in install
        assert f"[hc-{spec.server_id}]={spec.port}" not in install


def test_installer_dry_run_is_read_only_and_registry_backed() -> None:
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "install.sh"), "--dry-run", "--no-register"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = result.stdout
    assert "Dry run completed without cloning, installing, writing config, or registering clients" in stdout
    assert f"Server registry entries: {len(SERVER_REGISTRY)}" in stdout
    registry_env_names = {
        key.name
        for spec in SERVER_REGISTRY
        for key in (*spec.required_env, *spec.optional_env)
    }
    assert f"Registry environment keys: {len(registry_env_names)}" in stdout
    assert "MCP_LIVE_GATEWAY_AUDIT_LOG_PATH" in stdout
    for spec in SERVER_REGISTRY:
        if spec.zero_config:
            assert f"hc-{spec.server_id}" in stdout


def test_client_packaging_prefers_release_install_before_editable_install() -> None:
    docs = (REPO_ROOT / "docs" / "CLIENT_PACKAGING.md").read_text(encoding="utf-8")

    assert docs.index("pipx install healthcare-data-mcp") < docs.index("python3 -m pip install -e .")
    assert docs.index("hc-mcp doctor") < docs.index("hc-mcp --list")
    assert "pipx install git+https://github.com/ajhcs/healthcare-data-mcp@<tag>" in docs


def test_installer_dry_run_loads_registry_from_script_dir(tmp_path: Path) -> None:
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "install.sh"), "--dry-run", "--no-register"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    stdout = _strip_ansi(result.stdout)
    registry_env_names = sorted({key.name for spec in SERVER_REGISTRY for key in (*spec.required_env, *spec.optional_env)})
    zero_config_server_names = [f"hc-{spec.server_id}" for spec in SERVER_REGISTRY if spec.zero_config]

    assert f"Server registry entries: {len(SERVER_BY_ID)}" in stdout
    assert f"Registry environment keys: {len(registry_env_names)}" in stdout
    assert f"Zero-config servers: {', '.join(zero_config_server_names)}" in stdout
    assert f"Registry environment key names: {', '.join(registry_env_names)}" in stdout
    assert "without cloning, installing, writing config, or registering clients" in stdout


def test_installer_help_and_unknown_args_are_read_only() -> None:
    help_result = subprocess.run(
        ["bash", str(REPO_ROOT / "install.sh"), "--help"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    typo_result = subprocess.run(
        ["bash", str(REPO_ROOT / "install.sh"), "--dryrun"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert "Healthcare Data MCP" in help_result.stdout
    assert "--dry-run" in help_result.stdout
    assert "Checking prerequisites" not in help_result.stdout
    assert typo_result.returncode == 2
    assert "Unknown installer option: --dryrun" in typo_result.stderr
    assert "Checking prerequisites" not in typo_result.stdout


def test_registration_shell_scripts_are_parseable() -> None:
    for script_name in ("install.sh", "scripts/register-codex.sh", "scripts/setup.sh", "scripts/mcp_inspector_smoke.sh"):
        subprocess.run(
            ["bash", "-n", str(REPO_ROOT / script_name)],
            cwd=REPO_ROOT,
            check=True,
        )


def test_legacy_setup_script_is_read_only_wrapper() -> None:
    script = (REPO_ROOT / "scripts" / "setup.sh").read_text(encoding="utf-8")
    help_result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "setup.sh"), "--help"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "intentionally read-only when run without arguments" in help_result.stdout
    assert "hc-mcp doctor" in script
    assert "hc-mcp-setup" in script
    assert "report_server" not in script
    assert "prompt_key" not in script
    assert "18 servers" not in script


def test_ci_runs_mcp_inspector_smoke() -> None:
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    script = REPO_ROOT / "scripts" / "mcp_inspector_smoke.sh"
    script_text = script.read_text(encoding="utf-8")

    assert "actions/setup-node@v4" in ci
    assert "bash -n install.sh scripts/register-codex.sh scripts/setup.sh scripts/mcp_inspector_smoke.sh" in ci
    assert "scripts/mcp_inspector_smoke.sh" in ci
    assert "--expect-structured-path-all workflows[].identity_join_keys" in ci
    assert "--expect-structured-path-all workflows[].source_resolution" in ci
    assert "mcp_smoke.py --server discovery --expect-tool get_workflow_plan --call-tool get_workflow_plan" in ci
    assert "--expect-structured-key report_ingest_contract" in ci
    assert "--expect-structured-path identity_map.join_keys" in ci
    assert '"workflow_id":"system_reconciliation"' in ci
    assert '"query":"Jefferson Health"' in ci
    assert '"system_slug":"jefferson-health"' in ci
    assert "--expect-structured-path-all identity_map.resolution_plan[].qualified_tool" in ci
    assert "--expect-structured-path-all identity_map.resolution_plan[].merge_action" in ci
    assert "--expect-structured-path-all steps[].identity_contract" in ci
    assert "--expect-structured-path-all steps[].source_resolution" in ci
    assert "--expect-structured-path-all report_ingest_contract.fact_rows[].evidence_path" in ci
    assert "--expect-structured-path-all report_ingest_contract.fact_rows[].source_metadata_path" in ci
    assert "--expect-structured-path-all report_ingest_contract.fact_rows[].identity_path" in ci
    assert "--expect-structured-path-all report_ingest_contract.fact_rows[].identity_map_path" in ci
    assert "mcp_smoke.py --server discovery --expect-tool get_preset_plan --call-tool get_preset_plan" in ci
    assert "--expect-structured-key workflow_summaries" in ci
    assert "--expect-structured-path-all workflow_summaries[].identity_join_keys" in ci
    assert "--expect-structured-path-all workflow_summaries[].source_resolution" in ci
    assert "mcp_smoke.py --server live-gateway --expect-tool list_live_tools --call-tool list_live_tools" in ci
    assert "--expect-structured-path-all tools[].allowed_scopes" in ci
    assert "--expect-structured-path-all tools[].request_size_limit_bytes" in ci
    assert "--expect-structured-path-all tools[].result_size_limit_bytes" in ci
    assert "--expect-structured-path-all tools[].rate_limit_class" in ci
    assert "--expect-structured-path-all tools[].source_caveat_class" in ci
    assert "--expect-structured-path-all tools[].requires_provenance" in ci
    assert os.access(script, os.X_OK)
    assert "@modelcontextprotocol/inspector" in script_text
    assert "--method tools/call" in script_text
    assert "servers._launcher live-gateway" in script_text
    assert "--tool-name list_live_tools" in script_text


def test_ci_runs_doctor_in_read_only_cli_readiness_smoke() -> None:
    ci = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "Read-only CLI readiness smoke" in ci
    assert "hc-mcp --version" in ci
    assert "hc-mcp --list" in ci
    assert "hc-mcp doctor --check --json" in ci
    assert "hc-mcp workflow quality_measure_lookup --json" in ci
    assert "hc-mcp preset metadata-only --json" in ci


def test_env_example_has_registry_keys_once() -> None:
    env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    assigned = re.findall(r"^([A-Z][A-Z0-9_]+)=", env_example, flags=re.MULTILINE)

    assert env_example == render_env_example()
    assert len(assigned) == len(set(assigned)), "duplicate .env.example assignments"
    assert set(assigned) == expected_env_names()
    for spec in SERVER_REGISTRY:
        for env_key in spec.all_env_names:
            assert env_key in assigned


def _compose_service_blocks(compose: str) -> dict[str, str]:
    services_section = compose.split("\nvolumes:", 1)[0]
    matches = list(re.finditer(r"^  ([a-z0-9-]+):\n", services_section, flags=re.MULTILINE))
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(services_section)
        blocks[match.group(1)] = services_section[start:end]
    return blocks


def _strip_ansi(value: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", value)
