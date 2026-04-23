from __future__ import annotations

import json
import tomllib
from pathlib import Path

from scripts.build_mcpb import SERVER_NAMES
from servers._launcher import SERVERS


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_desktop_extension_manifest_mentions_all_current_servers() -> None:
    manifest = json.loads((REPO_ROOT / "desktop-extension" / "manifest.json").read_text(encoding="utf-8"))
    description = manifest["user_config"]["server_name"]["description"]

    for server_name in SERVERS:
        assert server_name in description


def test_mcpb_builder_accepts_all_launcher_servers() -> None:
    assert set(SERVERS) <= SERVER_NAMES


def test_codex_example_includes_new_servers_and_http_entries() -> None:
    config = tomllib.loads((REPO_ROOT / "examples" / "codex-config.toml").read_text(encoding="utf-8"))
    mcp_servers = config["mcp_servers"]

    assert mcp_servers["providerEnrollment"]["args"] == ["provider-enrollment"]
    assert mcp_servers["communityHealth"]["args"] == ["community-health"]
    assert mcp_servers["researchTrials"]["args"] == ["research-trials"]
    assert mcp_servers["providerEnrollmentHttp"]["url"].endswith(":8017/mcp")
    assert mcp_servers["communityHealthHttp"]["url"].endswith(":8018/mcp")
    assert mcp_servers["researchTrialsHttp"]["url"].endswith(":8019/mcp")


def test_claude_desktop_stdio_example_is_valid_json_and_includes_env_pointer() -> None:
    config = json.loads((REPO_ROOT / "examples" / "claude-desktop-stdio.json").read_text(encoding="utf-8"))
    mcp_servers = config["mcpServers"]

    assert mcp_servers["provider-enrollment"]["args"] == ["provider-enrollment"]
    assert mcp_servers["community-health"]["args"] == ["community-health"]
    assert mcp_servers["research-trials"]["args"] == ["research-trials"]
    assert "HC_MCP_ENV_FILE" in mcp_servers["public-records"]["env"]
