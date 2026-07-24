from __future__ import annotations

import json
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
PLUGIN_ROOT = REPO_ROOT / "plugins" / "form-990-facts"
SKILL_ROOT = PLUGIN_ROOT / "skills" / "form-990-facts"


def test_plugin_exposes_only_the_lan_read_only_query_tool() -> None:
    config = json.loads((PLUGIN_ROOT / ".mcp.json").read_text(encoding="utf-8"))

    assert list(config["mcpServers"]) == ["form-990-facts"]
    server = config["mcpServers"]["form-990-facts"]
    assert server["url"] == "http://192.168.1.60:8022/mcp"
    assert server["required"] is True
    assert server["enabled_tools"] == ["get_form_990_facts"]
    assert server["default_tools_approval_mode"] == "auto"


def test_skill_declares_the_same_mcp_dependency_and_stays_compact() -> None:
    metadata = yaml.safe_load((SKILL_ROOT / "agents" / "openai.yaml").read_text(encoding="utf-8"))
    dependency = metadata["dependencies"]["tools"]
    skill_text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")

    assert dependency == [
        {
            "type": "mcp",
            "value": "form-990-facts",
            "description": "LAN-only read-only exact Form 990 fact store",
            "transport": "streamable_http",
            "url": "http://192.168.1.60:8022/mcp",
        }
    ]
    assert metadata["policy"]["allow_implicit_invocation"] is True
    assert len(skill_text.split()) < 400
    assert "not_found" in skill_text
    assert "Do not claim that the IRS has no filing" in skill_text


def test_skill_does_not_embed_the_curated_filer_registry() -> None:
    skill_text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")

    for ein in ("741180155", "232829095", "010238552", "362174823"):
        assert ein not in skill_text
