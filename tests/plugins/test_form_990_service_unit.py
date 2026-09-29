from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
UNIT = REPO_ROOT / "ops" / "systemd" / "form-990-facts.service"


def test_operator_unit_binds_only_the_plumbob_private_address() -> None:
    unit = UNIT.read_text(encoding="utf-8")

    assert "Environment=MCP_HOST=192.168.1.60" in unit
    assert "Environment=MCP_PORT=8022" in unit
    assert "Environment=MCP_TRANSPORT=streamable-http" in unit
    assert "0.0.0.0" not in unit
    assert "MCP_HOST=::" not in unit


def test_operator_unit_can_write_only_the_local_fact_store_area() -> None:
    unit = UNIT.read_text(encoding="utf-8")

    assert "NoNewPrivileges=true" in unit
    assert "ProtectSystem=strict" in unit
    assert 'ReadWritePaths="/mnt/d/Coding Projects/healthcare-data-mcp/.local"' in unit
