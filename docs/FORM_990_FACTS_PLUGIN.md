# Plumbob Form 990 facts plugin

This private Codex plugin exposes one read-only MCP operation for an exact,
pre-ingested legal-filer EIN and tax-period year. It has no ingestion tool,
brand resolver, national registry, comparison workflow, UI, or public route.

## Package

The committed source is `plugins/form-990-facts/`:

- `.codex-plugin/plugin.json` describes the plugin.
- `.mcp.json` connects only to
  `http://192.168.1.60:8022/mcp` and allow-lists
  `get_form_990_facts`.
- `skills/form-990-facts/SKILL.md` contains the compact query and scope
  instructions.
- `skills/form-990-facts/agents/openai.yaml` declares the same MCP dependency.

The skill requires a known nine-digit legal-filer EIN and tax-period end year.
It must not resolve a brand, infer affiliates, substitute another filer, or
fall back to shell/web research when the curated store has no matching record.

## Private runtime

Before starting the service, verify port 8022 is unused:

```bash
ss -tlnp
```

Install the reviewed user unit:

```bash
install -Dm0644 ops/systemd/form-990-facts.service \
  ~/.config/systemd/user/form-990-facts.service
systemctl --user daemon-reload
systemctl --user enable --now form-990-facts.service
```

Verify the bind and health from Plumbob:

```bash
systemctl --user status form-990-facts.service --no-pager
ss -tlnp | rg ':8022'
curl -fsS http://192.168.1.60:8022/mcp
```

The last request may return an MCP protocol error without an initialized
session; that still proves the private listener is reachable. The server rejects
wildcard, public, and hostname bind values. Do not add Caddy, public DNS,
Cloudflare, tunnels, or external firewall rules.

## Validation and installation

Validate the source package before copying it to the Plumbob user's personal
plugin marketplace:

```bash
python3 /home/plumbob/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  plugins/form-990-facts/skills/form-990-facts
python3 /home/plumbob/.codex/skills/.system/plugin-creator/scripts/validate_plugin.py \
  plugins/form-990-facts
pytest -q tests/plugins tests/servers/form_990_facts
```

The personal installation is host-global for the `plumbob` Codex user, not for
unrelated Unix users. Copy the exact validated package to
`/home/plumbob/plugins/form-990-facts`, update the personal marketplace with the
official plugin-creator helper, install `form-990-facts@personal`, and start a
new Codex task so plugin metadata is reloaded.

## Operator updates

Users never ingest filings. Operators update
`configs/irs990-prototype-filers.csv` and run the official XML ingestion command
documented in `docs/FORM_990_FACTS_PROTOTYPE.md`. Re-run package, focused, full,
LAN, and sealed benchmark validation before updating the installed plugin.
