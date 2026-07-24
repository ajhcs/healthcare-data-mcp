# Codex Form 990 plugin integration

Research date: 2026-07-24

## Decision

Package the existing read-only Form 990 MCP server with one focused skill as a
local `form-990-facts` plugin. Connect the installed plugin to the persistent
Plumbob service over streamable HTTP at its explicit private-LAN address. Expose
exactly one user-facing tool:

```text
get_form_990_facts(ein: string, tax_year: integer)
```

This is the smallest surface that answers the supported question. Do not add a
registry/list/search tool, a brand resolver, a shell-command workflow,
programmatic tool calling, subagents, or UI. A single direct call returns a small
structured result and preserves provenance, which matches GPT-5.6 guidance to
use lean prompts, expose only relevant tools, and prefer direct calls when one
call is sufficient.

## Why this shape

- A plugin may bundle skills, an MCP server, or both; local marketplaces are the
  supported test/install path before any public submission. The current Codex
  guidance says to build and test the MCP server first, install the complete
  plugin, and test it in a new conversation.
- MCP tool names, descriptions, schemas, and annotations are user-facing routing
  behavior. The official guidance calls for one focused operation per distinct
  goal, explicit input/output schemas, accurate safety annotations, and useful
  `structuredContent`.
- A lookup does not need custom UI. The model can answer directly from the
  structured facts.
- GPT-5.6 guidance favors removing repeated instructions and extra tools.
  Programmatic tool calling is not appropriate because this workflow needs one
  small, direct call and must preserve source evidence.
- Multi-agent orchestration is unnecessary at query time. There is no parallel
  workstream to coordinate after ingestion.

## MCP contract

Keep `get_form_990_facts` strict:

- Require a normalized nine-digit EIN and four-digit tax year.
- Return a stable status (`found` or `not_found`) rather than silently falling
  back to web research.
- On `found`, return legal filer, EIN, form type, tax period, every requested
  metric with reported label, metric key, exact value and units, XML field path,
  and filing provenance: official source URL, XML member/source identifier,
  SHA-256, submission/object ID, and DLN/return ID when present.
- Represent an absent XML field as missing, never as numeric zero.
- On `not_found`, say only that the exact EIN/year is not loaded. Do not imply
  that no filing exists.
- Return the same compact object as `structuredContent`; keep text content to a
  short status/summary rather than duplicating the full payload.

Advertise:

```json
{
  "readOnlyHint": true,
  "destructiveHint": false,
  "openWorldHint": false,
  "idempotentHint": true
}
```

`openWorldHint` is false because warm queries read the local, already-ingested
store and do not contact IRS or another external system. The server must enforce
validation itself; annotations inform client behavior but are not an
authorization control.

Server-wide instructions are optional here because there is only one tool and
no cross-tool sequence. If supplied, keep the first 512 characters
self-contained and state only the exact-EIN/year and read-only boundary; do not
repeat the tool description or skill.

## Skill contract

Create one compact skill, also named `form-990-facts`.

Its description should trigger for exact Form 990 fact questions that include a
known EIN and tax year. Its body should:

1. Require the exact legal-filer EIN and tax year. For a brand-only or incomplete
   request, ask for those identifiers and state that brand resolution is outside
   scope.
2. Call `get_form_990_facts` once.
3. Use only returned values. Name the legal filer and reporting period; report
   total revenue, total expenses, net assets, and the top reported executive
   with total reported compensation when present.
4. Preserve provenance in the answer, including the official source URL,
   submission/object ID or equivalent, XML hash, and the XML paths associated
   with reported facts.
5. Distinguish missing/not reported from zero.
6. On `not_found`, say the curated store has no loaded record for that exact
   EIN/year. Do not resolve a brand, broaden to national research, infer another
   filer, or claim the IRS has no filing.
7. State when requests for brand resolution, non-990 forms, national discovery,
   or comparison products exceed the capability.

Do not place the 16-filer corpus, extraction mappings, or provenance schema
examples in `SKILL.md`. The tool schema and returned object carry those details.
Declare the MCP dependency in `agents/openai.yaml`, use a short UI description,
and make the default prompt explicitly mention `$form-990-facts`.

## Transport and LAN boundary

Codex supports both STDIO and streamable HTTP MCP servers. For this prototype,
use plugin-bundled streamable HTTP pointed at the stable Plumbob private-LAN URL:

- It exercises the same LAN query boundary already built and avoids duplicating
  retrieval through an optional shell/CLI sequence.
- It decouples the installed plugin from project working-directory and Python
  interpreter assumptions that a STDIO command would otherwise have to encode.
- The service must bind only to the selected RFC1918 interface, never
  `0.0.0.0`, `::`, or a public address. Check the active listener before and
  after installation.
- Keep the plugin tool allow-list to `get_form_990_facts`, mark the MCP server
  required for plugin startup, and use automatic approval only for this
  accurately annotated read-only tool.

This is a local/private installation, not a public plugin submission. The
public-HTTPS deployment requirements in the general plugin builder guide do not
apply; do not open a firewall, change DNS, create a tunnel, or publish the
endpoint. Plain LAN HTTP is acceptable for this public IRS-derived, read-only
prototype only within the stated trusted-LAN boundary; add authentication/TLS
before expanding that boundary or serving sensitive data.

## Package and installation

Keep the canonical, committed source in
`plugins/form-990-facts/`:

```text
plugins/form-990-facts/
├── .codex-plugin/plugin.json
├── .mcp.json
└── skills/form-990-facts/
    ├── SKILL.md
    └── agents/openai.yaml
```

Use the installed `plugin-creator` scaffold and validator rather than hand-made
manifest/marketplace structures. Use `skill-creator`'s `init_skill.py` and
`quick_validate.py` for the skill. After project tests and plugin validation
pass, copy the exact validated package to the Plumbob user's personal plugin
source (`~/plugins/form-990-facts`), create/update the default personal
marketplace through the scaffold helper, and install with:

```text
codex plugin add form-990-facts@<personal-marketplace-name>
```

Treat that personal installation as host-global for the Plumbob Codex user; do
not imply a machine-wide install for unrelated Unix users. A newly installed or
reinstalled plugin is picked up in a new Codex conversation. For later local
updates, use the plugin-creator cachebuster helper and reinstall flow rather than
editing marketplace configuration by hand.

## Validation matrix

Before installation:

- Contract tests: valid exact lookup, invalid EIN/year, missing filing, missing
  metric, provenance completeness, stable structured result.
- Metadata tests: the only advertised tool has the expected schemas and all
  read-only/idempotent annotations.
- Network tests: wildcard/public binds are refused; selected private address is
  accepted; no unexpected listener remains after tests.
- MCP tests: initialize, list tools, call representative/invalid/not-found
  inputs, and inspect schemas, errors, annotations, and structured content.
- Skill tests: direct trigger, indirect exact-EIN/year wording, incomplete input,
  a request that must not trigger, not-found data, and a brand/comparison request
  that must not invent scope.
- Package tests: `quick_validate.py`, `validate_plugin.py`, and a clean plugin
  install from the personal marketplace.

After installation, use a new sealed conversation for end-to-end tests. Compare
plugin-enabled and plugin-disabled/native research with matched GPT-5.6 model,
reasoning, prompts, and exact filer/year cases. Keep gold answers outside both
prompts; score accuracy and provenance after collection, and record latency,
tool/web calls, retrieved bytes, and tokens where observable.

## Primary sources

- [GPT-5.6 model guidance](https://developers.openai.com/api/docs/guides/model-guidance?model=gpt-5.6)
- [Build plugins in Codex](https://learn.chatgpt.com/docs/build-plugins)
- [Codex MCP configuration and plugin-provided servers](https://learn.chatgpt.com/docs/extend/mcp)
- [Build an MCP server for a plugin](https://developers.openai.com/plugins/build/mcp-server)
- [Build plugin skills](https://developers.openai.com/plugins/build/skills)
- [Package a plugin](https://developers.openai.com/plugins/build/plugins)

Local authoring guidance consulted:

- `/home/plumbob/.codex/skills/.system/plugin-creator/SKILL.md`
- `/home/plumbob/.codex/skills/.system/plugin-creator/references/plugin-json-spec.md`
- `/home/plumbob/.codex/skills/.system/plugin-creator/references/installing-and-updating.md`
- `/home/plumbob/.codex/skills/.system/skill-creator/SKILL.md`
- `/home/plumbob/.codex/skills/.system/skill-creator/references/openai_yaml.md`
