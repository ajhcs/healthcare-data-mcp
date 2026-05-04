"""Interactive setup wizard for healthcare-data-mcp environment values."""

from __future__ import annotations

import argparse
import asyncio
import getpass
import hashlib
import json
import re
import secrets
import shutil
from dataclasses import dataclass
from pathlib import Path

from shared.utils.env_file import read_env_file, write_env_file
from shared.state_health_data import acquire_sources


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV = Path.cwd() / ".env"
DEFAULT_TEMPLATE = REPO_ROOT / ".env.example"
DEFAULT_CACHE_ROOT = Path.home() / ".healthcare-data-mcp" / "cache"


@dataclass(frozen=True)
class ConfigKey:
    name: str
    prompt: str
    required: bool = False
    secret: bool = True
    help_text: str = ""


@dataclass(frozen=True)
class ManualCacheItem:
    name: str
    seed_path: Path
    parquet_path: Path
    tools: tuple[str, ...]
    source_url: str
    import_flag: str
    instructions: str
    automation_note: str
    agent_prompt: str
    acquire_flag: str = ""
    unavailable_reason: str = ""


@dataclass(frozen=True)
class PublicCacheItem:
    name: str
    cache_paths: tuple[Path, ...]
    tools: tuple[str, ...]
    source_url: str
    acquire_flag: str
    instructions: str


CONFIG_KEYS: tuple[ConfigKey, ...] = (
    ConfigKey(
        "SAM_GOV_API_KEY",
        "SAM.gov API key for Exclusions and opportunities",
        help_text="Required for SAM.gov API-backed tools.",
    ),
    ConfigKey("CHPL_API_KEY", "ONC CHPL API key for EHR enrichment", help_text="Optional public-records enrichment."),
    ConfigKey(
        "SEC_USER_AGENT",
        "SEC EDGAR User-Agent",
        required=True,
        secret=False,
        help_text='Required format: "AppName email@example.com".',
    ),
    ConfigKey("CENSUS_API_KEY", "Census API key", help_text="Optional geo-demographics rate-limit improvement."),
    ConfigKey("HUD_API_TOKEN", "HUD USPS Crosswalk token", help_text="Optional ZIP crosswalk support."),
    ConfigKey("ORS_API_KEY", "OpenRouteService API key", help_text="Optional drive-time isochrones."),
    ConfigKey("BLS_API_KEY", "BLS API key", help_text="Optional workforce analytics rate-limit improvement."),
    ConfigKey("GOOGLE_CSE_API_KEY", "Google Custom Search API key", help_text="Optional web-intelligence search."),
    ConfigKey(
        "GOOGLE_CSE_ID", "Google Custom Search Engine ID", secret=False, help_text="Used with GOOGLE_CSE_API_KEY."
    ),
    ConfigKey("PROXYCURL_API_KEY", "Proxycurl API key", help_text="Optional web-intelligence enrichment."),
)

MANUAL_CACHE_ITEMS: tuple[ManualCacheItem, ...] = (
    ManualCacheItem(
        name="HIPAA breach reports",
        seed_path=Path("public-records") / "hipaa_breaches.csv",
        parquet_path=Path("public-records") / "hipaa_breaches.parquet",
        tools=("public_records.get_breach_history",),
        source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
        import_flag="--import-breach-csv",
        instructions="Run hc-mcp-setup --acquire-hipaa-breaches to fetch the public OCR breach table.",
        automation_note="The OCR breach report is published as a public HTML table and can be acquired by the CLI.",
        agent_prompt=(
            "Run hc-mcp-setup --acquire-hipaa-breaches to fetch the public OCR breach table."
        ),
        acquire_flag="--acquire-hipaa-breaches",
    ),
    ManualCacheItem(
        name="DocGraph shared patients",
        seed_path=Path("docgraph") / "shared_patients.csv",
        parquet_path=Path("docgraph") / "shared_patients.parquet",
        tools=(
            "physician_referral_network.map_referral_network",
            "physician_referral_network.detect_leakage",
        ),
        source_url="https://careset.com/data-leadership/",
        import_flag="--import-docgraph-csv",
        instructions="Provide a CareSet/DocGraph shared-patients CSV or an already-converted Parquet file.",
        automation_note="DocGraph/CareSet data is large and separately licensed/distributed, so it is not bundled.",
        agent_prompt=(
            "Locate the local CareSet/DocGraph shared-patients CSV or Parquet file. If it is CSV, "
            "run hc-mcp-setup --import-docgraph-csv. If it is Parquet, run --import-docgraph-parquet."
        ),
        unavailable_reason="licensed_source_missing",
    ),
)


PUBLIC_CACHE_ITEMS: tuple[PublicCacheItem, ...] = (
    PublicCacheItem(
        name="PHC4 public reports",
        cache_paths=(
            Path("state-health-data") / "phc4" / "report_index.json",
            Path("state-health-data") / "phc4" / "report_index.parquet",
            Path("state-health-data") / "phc4" / "tables",
        ),
        tools=(
            "public_records.search_phc4_public_reports",
            "public_records.get_phc4_hospital_performance",
            "public_records.get_phc4_financial_analysis",
            "public_records.get_phc4_common_procedure_profile",
        ),
        source_url="https://www.phc4.org/reports-library/",
        acquire_flag="--acquire-phc4-public-reports",
        instructions="Index and cache public PHC4 report artifacts and extract supported tables.",
    ),
    PublicCacheItem(
        name="AHRQ HFMD",
        cache_paths=(
            Path("state-health-data") / "ahrq-hfmd" / "hfmd.csv",
            Path("state-health-data") / "ahrq-hfmd" / "hfmd.zip",
            Path("state-health-data") / "ahrq-hfmd" / "csv",
        ),
        tools=("financial_intelligence.get_public_financial_health_profile",),
        source_url="https://www.ahrq.gov/data/innovations/hfmd.html",
        acquire_flag="--acquire-ahrq-hfmd",
        instructions="Acquire the AHRQ Hospital Financial Measures Database CSV ZIP and normalized files.",
    ),
    PublicCacheItem(
        name="PA hospital reports",
        cache_paths=(
            Path("state-health-data") / "pa-hospital-reports" / "artifact_index.json",
            Path("state-health-data") / "pa-hospital-reports" / "artifact_metadata.csv",
        ),
        tools=("workforce_analytics.get_public_throughput_profile",),
        source_url="https://www.pa.gov/agencies/health/health-statistics/health-facilities/hospital-reports.html",
        acquire_flag="--acquire-pa-hospital-reports",
        instructions="Index Pennsylvania DOH public hospital report artifacts and normalized metadata.",
    ),
    PublicCacheItem(
        name="NJ hospital public data",
        cache_paths=(
            Path("state-health-data") / "nj-hospital-public-data" / "artifact_index.json",
            Path("state-health-data") / "nj-hospital-public-data" / "artifact_metadata.csv",
        ),
        tools=("financial_intelligence.get_public_financial_health_profile",),
        source_url="https://www.nj.gov/health/hcf/financial-reports/",
        acquire_flag="--acquire-nj-hospital-public-data",
        instructions="Index New Jersey financial, charity-care, and public hospital artifacts.",
    ),
    PublicCacheItem(
        name="DE hospital discharge public data",
        cache_paths=(
            Path("state-health-data") / "de-hospital-discharge" / "artifact_index.json",
            Path("state-health-data") / "de-hospital-discharge" / "artifact_metadata.csv",
        ),
        tools=("workforce_analytics.get_public_throughput_profile",),
        source_url="https://dhss.delaware.gov/dph/hp/hosp_dis_data/",
        acquire_flag="--acquire-de-hospital-discharge",
        instructions="Index Delaware hospital discharge public artifacts and normalized metadata.",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="hc-mcp-setup",
        description="Create, update, and validate healthcare-data-mcp .env configuration.",
    )
    parser.add_argument(
        "--env-file", type=Path, default=DEFAULT_ENV, help="Path to write/read. Default: .env in the repo."
    )
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE, help="Template dotenv file.")
    parser.add_argument(
        "--set", action="append", default=[], metavar="KEY=VALUE", help="Set a value non-interactively."
    )
    parser.add_argument("--interactive", action="store_true", help="Prompt for missing or changed values.")
    parser.add_argument(
        "--skip-optional", action="store_true", help="Only prompt for required values in interactive mode."
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Validate the selected env file without writing changes."
    )
    parser.add_argument(
        "--generate-gateway-token",
        action="store_true",
        help="Generate a gateway bearer token and store only its SHA-256 hash.",
    )
    parser.add_argument(
        "--print-client-snippets", action="store_true", help="Print install snippets for common MCP clients."
    )
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT, help="Data cache root.")
    parser.add_argument(
        "--cache-status", action="store_true", help="Print cache status and affected tools."
    )
    parser.add_argument(
        "--cache-guide",
        action="store_true",
        help="Print source-by-source acquisition and import guidance for cache datasets.",
    )
    parser.add_argument(
        "--agent-cache-instructions",
        action="store_true",
        help="Print concise agent instructions for supported cache acquisition/import tasks.",
    )
    parser.add_argument(
        "--acquire-public-caches",
        action="store_true",
        help="Fetch public cache datasets that expose stable unauthenticated acquisition paths.",
    )
    parser.add_argument(
        "--acquire-hipaa-breaches",
        action="store_true",
        help="Fetch the public HHS OCR HIPAA breach table and store it as the breach CSV seed.",
    )
    parser.add_argument("--acquire-pa-hospital-reports", action="store_true", help="Index public Pennsylvania hospital report artifacts.")
    parser.add_argument("--acquire-nj-hospital-public-data", action="store_true", help="Index public New Jersey hospital financial and charity-care artifacts.")
    parser.add_argument("--acquire-de-hospital-discharge", action="store_true", help="Index Delaware hospital discharge public data artifacts.")
    parser.add_argument("--acquire-phc4-public-reports", action="store_true", help="Index and cache PHC4 public report library artifacts.")
    parser.add_argument("--acquire-ahrq-hfmd", action="store_true", help="Acquire the AHRQ Hospital Financial Measures Database CSV ZIP.")
    parser.add_argument(
        "--acquire-provider-enrollment",
        action="store_true",
        help="Fetch CMS PECOS provider-enrollment, owners, CHOW, and owner-info datasets into Parquet caches.",
    )
    parser.add_argument(
        "--force-cache-refresh",
        action="store_true",
        help="Overwrite existing acquired public cache files.",
    )
    parser.add_argument(
        "--import-breach-csv", type=Path, help="Copy a HHS OCR breach CSV export into the public-records cache."
    )
    parser.add_argument(
        "--import-docgraph-csv",
        type=Path,
        help="Convert a DocGraph shared-patients CSV into the DocGraph Parquet cache.",
    )
    parser.add_argument(
        "--import-docgraph-parquet", type=Path, help="Copy an existing DocGraph shared-patients Parquet cache."
    )
    parser.add_argument(
        "--import-state-breach-notices",
        nargs=2,
        metavar=("STATE", "PATH"),
        help="Import a reviewed state breach notice CSV into the public-records cache.",
    )
    parser.add_argument(
        "--state-breach-source-url",
        default="",
        help="Source URL to apply to imported state breach notice rows that lack source_url.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    current = read_env_file(args.env_file)
    updates = dict(current)
    updates.update(_parse_set_values(args.set))

    if args.generate_gateway_token:
        token = secrets.token_urlsafe(32)
        updates["MCP_GATEWAY_AUTH_REQUIRED"] = "true"
        updates["MCP_GATEWAY_BEARER_TOKEN_SHA256"] = hashlib.sha256(token.encode()).hexdigest()
        print("Generated gateway bearer token. Store this now; only its SHA-256 hash is written to .env:")
        print(token)

    if args.interactive:
        updates.update(prompt_for_values(updates, required_only=args.skip_optional))

    errors = validate_env(updates)
    if errors:
        print("Configuration warnings/errors:")
        for error in errors:
            print(f"- {error}")

    if not args.validate_only:
        write_env_file(args.env_file, updates, template_path=args.template)
        print(f"Wrote {args.env_file}")

    if args.print_client_snippets:
        print_client_snippets(args.env_file)

    acquisition_results = acquire_public_caches(
        cache_root=args.cache_root,
        hipaa_breaches=args.acquire_public_caches or args.acquire_hipaa_breaches,
        provider_enrollment=args.acquire_public_caches or args.acquire_provider_enrollment,
        force=args.force_cache_refresh,
    )
    state_sources: list[str] = []
    if args.acquire_public_caches or args.acquire_pa_hospital_reports:
        state_sources.append("pa_hospital_reports")
    if args.acquire_public_caches or args.acquire_nj_hospital_public_data:
        state_sources.append("nj_hospital_public_data")
    if args.acquire_public_caches or args.acquire_de_hospital_discharge:
        state_sources.append("de_hospital_discharge")
    if args.acquire_public_caches or args.acquire_phc4_public_reports:
        state_sources.append("phc4_public_reports")
    if args.acquire_public_caches or args.acquire_ahrq_hfmd:
        state_sources.append("ahrq_hfmd")
    if state_sources:
        for status in asyncio.run(acquire_sources(state_sources, args.cache_root, force=args.force_cache_refresh)):
            acquisition_results.append(
                f"{status['source_id']}: {status['status']} -> {status.get('cache_path') or status.get('source_url')}"
                + (f" ({status.get('reason')})" if status.get("reason") else "")
            )
    for result in acquisition_results:
        print(result)

    import_results = import_manual_caches(
        cache_root=args.cache_root,
        breach_csv=args.import_breach_csv,
        docgraph_csv=args.import_docgraph_csv,
        docgraph_parquet=args.import_docgraph_parquet,
        state_breach_notices=args.import_state_breach_notices,
        state_breach_source_url=args.state_breach_source_url,
    )
    for result in import_results:
        print(result)

    if args.cache_status or import_results or acquisition_results:
        print_cache_status(args.cache_root)

    if args.cache_guide:
        print_cache_guide(args.cache_root)

    if args.agent_cache_instructions:
        print_agent_cache_instructions(args.cache_root)


def prompt_for_values(current: dict[str, str], *, required_only: bool = False) -> dict[str, str]:
    """Prompt for config values. Existing values are kept on blank input."""
    updates: dict[str, str] = {}
    for item in CONFIG_KEYS:
        if required_only and not item.required:
            continue

        existing = current.get(item.name, "")
        required_marker = " required" if item.required else " optional"
        print(f"\n{item.name} ({required_marker})")
        if item.help_text:
            print(item.help_text)
        if existing:
            print("Current value: [set]" if item.secret else f"Current value: {existing}")

        prompt = f"{item.prompt} (blank keeps current): "
        value = getpass.getpass(prompt) if item.secret else input(prompt)
        if value:
            updates[item.name] = value.strip()
        elif item.name not in current:
            updates[item.name] = ""
    return updates


def validate_env(values: dict[str, str]) -> list[str]:
    """Return validation messages for important configuration values."""
    messages: list[str] = []
    sec_user_agent = values.get("SEC_USER_AGENT", "").strip()
    if not sec_user_agent:
        messages.append("SEC_USER_AGENT is required for financial-intelligence SEC EDGAR tools.")
    elif "@" not in sec_user_agent or "example.com" in sec_user_agent.lower():
        messages.append("SEC_USER_AGENT should include a real contact email and must not use example.com.")

    sam_key = values.get("SAM_GOV_API_KEY", "").strip()
    if not sam_key:
        messages.append("SAM_GOV_API_KEY is empty; SAM.gov Exclusions tools will return a missing-key response.")

    gateway_hash = values.get("MCP_GATEWAY_BEARER_TOKEN_SHA256", "").strip()
    if gateway_hash and not re.fullmatch(r"[A-Fa-f0-9]{64}", gateway_hash):
        messages.append("MCP_GATEWAY_BEARER_TOKEN_SHA256 must be a 64-character SHA-256 hex digest.")

    google_key = values.get("GOOGLE_CSE_API_KEY", "").strip()
    google_id = values.get("GOOGLE_CSE_ID", "").strip()
    if bool(google_key) != bool(google_id):
        messages.append("GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID should be set together.")

    return messages


def print_client_snippets(env_file: Path) -> None:
    """Print concise setup snippets for common MCP clients."""
    env_path = env_file.resolve()
    print(
        f"""
Client snippets

Codex CLI / Codex IDE:
  codex mcp add publicRecords --env HC_MCP_ENV_FILE={env_path} -- hc-mcp public-records
  codex mcp add providerEnrollment --env HC_MCP_ENV_FILE={env_path} -- hc-mcp provider-enrollment

Claude Code:
  claude mcp add public-records --env HC_MCP_ENV_FILE={env_path} -- hc-mcp public-records
  claude mcp add provider-enrollment --env HC_MCP_ENV_FILE={env_path} -- hc-mcp provider-enrollment

Claude Desktop stdio JSON:
  {{
    "mcpServers": {{
      "public-records": {{
        "command": "hc-mcp",
        "args": ["public-records"],
        "env": {{"HC_MCP_ENV_FILE": "{env_path}"}}
      }}
    }}
  }}
"""
    )


def import_manual_caches(
    *,
    cache_root: Path,
    breach_csv: Path | None = None,
    docgraph_csv: Path | None = None,
    docgraph_parquet: Path | None = None,
    state_breach_notices: list[str] | None = None,
    state_breach_source_url: str = "",
) -> list[str]:
    """Import source files into the shared cache."""
    if docgraph_csv and docgraph_parquet:
        raise SystemExit("Use only one of --import-docgraph-csv or --import-docgraph-parquet.")

    root = cache_root.expanduser()
    results: list[str] = []
    if breach_csv:
        target = root / "public-records" / "hipaa_breaches.csv"
        _copy_seed_file(breach_csv, target)
        results.append(f"Imported HIPAA breach CSV -> {target}")

    if docgraph_parquet:
        target = root / "docgraph" / "shared_patients.parquet"
        _copy_seed_file(docgraph_parquet, target)
        results.append(f"Imported DocGraph Parquet -> {target}")

    if docgraph_csv:
        rows = _convert_docgraph_csv(docgraph_csv, root / "docgraph" / "shared_patients.parquet")
        results.append(f"Imported DocGraph CSV -> {root / 'docgraph' / 'shared_patients.parquet'} ({rows} rows)")

    if state_breach_notices:
        from servers.public_records import data_loaders

        state, path_text = state_breach_notices
        target = root / "public-records" / "state_breach_notices.parquet"
        original_parquet = data_loaders._STATE_BREACH_NOTICES_PARQUET
        try:
            data_loaders._STATE_BREACH_NOTICES_PARQUET = target
            result = data_loaders.import_state_breach_notices(
                state,
                Path(path_text).expanduser(),
                source_url=state_breach_source_url,
            )
        finally:
            data_loaders._STATE_BREACH_NOTICES_PARQUET = original_parquet
        results.append(
            f"Imported {result['rows_imported']} {state.upper()} state breach notices -> {target}"
        )

    return results


def acquire_public_caches(
    *,
    cache_root: Path,
    hipaa_breaches: bool = False,
    provider_enrollment: bool = False,
    force: bool = False,
) -> list[str]:
    """Acquire public datasets that have stable unauthenticated access paths."""
    root = cache_root.expanduser()
    results: list[str] = []

    if hipaa_breaches:
        target = root / "public-records" / "hipaa_breaches.csv"
        rows = _acquire_hipaa_breaches(target, force=force)
        results.append(f"Acquired HIPAA breach CSV -> {target} ({rows} rows)")

    if provider_enrollment:
        provider_dir = root / "provider-enrollment"
        manifests = asyncio.run(_acquire_provider_enrollment(provider_dir, force=force))
        total_rows = sum(manifest.record_count or 0 for manifest in manifests)
        results.append(
            f"Acquired CMS provider enrollment caches -> {provider_dir} "
            f"({len(manifests)} datasets, {total_rows} rows)"
        )

    return results


async def _acquire_provider_enrollment(cache_dir: Path, *, force: bool = False):
    from servers.provider_enrollment import data_loaders

    return await data_loaders.ensure_all_datasets_cached(cache_dir=cache_dir, force_refresh=force)


def print_cache_status(cache_root: Path) -> None:
    """Print cache status and setup instructions."""
    root = cache_root.expanduser()
    print(f"\nData cache: {root}")
    for item in MANUAL_CACHE_ITEMS:
        seed = root / item.seed_path
        parquet = root / item.parquet_path
        if item.unavailable_reason:
            ready = parquet.exists()
            status = "READY" if ready else "UNAVAILABLE"
        else:
            ready = seed.exists() or parquet.exists()
            status = "READY" if ready else "MISSING"
        print(f"- {item.name}: {status}")
        print(f"  seed: {seed}")
        print(f"  cache: {parquet}")
        if not ready:
            if item.unavailable_reason:
                print(f"  data_unavailable: {item.unavailable_reason}")
            print(f"  affected tools: {', '.join(item.tools)}")
            print(f"  next step: {item.instructions}")
            print(f"  source: {item.source_url}")
            if item.acquire_flag:
                print(f"  acquire: hc-mcp-setup {item.acquire_flag}")
            print(f"  import: hc-mcp-setup {item.import_flag} <downloaded-file>")
            print(f"  note: {item.automation_note}")
    for item in PUBLIC_CACHE_ITEMS:
        ready_paths = [root / path for path in item.cache_paths if _public_cache_path_ready(root / path)]
        status = "READY" if ready_paths else "MISSING"
        print(f"- {item.name}: {status}")
        for path in item.cache_paths:
            print(f"  cache: {root / path}")
        if not ready_paths:
            print(f"  affected tools: {', '.join(item.tools)}")
            print(f"  next step: {item.instructions}")
            print(f"  source: {item.source_url}")
            print(f"  acquire: hc-mcp-setup {item.acquire_flag}")


def print_cache_guide(cache_root: Path) -> None:
    """Print human-oriented acquisition steps for cache files."""
    root = cache_root.expanduser()
    print(f"\nData acquisition guide for cache root: {root}")
    for item in MANUAL_CACHE_ITEMS:
        print(f"\n{item.name}")
        print(f"  source: {item.source_url}")
        print(f"  automation: {item.automation_note}")
        if item.acquire_flag:
            print(f"  acquire: hc-mcp-setup {item.acquire_flag}")
        else:
            print(f"  acquire: {item.instructions}")
        print(f"  import: hc-mcp-setup {item.import_flag} /path/to/downloaded-file")
        print(f"  target seed: {root / item.seed_path}")
        print(f"  target cache: {root / item.parquet_path}")
    for item in PUBLIC_CACHE_ITEMS:
        print(f"\n{item.name}")
        print(f"  source: {item.source_url}")
        print(f"  acquire: hc-mcp-setup {item.acquire_flag}")
        print(f"  expected output: {item.instructions}")
        for path in item.cache_paths:
            print(f"  cache: {root / path}")


def print_agent_cache_instructions(cache_root: Path) -> None:
    """Print copy-paste instructions for a browser-capable coding agent."""
    root = cache_root.expanduser()
    print(
        f"""
Acquire healthcare-data-mcp cache datasets and import them into {root}.

For each missing dataset:
"""
    )
    for item in MANUAL_CACHE_ITEMS:
        seed = root / item.seed_path
        parquet = root / item.parquet_path
        if seed.exists() or parquet.exists():
            continue
        if item.acquire_flag:
            print(f"- {item.name}: run hc-mcp-setup {item.acquire_flag}")
            continue
        print(f"- {item.name}: {item.agent_prompt}")
        print(f"  Source: {item.source_url}")
        print(f"  Import command: hc-mcp-setup {item.import_flag} <downloaded-file>")
    for item in PUBLIC_CACHE_ITEMS:
        if any(_public_cache_path_ready(root / path) for path in item.cache_paths):
            continue
        print(f"- {item.name}: run hc-mcp-setup {item.acquire_flag}")
        print(f"  Source: {item.source_url}")
    print("\nAfter imports, run: hc-mcp-setup --cache-status")


def _public_cache_path_ready(path: Path) -> bool:
    if not path.exists():
        return False
    if path.name == "artifact_index.json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return False
        return int(payload.get("artifact_count") or len(payload.get("artifacts") or [])) > 0
    if path.name == "artifact_metadata.csv":
        try:
            return len(path.read_text(encoding="utf-8").splitlines()) > 1
        except Exception:
            return False
    if path.is_dir():
        return any(child.is_file() for child in path.rglob("*"))
    return path.stat().st_size > 0


def _copy_seed_file(source: Path, target: Path) -> None:
    source_path = source.expanduser()
    if not source_path.exists() or not source_path.is_file():
        raise SystemExit(f"Manual cache source file not found: {source_path}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, target)


def _convert_docgraph_csv(source: Path, target: Path) -> int:
    source_path = source.expanduser()
    if not source_path.exists() or not source_path.is_file():
        raise SystemExit(f"DocGraph CSV not found: {source_path}")

    import pandas as pd

    df = pd.read_csv(source_path, dtype=str, keep_default_na=False, low_memory=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    col_map = {}
    for col in df.columns:
        if "from" in col or col in ("npi1", "npi_1", "referring_npi"):
            col_map[col] = "npi_from"
        elif "to" in col or col in ("npi2", "npi_2", "referred_npi"):
            col_map[col] = "npi_to"
        elif "shared" in col or "patient" in col:
            col_map[col] = "shared_count"
        elif "transaction" in col or "claim" in col:
            col_map[col] = "transaction_count"
        elif "same_day" in col or "sameday" in col:
            col_map[col] = "same_day_count"

    df = df.rename(columns=col_map)
    for req_col in ["npi_from", "npi_to"]:
        if req_col not in df.columns:
            if len(df.columns) < 2:
                raise SystemExit(f"Cannot identify required column '{req_col}' in DocGraph CSV.")
            original = list(df.columns)
            df = df.rename(columns={original[0]: "npi_from", original[1]: "npi_to"})
            if len(original) >= 3:
                df = df.rename(columns={original[2]: "shared_count"})

    for col in ["shared_count", "transaction_count", "same_day_count"]:
        if col not in df.columns:
            df[col] = "0"
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)  # type: ignore[union-attr]

    df = df[["npi_from", "npi_to", "shared_count", "transaction_count", "same_day_count"]]
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(target, compression="zstd", index=False)
    except ImportError:
        import polars as pl

        pl.DataFrame(df.to_dict(orient="list")).write_parquet(target, compression="zstd")
    return len(df)


def _acquire_hipaa_breaches(target: Path, *, force: bool = False) -> int:
    if target.exists() and not force:
        raise SystemExit(f"HIPAA breach CSV already exists: {target}. Use --force-cache-refresh to replace it.")

    import pandas as pd

    tables = pd.read_html("https://ocrportal.hhs.gov/ocr/breach/breach_report_hip.jsf")
    breach_table = None
    for table in tables:
        columns = [str(col).strip() for col in table.columns]
        if "Name of Covered Entity" in columns and "Breach Submission Date" in columns:
            breach_table = table
            break

    if breach_table is None:
        raise SystemExit("Could not find the HHS OCR HIPAA breach results table.")

    breach_table = breach_table.dropna(axis="columns", how="all")
    breach_table.columns = [str(col).strip() for col in breach_table.columns]
    if breach_table.empty:
        raise SystemExit("HHS OCR HIPAA breach results table was empty.")

    target.parent.mkdir(parents=True, exist_ok=True)
    breach_table.to_csv(target, index=False)
    return len(breach_table)


def _parse_set_values(items: list[str]) -> dict[str, str]:
    updates: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"--set must be KEY=VALUE, got: {item}")
        key, value = item.split("=", 1)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise SystemExit(f"Invalid environment key: {key}")
        updates[key] = value
    return updates


if __name__ == "__main__":
    main()
