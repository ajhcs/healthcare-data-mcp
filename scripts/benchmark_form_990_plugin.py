#!/usr/bin/env python3
"""Run a sealed matched plugin-enabled versus native Form 990 benchmark."""

from __future__ import annotations

import argparse
from collections import Counter
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from servers.form_990_facts.fact_store import Form990FactStore  # noqa: E402
from servers.form_990_facts.sealed_benchmark import (  # noqa: E402
    METRIC_KEYS,
    parse_json_answer,
    replace_plugin_enabled,
    score_answer,
    summarize_jsonl,
)


PLUGIN_ID = "form-990-facts@personal"
PLUGIN_SKILL_MARKER = "form-990-facts:form-990-facts"
TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
)


def _atomic_write(path: Path, data: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        temporary.write(data)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    temporary_path.chmod(mode)
    os.replace(temporary_path, path)


def _plugin_state() -> bool:
    completed = subprocess.run(
        ["codex", "plugin", "list", "--json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    plugins = json.loads(completed.stdout)["installed"]
    match = [plugin for plugin in plugins if plugin["pluginId"] == PLUGIN_ID]
    if len(match) != 1:
        raise RuntimeError(f"Expected one installed {PLUGIN_ID} plugin")
    return bool(match[0]["enabled"])


def _skill_visible(prompt: str) -> bool:
    completed = subprocess.run(
        ["codex", "debug", "prompt-input", prompt],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return PLUGIN_SKILL_MARKER in completed.stdout


def _set_condition(config_path: Path, *, plugin_enabled: bool, prompt: str) -> None:
    current = config_path.read_bytes()
    updated = replace_plugin_enabled(current, enabled=plugin_enabled)
    _atomic_write(config_path, updated, config_path.stat().st_mode & 0o777)
    if _plugin_state() is not plugin_enabled:
        raise RuntimeError("Codex plugin list did not reflect the benchmark condition")
    if _skill_visible(prompt) is not plugin_enabled:
        raise RuntimeError("Codex prompt input did not reflect the benchmark condition")


def _gold_from_result(result: dict[str, Any]) -> dict[str, Any]:
    facts = result["facts"]
    executive = facts["top_reported_executive"]
    return {
        "legal_filer": result["filing"]["legal_filer"],
        "ein": result["filing"]["ein"],
        "tax_period_end": result["filing"]["tax_period_end"],
        "form_type": result["filing"]["form_type"],
        "total_revenue": facts["total_revenue"]["value"],
        "total_expenses": facts["total_expenses"]["value"],
        "net_assets": facts["net_assets"]["value"],
        "top_reported_executive": {
            "name": executive.get("name"),
            "title": executive.get("title"),
            "total_reported_compensation": executive.get("total_reported_compensation"),
        },
        "provenance": {
            metric_key: {
                key: result["fact_provenance"][metric_key][key]
                for key in (
                    "source_url",
                    "object_id",
                    "xml_sha256",
                    "metric_key",
                    "reported_label",
                    "units",
                    "xml_field_paths",
                )
            }
            for metric_key in METRIC_KEYS
        },
    }


def _prompt(case: dict[str, Any]) -> str:
    return f"""Retrieve the exact Form 990 facts for legal filer {case["legal_filer"]} \
(EIN {case["ein"]}) for tax-period end year {case["tax_year"]}.

Return total revenue, total expenses, end-of-year net assets or fund balances,
and the top reported executive's total compensation defined as Form 990 Part
VII columns D + E + F. If an individual executive is not reported for this
exact filer, return null rather than inferring one from a brand or affiliate.
Include per-metric official IRS provenance: source URL, Object ID, XML SHA-256,
metric key, reported label, units, and XML field path(s). State the exact-filer
and no-roll-up boundary.

Use only capabilities visible in this fresh session. Do not inspect local
projects, databases, or files, and do not execute shell commands. Use official
IRS evidence where available. Return only the requested schema-conforming JSON."""


def _run_codex(
    *,
    case: dict[str, Any],
    condition: str,
    model: str,
    reasoning: str,
    schema_path: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    prompt = _prompt(case)
    with tempfile.TemporaryDirectory(prefix=f"form990-{condition}-") as directory:
        command = [
            "codex",
            "exec",
            "--ephemeral",
            "--json",
            "--output-schema",
            str(schema_path),
            "--skip-git-repo-check",
            "--ignore-rules",
            "--sandbox",
            "read-only",
            "-C",
            directory,
            "-m",
            model,
            "-c",
            f'model_reasoning_effort="{reasoning}"',
            "-c",
            'service_tier="priority"',
            "-c",
            'web_search="live"',
            prompt,
        ]
        started = time.monotonic()
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env={**os.environ, "NO_COLOR": "1"},
            )
            timed_out = False
        except subprocess.TimeoutExpired as exc:
            completed = None
            timed_out = True
            stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
            stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        latency_ms = (time.monotonic() - started) * 1000
    if completed is not None:
        stdout = completed.stdout
        stderr = completed.stderr
        returncode = completed.returncode
    else:
        returncode = None

    telemetry = summarize_jsonl(stdout)
    failures: list[str] = []
    if timed_out:
        failures.append("timeout")
    if returncode not in (0, None):
        failures.append(f"codex_exit_{returncode}")
    if telemetry["calls"].get("command_execution", 0):
        failures.append("disallowed_command_execution")
    if condition == "plugin" and telemetry["calls"].get("mcp_tool_call", 0) == 0:
        failures.append("plugin_tool_not_used")

    answer: dict[str, Any] | None = None
    try:
        answer = parse_json_answer(telemetry["final_text"])
    except Exception as exc:
        failures.append(f"invalid_final_json:{type(exc).__name__}")
    return {
        "case_id": case["case_id"],
        "condition": condition,
        "latency_ms": round(latency_ms, 3),
        "timed_out": timed_out,
        "returncode": returncode,
        "failures": failures,
        "valid": not failures,
        "answer": answer,
        "telemetry": {key: value for key, value in telemetry.items() if key != "final_text"},
        "stderr_tail": stderr[-2000:],
        "native_web_retrieved_bytes": None,
        "native_web_retrieved_bytes_note": ("Codex JSONL does not expose native web response byte counts."),
    }


def _aggregate(runs: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for condition in ("plugin", "native"):
        selected = [run for run in runs if run["condition"] == condition]
        token_totals = Counter()
        call_totals = Counter()
        for run in selected:
            token_totals.update(run["telemetry"]["tokens"])
            call_totals.update(run["telemetry"]["calls"])
        valid_scores = [run["score"] for run in selected if run["valid"] and run.get("score")]
        result[condition] = {
            "runs": len(selected),
            "valid_runs": sum(run["valid"] for run in selected),
            "median_latency_ms": round(statistics.median(run["latency_ms"] for run in selected), 3),
            "fact_correct": sum(score["fact_correct"] for score in valid_scores),
            "fact_total": len(selected) * 10,
            "provenance_correct": sum(score["provenance_correct"] for score in valid_scores),
            "provenance_total": len(selected) * 28,
            "coverage_correct": sum(score["coverage_correct"] for score in valid_scores),
            "coverage_total": len(selected) * 4,
            "scope_honest_runs": sum(score["scope_honest"] for score in valid_scores),
            "valid_run_fact_correct": sum(score["fact_correct"] for score in valid_scores),
            "valid_run_fact_total": sum(score["fact_total"] for score in valid_scores),
            "valid_run_provenance_correct": sum(score["provenance_correct"] for score in valid_scores),
            "valid_run_provenance_total": sum(score["provenance_total"] for score in valid_scores),
            "call_totals": dict(sorted(call_totals.items())),
            "token_totals": {field: token_totals[field] for field in TOKEN_FIELDS},
            "observable_mcp_result_bytes": sum(run["telemetry"]["mcp_result_bytes"] for run in selected),
            "native_web_retrieved_bytes": None,
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=REPO_ROOT / ".local" / "irs990-facts.sqlite3",
    )
    parser.add_argument(
        "--cases",
        type=Path,
        default=REPO_ROOT / "configs" / "form990-plugin-benchmark-cases.json",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=REPO_ROOT / "configs" / "form990-plugin-benchmark-response-schema.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "qa" / "reports" / "form_990_plugin_benchmark.json",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("/home/plumbob/.codex/config.toml"),
    )
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--reasoning", default="xhigh")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args()

    cases = json.loads(args.cases.read_text(encoding="utf-8"))
    store = Form990FactStore(args.database)
    gold_by_case: dict[str, dict[str, Any]] = {}
    for case in cases:
        result = store.get_facts(ein=case["ein"], tax_year=case["tax_year"])
        if result["status"] != "ready":
            raise RuntimeError(f"Benchmark case is not warm and ready: {case['case_id']}")
        gold_by_case[case["case_id"]] = _gold_from_result(result)
    store.get_facts(ein="811244422", tax_year=2024)

    original_config = args.config.read_bytes()
    original_hash = sha256(original_config).hexdigest()
    config_mode = args.config.stat().st_mode & 0o777
    lock_path = Path("/tmp/healthcare-data-mcp-form990-plugin-benchmark.lock")
    runs: list[dict[str, Any]] = []
    with lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            sequence = 0
            for repetition in range(args.repetitions):
                for case_index, case in enumerate(cases):
                    conditions = ("plugin", "native") if (case_index + repetition) % 2 == 0 else ("native", "plugin")
                    for condition in conditions:
                        prompt = _prompt(case)
                        _set_condition(
                            args.config,
                            plugin_enabled=condition == "plugin",
                            prompt=prompt,
                        )
                        run = _run_codex(
                            case=case,
                            condition=condition,
                            model=args.model,
                            reasoning=args.reasoning,
                            schema_path=args.schema.resolve(),
                            timeout_seconds=args.timeout_seconds,
                        )
                        run["sequence"] = sequence
                        run["repetition"] = repetition
                        if run["answer"] is not None:
                            run["score"] = score_answer(
                                run["answer"],
                                gold_by_case[case["case_id"]],
                            )
                        runs.append(run)
                        sequence += 1
        finally:
            _atomic_write(args.config, original_config, config_mode)
            if sha256(args.config.read_bytes()).hexdigest() != original_hash:
                raise RuntimeError("Codex config restoration hash mismatch")
            if not _plugin_state():
                raise RuntimeError("Form 990 plugin was not restored enabled")

    codex_version = subprocess.run(
        ["codex", "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()
    report = {
        "benchmark": "sealed plugin-enabled versus plugin-disabled native research",
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "model": args.model,
        "reasoning": args.reasoning,
        "codex_version": codex_version,
        "cases": cases,
        "repetitions": args.repetitions,
        "warm_target": True,
        "cold_ingestion_included": False,
        "gold_exposed_to_model": False,
        "matched_prompt_and_output_schema": True,
        "condition_order": "deterministic alternating AB/BA",
        "retrieved_bytes_limit": (
            "MCP result bytes are observable; native web response bytes are not "
            "exposed by Codex JSONL and are reported as null."
        ),
        "runs": runs,
        "aggregate": _aggregate(runs),
        "config_restored_sha256": original_hash,
        "plugin_finished_enabled": True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(
        args.output,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
        0o644,
    )
    print(json.dumps(report["aggregate"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
