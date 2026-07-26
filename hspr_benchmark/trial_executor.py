"""Fail-closed controller for one isolated HSPR benchmark answer trial."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from .container_launcher import (
    assemble_answer_packet,
    codex_docker_command,
    run_codex_and_capture,
    stage_runtime,
    summarize_codex_trace,
    write_new_text,
)
from .leakage import audit_registry_packet


def trial_prompt(hspr_available: bool) -> str:
    identity_instruction = (
        "First read /input/hspr-identity.json and use it only for identity/perimeter resolution. "
        if hspr_available
        else "No registry is available; research identity/perimeter from live sources. "
    )
    return (
        "You are one blinded financial-research benchmark answer context. "
        "Read /input/question.json. "
        + identity_instruction
        + "Retrieve the financial result now from a live authoritative primary source using web search/open. "
        "Do not rely on memory alone. Do not estimate or silently aggregate entities. "
        "Return exactly one JSON object conforming to /input/response-schema.json, including the primary "
        "source URL, exact locator, perimeter, and all material caveats. You cannot ask a human for help."
    )


def _validate_answer_shape(answer: dict[str, Any], question_id: str) -> None:
    required = {
        "question_id",
        "answer_status",
        "reporting_perimeter",
        "metric_label",
        "period",
        "units",
        "value",
        "primary_source_url",
        "exact_locator",
        "caveats",
        "aggregated_entities",
    }
    if set(answer) - {
        *required,
        "clarification_needed",
        "closest_reported_subtotal",
    }:
        raise ValueError("answer contains fields outside the locked schema")
    if not required <= set(answer) or answer.get("question_id") != question_id:
        raise ValueError("answer is missing locked fields or has the wrong question_id")


def run_trial(
    *,
    question: dict[str, Any],
    arm: dict[str, Any],
    registry_packet: Path,
    sealed_gold: Path,
    response_schema: Path,
    runtime_source: Path,
    codex_package_dir: Path,
    output_dir: Path,
    credential_json: bytes,
    allow_live_credential: bool = False,
) -> dict[str, Any]:
    """Execute one answer context; sealed gold is audited but never mounted."""
    if not allow_live_credential:
        raise PermissionError("live credential use requires an explicit, risk-aware caller opt-in")
    hspr_available = bool(arm.get("hspr_available"))
    audit = audit_registry_packet(registry_packet, sealed_gold)
    if not audit["passed"]:
        raise ValueError(f"registry leakage audit failed: {audit['findings']}")
    selected_registry: Path | None = registry_packet if hspr_available else None
    if ".benchmark-sealed" in output_dir.absolute().parts:
        raise ValueError("answer results may not be written under the sealed root")
    output_parent = output_dir.parent.resolve(strict=True)
    if output_dir.parent.absolute() != output_parent:
        raise ValueError("answer result parent may not traverse symlinks")
    output_dir = output_parent / output_dir.name
    output_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
    output_dir.chmod(0o700)
    with tempfile.TemporaryDirectory(prefix="hspr-answer-context-") as temp_name:
        temporary_root = Path(temp_name)
        packet = assemble_answer_packet(
            temporary_root / "packet",
            question=question,
            response_schema=response_schema,
            registry_packet=selected_registry,
        )
        runtime = stage_runtime(runtime_source, temporary_root / "runtime")
        command = codex_docker_command(
            packet_dir=packet,
            output_dir=output_dir,
            runtime_dir=runtime,
            codex_package_dir=codex_package_dir,
            model="gpt-5.6-luna",
            reasoning=str(arm["reasoning"]),
            prompt=trial_prompt(hspr_available),
        )
        rendered = " ".join(command)
        for forbidden in (str(sealed_gold.resolve()), ".benchmark-sealed", str(Path.cwd().resolve())):
            if forbidden in rendered:
                raise RuntimeError("forbidden host path escaped into the answer command")
        trace = run_codex_and_capture(
            command,
            output_dir / "native-trace.json",
            credential_json,
        )
    if trace["events"][-1].get("exit_code") != 0:
        raise RuntimeError("answer runtime failed; inspect the redacted native trace")
    answer = trace.get("answer")
    if not isinstance(answer, dict):
        raise RuntimeError("trusted supervisor answer handoff is missing")
    _validate_answer_shape(answer, str(question["question_id"]))
    write_new_text(output_dir / "answer.json", json.dumps(answer, indent=2))
    summary = summarize_codex_trace(trace)
    write_new_text(output_dir / "observable-summary.json", json.dumps(summary, indent=2))
    return {"answer": answer, "trace": trace, "observable_summary": summary}
