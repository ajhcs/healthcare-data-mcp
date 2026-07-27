"""Fail-closed controller for one isolated HSPR benchmark answer trial."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .container_launcher import (
    assemble_answer_packet,
    codex_docker_command,
    is_sealed_location,
    run_codex_and_capture,
    stage_runtime,
    summarize_codex_trace,
    write_new_text,
)
from .leakage import audit_registry_packet

# This interlock records the launcher property that was proven by the hostile
# mount/auth probe: the answer container receives only its packet, staged
# runtime, output directory, and a minimized short-lived subscription token.
# It does not claim that native web access can prevent an agent from discovering
# public benchmark material.
CORE_ANSWER_ISOLATION_VALIDATED = True
CORE_ANSWER_ISOLATION_POLICY = "container-no-repository-sealed-history-host-output-v1"


def require_core_answer_isolation(active_boundary_attestation: dict[str, Any] | None = None) -> None:
    if (
        not CORE_ANSWER_ISOLATION_VALIDATED
        or not active_boundary_attestation
        or not active_boundary_attestation.get("active_manifest_sha256")
        or active_boundary_attestation.get("core_isolation_policy") != CORE_ANSWER_ISOLATION_POLICY
    ):
        raise RuntimeError(
            "official execution is disabled: the protected unpublished active packet "
            "is not bound to the validated core answer-container isolation policy"
        )


def require_official_web_boundary(active_boundary_attestation: dict[str, Any] | None = None) -> None:
    """Backward-compatible name for callers predating the scoped isolation policy."""
    require_core_answer_isolation(active_boundary_attestation)


def trial_prompt(hspr_available: bool) -> str:
    identity_instruction = (
        "First read /input/hspr-identity.json with /bin/cat and use it only for identity/perimeter resolution. "
        if hspr_available
        else "No registry is available; research identity/perimeter from live sources. "
    )
    return (
        "You are one blinded financial-research benchmark answer context. "
        "Read /input/question.json with /bin/cat. "
        + identity_instruction
        + "Do not search for or use benchmark materials, this project's repository, GitHub history, issues, "
        "pull requests or forks, prior benchmark answers, or scoring/key artifacts. If you encounter any "
        "incidentally, ignore them and continue with independent authoritative-source research. "
        + "Retrieve the financial result now from a live authoritative primary source using web search/open. "
        "Do not rely on memory alone. Do not estimate or silently aggregate entities. "
        "Report each numeric value exactly in the source's displayed scale and set units.currency to USD and "
        "units.scale to ones, thousands, millions, or billions; never rescale without changing units.scale. "
        "Return exactly one JSON object conforming to /input/response-schema.json, including the primary "
        "source URL, exact locator, perimeter, and all material caveats. You cannot ask a human for help."
    )


def _validate_answer_shape(answer: dict[str, Any], question_id: str, response_schema: Path) -> None:
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
        "clarification_needed",
        "closest_reported_subtotal",
    }
    if set(answer) - required:
        raise ValueError("answer contains fields outside the locked schema")
    if not required <= set(answer) or answer.get("question_id") != question_id:
        raise ValueError("answer is missing locked fields or has the wrong question_id")
    schema = json.loads(response_schema.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    errors = sorted(Draft202012Validator(schema).iter_errors(answer), key=lambda item: list(item.path))
    if errors:
        paths = [".".join(str(part) for part in error.path) or "$" for error in errors]
        raise ValueError(f"answer violates the locked response schema at: {', '.join(paths)}")


def _validate_subscription_credential_boundary(credential_json: bytes) -> None:
    try:
        document = json.loads(credential_json)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("answer credential is not valid JSON") from error
    if not isinstance(document, dict) or set(document) != {"auth_mode", "OPENAI_API_KEY", "tokens"}:
        raise ValueError("answer credential has fields outside the minimized subscription schema")
    if document.get("auth_mode") != "chatgpt" or document.get("OPENAI_API_KEY") not in (None, ""):
        raise ValueError("answer credential must use ChatGPT subscription auth without an API key")
    tokens = document.get("tokens")
    required = {"access_token", "account_id"}
    if not isinstance(tokens, dict) or set(tokens) != required:
        raise ValueError("answer credential token fields do not match the minimized schema")
    if not all(isinstance(tokens.get(name), str) and tokens[name] for name in required):
        raise ValueError("answer credential is missing a required short-lived field")


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
    registry_approved_root: Path | None = None,
    active_boundary_attestation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute one answer context; sealed gold is audited but never mounted."""
    if not allow_live_credential:
        raise PermissionError("live credential use requires an explicit, risk-aware caller opt-in")
    require_core_answer_isolation(active_boundary_attestation)
    _validate_subscription_credential_boundary(credential_json)
    hspr_available = bool(arm.get("hspr_available"))
    audit = audit_registry_packet(registry_packet, sealed_gold)
    if not audit["passed"]:
        raise ValueError(f"registry leakage audit failed: {audit['findings']}")
    selected_registry: Path | None = registry_packet if hspr_available else None
    if is_sealed_location(output_dir):
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
            registry_approved_root=registry_approved_root,
        )
        runtime = stage_runtime(runtime_source, temporary_root / "runtime")
        command = codex_docker_command(
            packet_dir=packet,
            output_dir=output_dir,
            runtime_dir=runtime,
            codex_package_dir=codex_package_dir,
            model=str(arm.get("model", "gpt-5.6-luna")),
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
        raise TypeError("trusted supervisor answer handoff is missing")
    _validate_answer_shape(answer, str(question["question_id"]), response_schema)
    write_new_text(output_dir / "answer.json", json.dumps(answer, indent=2))
    summary = summarize_codex_trace(trace)
    write_new_text(output_dir / "observable-summary.json", json.dumps(summary, indent=2))
    return {"answer": answer, "trace": trace, "observable_summary": summary}
