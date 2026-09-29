from __future__ import annotations

import json

from servers.form_990_facts.sealed_benchmark import (
    parse_json_answer,
    replace_plugin_enabled,
    score_answer,
    summarize_jsonl,
)


def test_plugin_toggle_replaces_only_the_exact_personal_plugin_section() -> None:
    config = b"""[plugins."other@personal"]
enabled = true

[plugins."form-990-facts@personal"]
enabled = true
"""

    disabled = replace_plugin_enabled(config, enabled=False)

    assert b'[plugins."other@personal"]\nenabled = true' in disabled
    assert b'[plugins."form-990-facts@personal"]\nenabled = false' in disabled
    assert replace_plugin_enabled(disabled, enabled=True) == config


def test_jsonl_summary_counts_calls_tokens_and_observable_bytes() -> None:
    lines = [
        json.dumps(
            {
                "type": "item.completed",
                "item": {
                    "id": "mcp-1",
                    "type": "mcp_tool_call",
                    "server": "form-990-facts",
                    "tool": "get_form_990_facts",
                    "result": {"status": "ready"},
                },
            }
        ),
        json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "web-1", "type": "web_search", "query": "IRS 990"},
            }
        ),
        json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "answer-1", "type": "agent_message", "text": '{"status":"ready"}'},
            }
        ),
        json.dumps(
            {
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 100,
                    "cached_input_tokens": 20,
                    "output_tokens": 30,
                    "reasoning_output_tokens": 10,
                },
            }
        ),
    ]
    jsonl = "\n".join(lines) + "\n"

    summary = summarize_jsonl(jsonl)

    assert summary["calls"] == {"mcp_tool_call": 1, "web_search": 1}
    assert summary["tokens"] == {
        "input_tokens": 100,
        "cached_input_tokens": 20,
        "cache_write_input_tokens": 0,
        "output_tokens": 30,
        "reasoning_output_tokens": 10,
    }
    assert summary["mcp_result_bytes"] == len(b'{"status":"ready"}')
    assert summary["final_text"] == '{"status":"ready"}'


def test_scoring_keeps_fact_and_provenance_exactness_separate() -> None:
    provenance = {
        "source_url": "https://apps.irs.gov/example.zip",
        "object_id": "202641349349301439",
        "xml_sha256": "a" * 64,
        "metric_key": "total_revenue",
        "reported_label": "Total revenue",
        "units": "USD",
        "xml_field_paths": ["/Return/ReturnData/IRS990/CYTotalRevenueAmt"],
    }
    gold = {
        "legal_filer": "EXACT FILER",
        "ein": "123456789",
        "tax_period_end": "2025-06-30",
        "form_type": "990",
        "total_revenue": 100,
        "total_expenses": 90,
        "net_assets": 10,
        "top_reported_executive": {
            "name": "JANE DOE",
            "title": "CEO",
            "total_reported_compensation": 5,
        },
        "provenance": {
            "total_revenue": provenance,
            "total_expenses": {**provenance, "metric_key": "total_expenses"},
            "net_assets": {**provenance, "metric_key": "net_assets"},
            "top_reported_executive": {
                **provenance,
                "metric_key": "top_reported_executive",
            },
        },
    }
    answer = {
        **gold,
        "total_revenue": 101,
        "provenance": {**gold["provenance"], "total_revenue": None},
        "scope_caveat": "Exact legal filer only; no brand or affiliate roll-up.",
    }

    score = score_answer(answer, gold)

    assert score["fact_correct"] == score["fact_total"] - 1
    assert score["provenance_correct"] == score["provenance_total"] - 7
    assert score["coverage_correct"] == score["coverage_total"]
    assert score["scope_honest"] is True


def test_final_json_parser_accepts_a_fenced_model_response() -> None:
    assert parse_json_answer('```json\n{"status":"ready"}\n```') == {"status": "ready"}
