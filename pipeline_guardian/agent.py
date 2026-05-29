"""Hybrid LLM agent.

When ANTHROPIC_API_KEY is set and force_replay=False: calls real Claude.
When key is missing OR force_replay=True: replays from JSONL fixture.

The fixture maps `kind` -> `tool_name` + `tool_input_template`. Templates
use `$<key>` placeholders that get rendered from the failure's signature dict.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pipeline_guardian.runbook import TOOLS
from pipeline_guardian.types import DetectedFailure, ProposedRemediation, RemediationTool

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "llm_replay.jsonl"


def _load_fixtures() -> dict[str, dict[str, Any]]:
    """Read llm_replay.jsonl and return {kind: entry}."""
    out: dict[str, dict[str, Any]] = {}
    with _FIXTURE_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            out[entry["kind"]] = entry
    return out


def _render_template(template: dict[str, Any], signature: dict[str, Any]) -> dict[str, Any]:
    """Replace $<key> placeholders in template values with signature[key]."""
    out: dict[str, Any] = {}
    for k, v in template.items():
        if isinstance(v, str) and v.startswith("$"):
            sig_key = v[1:]
            if sig_key not in signature:
                raise KeyError(f"Template references missing signature key: {sig_key}")
            out[k] = signature[sig_key]
        else:
            out[k] = v
    return out


def _replay_from_fixture(failure: DetectedFailure) -> ProposedRemediation:
    fixtures = _load_fixtures()
    if failure.kind not in fixtures:
        raise KeyError(f"No fixture entry for kind: {failure.kind}")
    entry = fixtures[failure.kind]
    tool_name: RemediationTool = entry["tool_name"]
    tool_input = _render_template(entry["tool_input_template"], failure.signature)
    return ProposedRemediation(tool_name=tool_name, tool_input=tool_input)


async def _call_claude(failure: DetectedFailure, api_key: str) -> ProposedRemediation:
    """Call real Claude API with structured tool use."""
    import anthropic

    client = anthropic.AsyncAnthropic(api_key=api_key)
    system_prompt = (
        "You are pipeline-guardian, a remediation agent for ETL failures. "
        "You will be given a detected failure with a signature. Pick exactly one "
        "tool to remediate it. Fill in the tool arguments from the signature. "
        "Never invent values not present in the signature."
    )
    user_content = (
        f"Detected failure (kind={failure.kind}):\n"
        f"{json.dumps(failure.signature, indent=2)}\n\n"
        "Pick the correct remediation."
    )
    response = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=512,
        system=system_prompt,
        tools=TOOLS,
        tool_choice={"type": "any"},
        messages=[{"role": "user", "content": user_content}],
    )
    for block in response.content:
        if block.type == "tool_use":
            return ProposedRemediation(
                tool_name=block.name,  # type: ignore[arg-type]
                tool_input=dict(block.input),  # type: ignore[arg-type]
            )
    raise RuntimeError("Claude did not return a tool_use block")


async def propose_remediation(
    failure: DetectedFailure,
    *,
    force_replay: bool = False,
) -> ProposedRemediation:
    """Top-level entry point. Routes to real Claude or fixture replay."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if force_replay or not api_key:
        return _replay_from_fixture(failure)
    return await _call_claude(failure, api_key)
