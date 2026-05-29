"""Golden-set evaluation harness.

Loads golden_set/cases.jsonl, runs each through the agent (force_replay=True
for deterministic CI), and reports pass rate. CI fails the build if pass rate
drops below 90%.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline_guardian.agent import propose_remediation
from pipeline_guardian.types import DetectedFailure, EvalResult

_GOLDEN_SET_PATH = Path("golden_set/cases.jsonl")


def _load_cases() -> list[dict]:  # type: ignore[type-arg]
    with _GOLDEN_SET_PATH.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


async def run_golden_set() -> list[EvalResult]:
    """Run every case in the golden set. Returns the per-case results."""
    cases = _load_cases()
    results: list[EvalResult] = []
    for case in cases:
        sig = case["failure_signature"]
        failure = DetectedFailure(
            failure_id=sig["failure_id"],
            kind=sig["kind"],
            signature_hash=sig["signature_hash"],
            signature=sig["signature"],
        )
        proposal = await propose_remediation(failure, force_replay=True)
        results.append(
            EvalResult(
                case_id=case["case_id"],
                expected_tool=case["expected_tool"],
                actual_tool=proposal.tool_name,
                passed=proposal.tool_name == case["expected_tool"],
            )
        )
    return results
