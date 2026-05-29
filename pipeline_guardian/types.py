"""Typed data contracts. No logic, no IO."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

FailureKind = Literal["orphaned_baseline", "stuck_advisory_lock", "stale_watermark"]
RemediationTool = Literal["prune_orphaned", "clear_stale_lock", "reset_watermark"]


@dataclass(frozen=True)
class DetectedFailure:
    """A single failure detected by the scanner."""

    failure_id: int            # local ID assigned by detector for the CLI handle
    kind: FailureKind
    signature_hash: str        # 16-hex-char hash of the canonical signature
    signature: dict[str, Any]  # the actual signature payload, kind-specific


@dataclass(frozen=True)
class ProposedRemediation:
    """What the agent proposed."""

    tool_name: RemediationTool
    tool_input: dict[str, Any]


@dataclass(frozen=True)
class GateResult:
    """Outcome of a deterministic precondition check."""

    passed: bool
    reason: str


@dataclass(frozen=True)
class EvalResult:
    """One entry in a golden-set evaluation."""

    case_id: str
    expected_tool: RemediationTool
    actual_tool: str
    passed: bool


class ApplyError(Exception):
    """Raised when an apply step fails despite a passing gate (DB-level error)."""
