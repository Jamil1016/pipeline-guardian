"""Deterministic per-remediation safety gates.

Each gate re-validates the precondition at apply time. The LLM is never the
last check. If a gate fails, the proposed remediation must NOT be applied.
"""

from __future__ import annotations

import asyncpg

from pipeline_guardian.types import GateResult, ProposedRemediation


async def _gate_prune_orphaned(pool: asyncpg.Pool, args: dict) -> GateResult:
    run_id = args.get("run_id")
    expected = args.get("expected_count")
    if not isinstance(run_id, int) or not isinstance(expected, int):
        return GateResult(passed=False, reason="invalid args: run_id and expected_count must be int")

    async with pool.acquire() as conn:
        status = await conn.fetchval("select status from pipeline.runs where id=$1", run_id)
        if status is None:
            return GateResult(passed=False, reason=f"run_id {run_id} not found")
        if status != "failed":
            return GateResult(
                passed=False,
                reason=f"run status is '{status}', not 'failed' — refusing to prune",
            )
        actual = await conn.fetchval(
            "select count(*) from stg_events where pipeline_run_id=$1", run_id
        )

    if actual != expected:
        return GateResult(
            passed=False,
            reason=f"count mismatch: expected {expected}, found {actual}",
        )
    return GateResult(passed=True, reason="ok")


async def _gate_clear_stale_lock(pool: asyncpg.Pool, args: dict) -> GateResult:
    lock_id = args.get("lock_id")
    if not isinstance(lock_id, int):
        return GateResult(passed=False, reason="invalid args: lock_id must be int")

    async with pool.acquire() as conn:
        await conn.execute(
            "create table if not exists pipeline.stuck_locks ("
            " lock_id bigint primary key, held_since timestamptz not null default now())"
        )
        row = await conn.fetchrow(
            "select lock_id from pipeline.stuck_locks where lock_id=$1", lock_id
        )

    if row is None:
        return GateResult(passed=False, reason=f"lock_id {lock_id} not found in stuck_locks")
    return GateResult(passed=True, reason="ok")


async def _gate_reset_watermark(pool: asyncpg.Pool, args: dict) -> GateResult:
    stream = args.get("stream_name")
    to_value = args.get("to_value")
    if not isinstance(stream, str) or not isinstance(to_value, str):
        return GateResult(
            passed=False, reason="invalid args: stream_name and to_value must be str"
        )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select w.value as current_value, s.source_latest
            from pipeline.watermarks w
            join pipeline.source_status s on s.stream_name = w.stream_name
            where w.stream_name = $1
            """,
            stream,
        )
    if row is None:
        return GateResult(passed=False, reason=f"stream '{stream}' has no watermark+source_status")

    current = row["current_value"]
    source = row["source_latest"]
    # Parse as integers for numeric comparison; fall back to lexicographic if non-numeric.
    try:
        to_int = int(to_value)
        current_int = int(current)
        source_int = int(source)
        backward = to_int < current_int
        past_source = to_int > source_int
    except ValueError:
        backward = to_value < current
        past_source = to_value > source

    if backward:
        return GateResult(
            passed=False,
            reason=f"to_value '{to_value}' would move watermark backward from current '{current}'",
        )
    if past_source:
        return GateResult(
            passed=False,
            reason=f"to_value '{to_value}' exceeds source_latest '{source}' — would skip data",
        )
    return GateResult(passed=True, reason="ok")


_GATES = {
    "prune_orphaned": _gate_prune_orphaned,
    "clear_stale_lock": _gate_clear_stale_lock,
    "reset_watermark": _gate_reset_watermark,
}


async def gate_remediation(pool: asyncpg.Pool, proposal: ProposedRemediation) -> GateResult:
    """Dispatch to the per-tool gate. Returns BLOCK for unknown tools."""
    gate_fn = _GATES.get(proposal.tool_name)
    if gate_fn is None:
        return GateResult(passed=False, reason=f"unknown tool: {proposal.tool_name}")
    return await gate_fn(pool, proposal.tool_input)
