"""Apply functions — actually mutate the DB. Called ONLY after the gate passes."""

from __future__ import annotations

import asyncpg

from pipeline_guardian.types import ProposedRemediation


async def _apply_prune_orphaned(pool: asyncpg.Pool, args: dict) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "delete from stg_events where pipeline_run_id=$1", args["run_id"]
        )


async def _apply_clear_stale_lock(pool: asyncpg.Pool, args: dict) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "delete from pipeline.stuck_locks where lock_id=$1", args["lock_id"]
        )


async def _apply_reset_watermark(pool: asyncpg.Pool, args: dict) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "update pipeline.watermarks set value=$2, updated_at=now() where stream_name=$1",
            args["stream_name"],
            args["to_value"],
        )


_APPLIES = {
    "prune_orphaned": _apply_prune_orphaned,
    "clear_stale_lock": _apply_clear_stale_lock,
    "reset_watermark": _apply_reset_watermark,
}


async def apply_remediation(pool: asyncpg.Pool, proposal: ProposedRemediation) -> None:
    """Dispatch to the per-tool apply. Raises KeyError on unknown tool."""
    if proposal.tool_name not in _APPLIES:
        raise KeyError(f"Unknown tool: {proposal.tool_name}")
    await _APPLIES[proposal.tool_name](pool, proposal.tool_input)
