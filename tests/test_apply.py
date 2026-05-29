import pytest

from pipeline_guardian.apply import apply_remediation
from pipeline_guardian.seed import (
    seed_orphaned_baseline,
    seed_stale_watermark,
    seed_stuck_advisory_lock,
)
from pipeline_guardian.types import ProposedRemediation


class TestApplyPruneOrphaned:
    @pytest.mark.asyncio
    async def test_deletes_expected_rows(self, db_pool) -> None:
        ctx = await seed_orphaned_baseline(db_pool)
        proposal = ProposedRemediation(
            tool_name="prune_orphaned",
            tool_input={"run_id": ctx["run_id"], "expected_count": 100},
        )
        await apply_remediation(db_pool, proposal)
        async with db_pool.acquire() as conn:
            remaining = await conn.fetchval(
                "select count(*) from stg_events where pipeline_run_id=$1", ctx["run_id"]
            )
        assert remaining == 0


class TestApplyClearStaleLock:
    @pytest.mark.asyncio
    async def test_removes_marker_row(self, db_pool) -> None:
        ctx = await seed_stuck_advisory_lock(db_pool)
        proposal = ProposedRemediation(
            tool_name="clear_stale_lock",
            tool_input={"lock_id": ctx["lock_id"]},
        )
        await apply_remediation(db_pool, proposal)
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "select lock_id from pipeline.stuck_locks where lock_id=$1", ctx["lock_id"]
            )
        assert row is None


class TestApplyResetWatermark:
    @pytest.mark.asyncio
    async def test_updates_watermark_value(self, db_pool) -> None:
        ctx = await seed_stale_watermark(db_pool)
        proposal = ProposedRemediation(
            tool_name="reset_watermark",
            tool_input={
                "stream_name": ctx["stream_name"],
                "to_value": ctx["source_latest"],
            },
        )
        await apply_remediation(db_pool, proposal)
        async with db_pool.acquire() as conn:
            value = await conn.fetchval(
                "select value from pipeline.watermarks where stream_name=$1", ctx["stream_name"]
            )
        assert value == ctx["source_latest"]


class TestApplyUnknownTool:
    @pytest.mark.asyncio
    async def test_raises_on_unknown_tool(self, db_pool) -> None:
        proposal = ProposedRemediation(
            tool_name="nope",  # type: ignore[arg-type]
            tool_input={},
        )
        with pytest.raises(KeyError):
            await apply_remediation(db_pool, proposal)
