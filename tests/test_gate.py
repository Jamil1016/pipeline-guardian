import pytest

from pipeline_guardian.gate import gate_remediation
from pipeline_guardian.seed import (
    seed_orphaned_baseline,
    seed_stale_watermark,
    seed_stuck_advisory_lock,
)
from pipeline_guardian.types import ProposedRemediation


class TestGatePruneOrphaned:
    @pytest.mark.asyncio
    async def test_pass_when_count_matches(self, db_pool) -> None:
        ctx = await seed_orphaned_baseline(db_pool)
        proposal = ProposedRemediation(
            tool_name="prune_orphaned",
            tool_input={"run_id": ctx["run_id"], "expected_count": 100},
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_BLOCK_when_count_mismatches(self, db_pool) -> None:
        """Gate must refuse if actual orphaned count differs from expected."""
        ctx = await seed_orphaned_baseline(db_pool)
        proposal = ProposedRemediation(
            tool_name="prune_orphaned",
            tool_input={"run_id": ctx["run_id"], "expected_count": 50},  # WRONG
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is False
        assert "count" in result.reason.lower() or "mismatch" in result.reason.lower()

    @pytest.mark.asyncio
    async def test_BLOCK_when_run_not_failed_anymore(self, db_pool) -> None:
        """Gate must refuse if the run's status changed to running/success."""
        ctx = await seed_orphaned_baseline(db_pool)
        async with db_pool.acquire() as conn:
            await conn.execute(
                "update pipeline.runs set status='success' where id=$1",
                ctx["run_id"],
            )
        proposal = ProposedRemediation(
            tool_name="prune_orphaned",
            tool_input={"run_id": ctx["run_id"], "expected_count": 100},
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is False


class TestGateClearStaleLock:
    @pytest.mark.asyncio
    async def test_pass_when_lock_is_seeded(self, db_pool) -> None:
        ctx = await seed_stuck_advisory_lock(db_pool)
        proposal = ProposedRemediation(
            tool_name="clear_stale_lock",
            tool_input={"lock_id": ctx["lock_id"]},
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_BLOCK_when_lock_id_unknown(self, db_pool) -> None:
        """Gate must refuse if no stuck lock with that ID exists."""
        proposal = ProposedRemediation(
            tool_name="clear_stale_lock",
            tool_input={"lock_id": 99999},  # never seeded
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is False


class TestGateResetWatermark:
    @pytest.mark.asyncio
    async def test_pass_when_target_in_range(self, db_pool) -> None:
        ctx = await seed_stale_watermark(db_pool)
        proposal = ProposedRemediation(
            tool_name="reset_watermark",
            tool_input={
                "stream_name": ctx["stream_name"],
                "to_value": ctx["source_latest"],
            },
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_BLOCK_when_target_below_current(self, db_pool) -> None:
        """Gate must refuse if to_value < current_value (would lose data)."""
        ctx = await seed_stale_watermark(db_pool)
        proposal = ProposedRemediation(
            tool_name="reset_watermark",
            tool_input={
                "stream_name": ctx["stream_name"],
                "to_value": "50",  # below current 100
            },
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is False
        assert "backward" in result.reason.lower() or "current" in result.reason.lower()

    @pytest.mark.asyncio
    async def test_BLOCK_when_target_above_source_latest(self, db_pool) -> None:
        """Gate must refuse if to_value > source_latest (would skip data)."""
        ctx = await seed_stale_watermark(db_pool)
        proposal = ProposedRemediation(
            tool_name="reset_watermark",
            tool_input={
                "stream_name": ctx["stream_name"],
                "to_value": "9999",  # way past source_latest 250
            },
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is False


class TestGateDispatchUnknown:
    @pytest.mark.asyncio
    async def test_BLOCK_unknown_tool(self, db_pool) -> None:
        proposal = ProposedRemediation(
            tool_name="not_a_real_tool",  # type: ignore[arg-type]
            tool_input={},
        )
        result = await gate_remediation(db_pool, proposal)
        assert result.passed is False
