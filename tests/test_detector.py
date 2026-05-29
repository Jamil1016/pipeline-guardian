import pytest

from pipeline_guardian.detector import detect_all
from pipeline_guardian.seed import (
    seed_orphaned_baseline,
    seed_stale_watermark,
    seed_stuck_advisory_lock,
)


class TestDetectorEmpty:
    @pytest.mark.asyncio
    async def test_no_failures_in_clean_state(self, db_pool) -> None:
        found = await detect_all(db_pool)
        assert found == []


class TestDetectorOrphanedBaseline:
    @pytest.mark.asyncio
    async def test_finds_orphaned_run(self, db_pool) -> None:
        ctx = await seed_orphaned_baseline(db_pool)
        found = await detect_all(db_pool)
        kinds = [f.kind for f in found]
        assert "orphaned_baseline" in kinds
        orphaned = next(f for f in found if f.kind == "orphaned_baseline")
        assert orphaned.signature["run_id"] == ctx["run_id"]
        assert orphaned.signature["orphaned_count"] == 100


class TestDetectorStuckLock:
    @pytest.mark.asyncio
    async def test_finds_stuck_lock(self, db_pool) -> None:
        ctx = await seed_stuck_advisory_lock(db_pool)
        found = await detect_all(db_pool)
        kinds = [f.kind for f in found]
        assert "stuck_advisory_lock" in kinds
        lock = next(f for f in found if f.kind == "stuck_advisory_lock")
        assert lock.signature["lock_id"] == ctx["lock_id"]


class TestDetectorStaleWatermark:
    @pytest.mark.asyncio
    async def test_finds_stale_watermark(self, db_pool) -> None:
        ctx = await seed_stale_watermark(db_pool)
        found = await detect_all(db_pool)
        kinds = [f.kind for f in found]
        assert "stale_watermark" in kinds
        wm = next(f for f in found if f.kind == "stale_watermark")
        assert wm.signature["stream_name"] == ctx["stream_name"]
        assert wm.signature["current_value"] == ctx["current_value"]
        assert wm.signature["source_latest"] == ctx["source_latest"]


class TestSignatureHashStability:
    @pytest.mark.asyncio
    async def test_same_signature_same_hash(self, db_pool) -> None:
        """Detecting the same state twice produces the same signature_hash."""
        await seed_orphaned_baseline(db_pool)
        first = await detect_all(db_pool)
        second = await detect_all(db_pool)
        assert first[0].signature_hash == second[0].signature_hash


class TestDetectorMultipleFailures:
    @pytest.mark.asyncio
    async def test_finds_all_three_when_all_seeded(self, db_pool) -> None:
        await seed_orphaned_baseline(db_pool)
        await seed_stuck_advisory_lock(db_pool)
        await seed_stale_watermark(db_pool)
        found = await detect_all(db_pool)
        kinds = sorted(f.kind for f in found)
        assert kinds == sorted(["orphaned_baseline", "stuck_advisory_lock", "stale_watermark"])
