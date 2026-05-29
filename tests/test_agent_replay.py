import pytest

from pipeline_guardian.agent import propose_remediation
from pipeline_guardian.types import DetectedFailure


class TestReplayMode:
    @pytest.mark.asyncio
    async def test_orphaned_maps_to_prune(self, monkeypatch) -> None:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        failure = DetectedFailure(
            failure_id=1,
            kind="orphaned_baseline",
            signature_hash="abc",
            signature={"run_id": 7, "orphaned_count": 42},
        )
        result = await propose_remediation(failure)
        assert result.tool_name == "prune_orphaned"
        assert result.tool_input == {"run_id": 7, "expected_count": 42}

    @pytest.mark.asyncio
    async def test_stuck_lock_maps_to_clear(self, monkeypatch) -> None:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        failure = DetectedFailure(
            failure_id=1,
            kind="stuck_advisory_lock",
            signature_hash="def",
            signature={"lock_id": 999},
        )
        result = await propose_remediation(failure)
        assert result.tool_name == "clear_stale_lock"
        assert result.tool_input == {"lock_id": 999}

    @pytest.mark.asyncio
    async def test_stale_watermark_maps_to_reset(self, monkeypatch) -> None:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        failure = DetectedFailure(
            failure_id=1,
            kind="stale_watermark",
            signature_hash="ghi",
            signature={
                "stream_name": "events_stream",
                "current_value": "100",
                "source_latest": "250",
            },
        )
        result = await propose_remediation(failure)
        assert result.tool_name == "reset_watermark"
        assert result.tool_input == {
            "stream_name": "events_stream",
            "to_value": "250",
        }

    @pytest.mark.asyncio
    async def test_force_replay_ignores_env(self, monkeypatch) -> None:
        """force_replay=True bypasses the real API even if a key is set."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fake")
        failure = DetectedFailure(
            failure_id=1,
            kind="orphaned_baseline",
            signature_hash="abc",
            signature={"run_id": 1, "orphaned_count": 5},
        )
        result = await propose_remediation(failure, force_replay=True)
        assert result.tool_name == "prune_orphaned"
        assert result.tool_input == {"run_id": 1, "expected_count": 5}
