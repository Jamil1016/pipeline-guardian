import pytest

from pipeline_guardian.evals import run_golden_set
from pipeline_guardian.types import EvalResult


class TestEvalHarness:
    @pytest.mark.asyncio
    async def test_runs_all_cases(self) -> None:
        results = await run_golden_set()
        assert len(results) == 10
        for r in results:
            assert isinstance(r, EvalResult)
            assert r.case_id
            assert r.expected_tool
            assert r.actual_tool

    @pytest.mark.asyncio
    async def test_pass_rate_at_least_90_pct(self) -> None:
        """The defining CI gate — fixture replay should pass ≥90% of cases."""
        results = await run_golden_set()
        passed = sum(1 for r in results if r.passed)
        pass_rate = passed / len(results)
        assert pass_rate >= 0.9, (
            f"pass rate {pass_rate:.0%} below 90% threshold; "
            f"failed: {[r.case_id for r in results if not r.passed]}"
        )
