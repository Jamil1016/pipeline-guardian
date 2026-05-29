import pytest

from pipeline_guardian.cli import build_parser, main


class TestCLIParser:
    def test_init_db(self) -> None:
        args = build_parser().parse_args(["init-db"])
        assert args.command == "init-db"

    def test_seed_failure_orphaned(self) -> None:
        args = build_parser().parse_args(["seed-failure", "orphaned"])
        assert args.command == "seed-failure"
        assert args.mode == "orphaned"

    def test_detect(self) -> None:
        args = build_parser().parse_args(["detect"])
        assert args.command == "detect"

    def test_propose_with_failure_id(self) -> None:
        args = build_parser().parse_args(["propose", "--failure-id", "3"])
        assert args.command == "propose"
        assert args.failure_id == 3

    def test_apply_dry_run_default(self) -> None:
        args = build_parser().parse_args(["apply", "--failure-id", "1"])
        assert args.command == "apply"
        assert args.apply is False  # dry-run default

    def test_apply_with_flag(self) -> None:
        args = build_parser().parse_args(["apply", "--failure-id", "1", "--apply"])
        assert args.apply is True

    def test_eval(self) -> None:
        args = build_parser().parse_args(["eval"])
        assert args.command == "eval"


class TestEvalExitCode:
    @pytest.mark.asyncio
    async def test_eval_passes(self) -> None:
        """eval should exit 0 since fixture replay is deterministically 100%."""
        exit_code = await main(["eval"])
        assert exit_code == 0
