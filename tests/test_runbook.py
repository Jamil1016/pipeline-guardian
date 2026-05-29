import pytest

from pipeline_guardian.runbook import TOOLS, tool_for


class TestRunbookTools:
    def test_exactly_three_tools(self) -> None:
        assert len(TOOLS) == 3

    def test_tool_names_match_remediation_set(self) -> None:
        names = {t["name"] for t in TOOLS}
        assert names == {"prune_orphaned", "clear_stale_lock", "reset_watermark"}

    def test_each_tool_has_required_keys(self) -> None:
        for t in TOOLS:
            assert "name" in t
            assert "description" in t
            assert "input_schema" in t
            assert t["input_schema"]["type"] == "object"
            assert "properties" in t["input_schema"]
            assert "required" in t["input_schema"]

    def test_prune_orphaned_schema(self) -> None:
        t = tool_for("prune_orphaned")
        props = t["input_schema"]["properties"]
        assert "run_id" in props and props["run_id"]["type"] == "integer"
        assert "expected_count" in props and props["expected_count"]["type"] == "integer"
        assert set(t["input_schema"]["required"]) == {"run_id", "expected_count"}

    def test_clear_stale_lock_schema(self) -> None:
        t = tool_for("clear_stale_lock")
        props = t["input_schema"]["properties"]
        assert "lock_id" in props and props["lock_id"]["type"] == "integer"
        assert set(t["input_schema"]["required"]) == {"lock_id"}

    def test_reset_watermark_schema(self) -> None:
        t = tool_for("reset_watermark")
        props = t["input_schema"]["properties"]
        assert "stream_name" in props and props["stream_name"]["type"] == "string"
        assert "to_value" in props and props["to_value"]["type"] == "string"
        assert set(t["input_schema"]["required"]) == {"stream_name", "to_value"}

    def test_tool_for_unknown_raises(self) -> None:
        with pytest.raises(KeyError):
            tool_for("not_a_tool")
