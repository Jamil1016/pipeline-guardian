"""Anthropic tool-use schemas for the three remediations.

These schemas are passed verbatim to `anthropic.Messages.create(tools=...)`.
Single source of truth for what the LLM is allowed to propose.
"""

from __future__ import annotations

from typing import Any

TOOLS: list[dict[str, Any]] = [
    {
        "name": "prune_orphaned",
        "description": (
            "Delete orphaned baseline rows in stg_events that are tagged with "
            "a failed pipeline_run_id and older than 24 hours. Provide the "
            "exact run_id and expected_count from the detected failure."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {
                    "type": "integer",
                    "description": "ID of the failed run whose orphaned rows should be pruned.",
                },
                "expected_count": {
                    "type": "integer",
                    "description": "Exact number of orphaned rows currently in the table.",
                },
            },
            "required": ["run_id", "expected_count"],
        },
    },
    {
        "name": "clear_stale_lock",
        "description": (
            "Release a stuck advisory lock that was acquired by a now-disconnected "
            "session. Provide the lock_id from the detected failure."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "lock_id": {
                    "type": "integer",
                    "description": "The advisory lock id to release.",
                },
            },
            "required": ["lock_id"],
        },
    },
    {
        "name": "reset_watermark",
        "description": (
            "Update a stale pipeline watermark to a safer value. The new value "
            "must be >= current watermark and <= source_latest. Never move backward."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "stream_name": {
                    "type": "string",
                    "description": "Stream whose watermark to reset.",
                },
                "to_value": {
                    "type": "string",
                    "description": "New watermark value. Must be in [current_value, source_latest].",
                },
            },
            "required": ["stream_name", "to_value"],
        },
    },
]


def tool_for(name: str) -> dict[str, Any]:
    """Return the tool schema by name. Raises KeyError if unknown."""
    for t in TOOLS:
        if t["name"] == name:
            return t
    raise KeyError(f"Unknown tool: {name}")
