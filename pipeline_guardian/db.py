"""asyncpg pool + schema application + truncate helpers."""

from __future__ import annotations

import os
from pathlib import Path

import asyncpg

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    return url


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(database_url(), min_size=1, max_size=5)
    assert pool is not None
    return pool


async def init_schema(pool: asyncpg.Pool) -> None:
    sql = _SCHEMA_PATH.read_text()
    async with pool.acquire() as conn:
        await conn.execute(sql)


async def truncate_all(pool: asyncpg.Pool) -> None:
    """Truncate every guardian table. Used by tests for fresh state."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            truncate
              stg_events,
              pipeline.runs,
              pipeline.watermarks,
              pipeline.source_status
            restart identity cascade
            """
        )
