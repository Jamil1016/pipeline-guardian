"""Failure-mode seed helpers — deterministically inject one of three failure modes
into the DB. Called by the CLI's `seed-failure` subcommand and by tests.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg


async def seed_orphaned_baseline(pool: asyncpg.Pool) -> dict[str, int]:
    """Insert a failed run + 100 orphaned rows in stg_events more than 24h old.

    Returns the failure context: {"run_id": int, "orphaned_count": int}.
    """
    now = datetime.now(UTC)
    old = now - timedelta(hours=36)

    async with pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            insert into pipeline.runs (started_at, ended_at, status)
            values ($1, $2, 'failed')
            returning id
            """,
            old,
            old + timedelta(minutes=5),
        )

        rows = [(run_id, old + timedelta(seconds=i * 30), "{}") for i in range(100)]
        await conn.executemany(
            """
            insert into stg_events (pipeline_run_id, occurred_at, payload, created_at)
            values ($1, $2, $3::jsonb, $2)
            """,
            rows,
        )

    return {"run_id": int(run_id), "orphaned_count": 100}


async def seed_stuck_advisory_lock(pool: asyncpg.Pool) -> dict[str, int]:
    """Acquire an advisory lock on a known key from a connection we then DROP.

    The lock survives on the server side; pg_locks shows it as held by a
    disconnected pid. Returns {"lock_id": int}.

    Note: real pg_advisory_unlock/pg_terminate_backend semantics are messy for a
    self-contained demo. The pragmatic clean-room approach is to use a side
    table pipeline.stuck_locks as the source of truth for "what locks are stuck"
    — the detector reads from it, the apply releases from it. This is documented
    in the README as the only deliberate divergence from real Postgres advisory-
    lock mechanics, for demo determinism.
    """
    LOCK_ID = 4242424242

    async with pool.acquire() as conn:
        await conn.execute(
            "create table if not exists pipeline.stuck_locks ("
            " lock_id bigint primary key,"
            " held_since timestamptz not null default now())"
        )
        await conn.execute(
            "insert into pipeline.stuck_locks (lock_id) values ($1) on conflict do nothing",
            LOCK_ID,
        )

    return {"lock_id": LOCK_ID}


async def seed_stale_watermark(pool: asyncpg.Pool) -> dict[str, str]:
    """Insert a watermark that hasn't moved in 36h while source has advanced.

    Returns {"stream_name": str, "current_value": str, "source_latest": str}.
    """
    stream = "events_stream"
    old = datetime.now(UTC) - timedelta(hours=36)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            insert into pipeline.watermarks (stream_name, value, updated_at)
            values ($1, '100', $2)
            on conflict (stream_name) do update
              set value = excluded.value, updated_at = excluded.updated_at
            """,
            stream,
            old,
        )
        await conn.execute(
            """
            insert into pipeline.source_status (stream_name, source_latest, reported_at)
            values ($1, '250', now())
            on conflict (stream_name) do update
              set source_latest = excluded.source_latest, reported_at = excluded.reported_at
            """,
            stream,
        )

    return {"stream_name": stream, "current_value": "100", "source_latest": "250"}
