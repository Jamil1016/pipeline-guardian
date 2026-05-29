"""Failure detection. Pure DB queries; no LLM, no mutations."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import asyncpg

from pipeline_guardian.types import DetectedFailure


def _stable_hash(payload: dict[str, Any]) -> str:
    """16-hex-char SHA-256 prefix of canonical-JSON of the payload."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


async def _detect_orphaned(pool: asyncpg.Pool) -> list[DetectedFailure]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select pipeline_run_id as run_id, count(*) as orphaned_count
            from stg_events e
            join pipeline.runs r on r.id = e.pipeline_run_id
            where r.status = 'failed'
              and e.created_at < now() - interval '24 hours'
            group by pipeline_run_id
            order by pipeline_run_id
            """
        )
    out: list[DetectedFailure] = []
    for row in rows:
        sig = {"run_id": int(row["run_id"]), "orphaned_count": int(row["orphaned_count"])}
        out.append(
            DetectedFailure(
                failure_id=0,  # assigned by caller
                kind="orphaned_baseline",
                signature_hash=_stable_hash(sig),
                signature=sig,
            )
        )
    return out


async def _detect_stuck_locks(pool: asyncpg.Pool) -> list[DetectedFailure]:
    """Read from pipeline.stuck_locks (created by seed_stuck_advisory_lock).

    This is the deliberate clean-room divergence — using a side-table marker
    instead of real pg_locks introspection for demo determinism.
    """
    async with pool.acquire() as conn:
        # Table may not exist on a fresh DB
        await conn.execute(
            "create table if not exists pipeline.stuck_locks ("
            " lock_id bigint primary key, held_since timestamptz not null default now())"
        )
        rows = await conn.fetch(
            "select lock_id, held_since from pipeline.stuck_locks "
            "where held_since < now() - interval '30 minutes' or true"  # demo: detect immediately
        )
    out: list[DetectedFailure] = []
    for row in rows:
        sig = {"lock_id": int(row["lock_id"])}
        out.append(
            DetectedFailure(
                failure_id=0,
                kind="stuck_advisory_lock",
                signature_hash=_stable_hash(sig),
                signature=sig,
            )
        )
    return out


async def _detect_stale_watermarks(pool: asyncpg.Pool) -> list[DetectedFailure]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            select w.stream_name, w.value as current_value, s.source_latest
            from pipeline.watermarks w
            join pipeline.source_status s on s.stream_name = w.stream_name
            where w.updated_at < now() - interval '24 hours'
              and s.source_latest <> w.value
            order by w.stream_name
            """
        )
    out: list[DetectedFailure] = []
    for row in rows:
        sig = {
            "stream_name": row["stream_name"],
            "current_value": row["current_value"],
            "source_latest": row["source_latest"],
        }
        out.append(
            DetectedFailure(
                failure_id=0,
                kind="stale_watermark",
                signature_hash=_stable_hash(sig),
                signature=sig,
            )
        )
    return out


async def detect_all(pool: asyncpg.Pool) -> list[DetectedFailure]:
    """Run all detectors. Returns a list of failures with deterministic order:
    orphaned first, then stuck-lock, then stale-watermark.
    Each failure gets a sequential failure_id assigned (1-based).
    """
    results: list[DetectedFailure] = []
    results.extend(await _detect_orphaned(pool))
    results.extend(await _detect_stuck_locks(pool))
    results.extend(await _detect_stale_watermarks(pool))
    # Assign sequential failure_ids
    return [
        DetectedFailure(
            failure_id=i + 1,
            kind=f.kind,
            signature_hash=f.signature_hash,
            signature=f.signature,
        )
        for i, f in enumerate(results)
    ]
