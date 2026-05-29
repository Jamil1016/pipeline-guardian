"""Command-line interface."""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any

from dotenv import load_dotenv

from pipeline_guardian.agent import propose_remediation
from pipeline_guardian.apply import apply_remediation
from pipeline_guardian.db import create_pool, init_schema
from pipeline_guardian.detector import detect_all
from pipeline_guardian.evals import run_golden_set
from pipeline_guardian.gate import gate_remediation
from pipeline_guardian.seed import (
    seed_orphaned_baseline,
    seed_stale_watermark,
    seed_stuck_advisory_lock,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline-guardian",
        description="LLM-driven ETL remediation agent with deterministic safety gates.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Apply schema to the configured DATABASE_URL")

    seed = sub.add_parser("seed-failure", help="Inject a synthetic failure")
    seed.add_argument("mode", choices=["orphaned", "stuck-lock", "stale-watermark"])

    sub.add_parser("detect", help="Scan for detected failures and list them")

    propose = sub.add_parser("propose", help="Show the agent's proposed remediation")
    propose.add_argument("--failure-id", type=int, required=True)

    apply_p = sub.add_parser("apply", help="Gate + (optionally) apply a remediation")
    apply_p.add_argument("--failure-id", type=int, required=True)
    apply_p.add_argument(
        "--apply",
        action="store_true",
        help="Actually mutate. WITHOUT this flag, runs in dry-run mode (default).",
    )

    sub.add_parser("eval", help="Run the golden set; exit non-zero if pass rate < 90%%")

    return parser


async def _init_db() -> int:
    pool = await create_pool()
    try:
        await init_schema(pool)
        print("Schema applied.")
        return 0
    finally:
        await pool.close()


async def _seed(mode: str) -> int:
    pool = await create_pool()
    try:
        ctx: dict[str, Any]
        if mode == "orphaned":
            ctx = await seed_orphaned_baseline(pool)
        elif mode == "stuck-lock":
            ctx = await seed_stuck_advisory_lock(pool)
        elif mode == "stale-watermark":
            ctx = await seed_stale_watermark(pool)
        else:
            print(f"Unknown mode: {mode}", file=sys.stderr)
            return 1
        print(f"Seeded {mode} failure: {ctx}")
        return 0
    finally:
        await pool.close()


async def _detect() -> int:
    pool = await create_pool()
    try:
        failures = await detect_all(pool)
        if not failures:
            print("No failures detected.")
            return 0
        for f in failures:
            print(f"  [{f.failure_id}] kind={f.kind}  sig={f.signature}")
        return 0
    finally:
        await pool.close()


async def _propose(failure_id: int) -> int:
    pool = await create_pool()
    try:
        failures = await detect_all(pool)
        match = next((f for f in failures if f.failure_id == failure_id), None)
        if match is None:
            print(f"No detected failure with id {failure_id}", file=sys.stderr)
            return 1
        proposal = await propose_remediation(match)
        print(f"Tool: {proposal.tool_name}")
        print(f"Input: {proposal.tool_input}")
        return 0
    finally:
        await pool.close()


async def _apply(failure_id: int, apply_flag: bool) -> int:
    pool = await create_pool()
    try:
        failures = await detect_all(pool)
        match = next((f for f in failures if f.failure_id == failure_id), None)
        if match is None:
            print(f"No detected failure with id {failure_id}", file=sys.stderr)
            return 1
        proposal = await propose_remediation(match)
        print(f"Proposed: {proposal.tool_name}({proposal.tool_input})")
        gate_result = await gate_remediation(pool, proposal)
        print(f"Gate: {'PASS' if gate_result.passed else 'BLOCK'} — {gate_result.reason}")
        if not gate_result.passed:
            print("Gate refused — not applying.")
            return 1
        if not apply_flag:
            print("Dry-run (use --apply to mutate).")
            return 0
        await apply_remediation(pool, proposal)
        print("Applied.")
        return 0
    finally:
        await pool.close()


async def _eval() -> int:
    results = await run_golden_set()
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    rate = passed / total if total else 0.0
    print(f"Golden set: {passed}/{total} passed ({rate:.0%})")
    for r in results:
        marker = "OK" if r.passed else "FAIL"
        print(f"  [{marker}] {r.case_id}  expected={r.expected_tool}  actual={r.actual_tool}")
    if rate < 0.9:
        print("FAIL: pass rate below 90% threshold", file=sys.stderr)
        return 1
    return 0


async def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = build_parser().parse_args(argv)
    if args.command == "init-db":
        return await _init_db()
    if args.command == "seed-failure":
        return await _seed(args.mode)
    if args.command == "detect":
        return await _detect()
    if args.command == "propose":
        return await _propose(args.failure_id)
    if args.command == "apply":
        return await _apply(args.failure_id, args.apply)
    if args.command == "eval":
        return await _eval()
    return 1


def cli_entry() -> None:
    sys.exit(asyncio.run(main()))
