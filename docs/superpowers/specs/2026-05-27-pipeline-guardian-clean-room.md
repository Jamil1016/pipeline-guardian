# Clean-Room pipeline-guardian — Design Spec

**Date:** 2026-05-27
**Owner:** Jamil Mendez (`Jamil1016` on GitHub)
**Status:** Approved for implementation planning
**Context:** Third of six clean-room reference implementations (Sub-project B.3 of the portfolio expansion). The production version operates privately at the user's employer (auto-remediating Swift API ETL failures); this repo is the open-source pattern-demo on a synthetic ETL pipeline with deliberately mutation-class failure modes, so the safety-gate story has real stakes.

**Cross-references:**
- Portfolio case study: https://portfolio-gules-gamma-14.vercel.app/projects/pipeline-guardian
- First sibling clean-room: https://github.com/Jamil1016/gmail-scraper (input direction)
- Second sibling clean-room: https://github.com/Jamil1016/report-automation (output direction)
- Pattern source: production work on private repo (`jamilmendez-ontel/pipeline-guardian`)

---

## 1. Summary

A working open-source Python implementation of the **"LLM-driven remediation agent with deterministic safety gates"** pattern. A tiny synthetic ETL pipeline is intentionally broken in one of three mutation-class failure modes (orphaned baseline rows, stuck advisory locks, stale watermarks). A detector finds the failure, an LLM agent proposes a structured remediation tool call, a per-tool deterministic gate re-validates the precondition immediately before applying — the LLM is never the last check. A golden-set eval harness catches prompt regressions before deploy. Dry-run is the default; `--apply` is required to mutate.

This is the marquee piece of the portfolio: it demonstrates **how to deploy LLM agents safely in production systems where wrong actions cause real damage**. Most LLM agent demos handwave the safety story; this one builds it explicitly.

---

## 2. Goals & Non-Goals

### Goals
- Runnable end-to-end on a fresh machine: `git clone && docker compose up -d && pip install -e . && python -m pipeline_guardian init-db && seed-failure orphaned && detect && apply --apply`
- Works **without** an `ANTHROPIC_API_KEY` — fixture replay mode delivers the same demo flow
- Works **with** an `ANTHROPIC_API_KEY` — agent calls real Claude API for genuinely-novel inputs (recruiter / engineer testing)
- Demonstrates all four distinctive patterns: **structured tool calls**, **deterministic per-tool safety gates**, **dry-run-by-default**, **golden-set evals**
- Each gate has at least one test that proves the gate **blocks** a bad proposal (negative test, not just happy-path)
- ≥ 10 golden-set cases covering each failure mode + ambiguous/false-positive/negative cases
- pytest suite ≥ 80% coverage on `detector.py`, `runbook.py`, `gate.py`, `apply.py`, `evals.py`
- GitHub Actions CI on the public repo (Tests + Lint) — green badges
- README ≤ 200 lines with Mermaid + quick-start + portfolio backlink
- Sanitization clean — no employer / customer / Swift API / telecom terms anywhere

### Non-Goals (v1)
- Real-time monitoring loop / daemon — CLI-driven only
- External alerts (PagerDuty / Slack / email) — logging only
- Multi-step remediation chains — each failure handled by exactly one tool call
- Cross-failure dependency resolution — each detected failure resolved independently
- Anthropic Batch API for eval runs
- Self-improving / RLHF / fine-tuning loops
- Multi-tenant / multi-database support
- Web UI for inspecting agent reasoning
- Postgres replication / failover handling
- Cost tracking / token accounting in production code (would belong in a separate "ops observability" repo)

---

## 3. Domain — Synthetic ETL pipeline with mutation-class failures

The clean-room repo simulates a small ETL pipeline that ingests events from a source into a staging table and tracks ingestion progress via watermarks. Three failure modes can be intentionally seeded:

### 3.1 Failure mode 1: Orphaned baseline rows

**Scenario:** A prior pipeline run inserted rows into `stg_events` tagged with its `pipeline_run_id`, then crashed before finalizing. The run is marked failed in `pipeline.runs`, but the rows remain. They'll inflate downstream aggregations until cleaned up.

**Detection signature:** Rows in `stg_events` where `pipeline_run_id` references a run with status `failed` AND `created_at` > 24h ago (so we don't clean up rows that might still be in-flight from a current run).

**Remediation:** `prune_orphaned(run_id, expected_count)` — DELETE the orphaned rows.

**Why it needs a gate:** The agent might propose the wrong `run_id`. The gate re-counts the rows at apply time and refuses to proceed if the count doesn't match the agent's `expected_count`. Race condition example: another process restarts the failed run while the agent is thinking; the gate catches that.

### 3.2 Failure mode 2: Stuck advisory lock

**Scenario:** A pipeline component acquired a Postgres advisory lock at the start of a run, then crashed before releasing it. The lock is held by a session ID that's no longer connected. New runs block waiting for the lock.

**Detection signature:** A row in `pg_locks` with `locktype = 'advisory'` held by a `pid` not in `pg_stat_activity`, for > 30 minutes.

**Remediation:** `clear_stale_lock(lock_id)` — call `pg_advisory_unlock(lock_id)` from a fresh session.

**Why it needs a gate:** The agent might pick the wrong `lock_id`. The gate verifies, at apply time, that the named lock is still held by a disconnected pid. If a new process has taken it over legitimately, the gate refuses.

### 3.3 Failure mode 3: Stale watermark

**Scenario:** The pipeline tracks ingestion progress per source stream in `pipeline.watermarks`. If the pipeline gets stuck (data source temporarily unavailable, parsing error, etc.), the watermark stays at the same value while source data advances. Eventually the gap is so large that resuming from the watermark would re-process days of data or miss data entirely.

**Detection signature:** A watermark row where `value` hasn't changed for > 24h AND the source has new data since (encoded as a fact in `pipeline.source_status` table).

**Remediation:** `reset_watermark(stream_name, to_value)` — UPDATE the watermark to a safer value (typically the latest source-acknowledged value minus a safety buffer).

**Why it needs a gate:** This is the highest-risk remediation. The gate verifies the proposed `to_value` is in the range `[current_watermark, source_latest)` — never advancing past what the source has confirmed, never moving backward. If the agent proposes a value outside this range, the gate refuses.

---

## 4. Architecture

```
                  ┌─────────────────────┐
                  │ Postgres (seeded    │
                  │ failing state)      │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ detector.py         │
                  │ scan for known      │
                  │ failure signatures  │
                  └──────────┬──────────┘
                             │ list[DetectedFailure]
                             ▼
                  ┌─────────────────────┐
                  │ agent.py            │
                  │ Claude API (or      │
                  │ fixture replay)     │
                  │ + structured tools  │
                  └──────────┬──────────┘
                             │ ProposedRemediation
                             ▼
                  ┌─────────────────────┐
                  │ gate.py             │
                  │ deterministic       │
                  │ precondition        │
                  │ re-validation       │
                  └──────────┬──────────┘
                             │ GateResult
              ┌──────────────┴──────────────┐
              ▼ pass                        ▼ fail
       ┌────────────┐                ┌────────────┐
       │ apply.py   │                │ log +      │
       │ (dry-run   │                │ alert +    │
       │ unless     │                │ exit non-0 │
       │ --apply)   │                │            │
       └────────────┘                └────────────┘

      evals.py: replays golden_set/cases.jsonl through agent + asserts pass rate
```

### Module responsibilities

- `detector.py` — pure DB queries; returns `list[DetectedFailure]`. No LLM, no mutations.
- `runbook.py` — declarative tool schemas (per-remediation Anthropic tool-use format). Single source of truth.
- `agent.py` — hybrid LLM caller. Either real Claude or fixture replay. Returns `ProposedRemediation`.
- `gate.py` — pure functions taking `(pool, ProposedRemediation)` → `GateResult`. The LLM never sees these.
- `apply.py` — actually performs the mutation. Idempotent where possible. Called only after gate passes.
- `evals.py` — golden-set runner. Reports pass rate, fails CI if below threshold.

Each module is independently testable. The agent never touches the DB directly; the detector and apply do. Tests can swap out individual modules.

---

## 5. Repository Layout

```
pipeline-guardian/                          # Jamil1016/pipeline-guardian
├── pipeline_guardian/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── detector.py
│   ├── runbook.py
│   ├── agent.py
│   ├── gate.py
│   ├── apply.py
│   ├── evals.py
│   ├── db.py
│   ├── schema.sql
│   ├── types.py
│   ├── seed.py                             # seed-failure CLI helper
│   └── fixtures/
│       └── llm_replay.jsonl                # pre-recorded LLM responses
├── golden_set/
│   └── cases.jsonl                         # 10 eval cases
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_detector.py
│   ├── test_runbook.py
│   ├── test_gate.py
│   ├── test_apply.py
│   ├── test_agent_replay.py
│   ├── test_evals.py
│   └── test_cli.py
├── .github/workflows/{test.yml,lint.yml}
├── .env.example
├── .gitignore
├── docker-compose.yml
├── pyproject.toml
├── README.md
└── LICENSE
```

---

## 6. Data Model

```sql
-- pipeline_guardian/schema.sql

-- Schema for pipeline infrastructure tables. Must be created BEFORE the
-- CREATE TABLE statements that reference pipeline.<table>.
create schema if not exists pipeline;

-- A pipeline run with status
create table if not exists pipeline.runs (
  id          bigserial primary key,
  started_at  timestamptz not null,
  ended_at    timestamptz,
  status      text not null check (status in ('running','success','failed'))
);

-- Per-stream watermarks
create table if not exists pipeline.watermarks (
  stream_name   text primary key,
  value         text not null,
  updated_at    timestamptz not null default now()
);

-- Source status (what the source claims is its latest data)
create table if not exists pipeline.source_status (
  stream_name      text primary key,
  source_latest    text not null,
  reported_at      timestamptz not null default now()
);

-- Staging events tied to a run (public schema, mirrors typical raw->staging pattern)
create table if not exists stg_events (
  id                bigserial primary key,
  pipeline_run_id   bigint not null references pipeline.runs(id),
  occurred_at       timestamptz not null,
  payload           jsonb not null default '{}'::jsonb,
  created_at        timestamptz not null default now()
);

create index if not exists stg_events_run_idx on stg_events (pipeline_run_id);
create index if not exists pipeline_runs_status_idx on pipeline.runs (status, started_at);
```

The `pipeline` schema groups infrastructure tables; `stg_events` is the data staging table.

---

## 7. Hybrid LLM Strategy

Three operating modes for `agent.propose_remediation`:

1. **Real Claude** (when `ANTHROPIC_API_KEY` is set): instantiate the Anthropic SDK, send the failure signature + runbook tool definitions + a short system prompt, expect a tool_use response. Parse and return.
2. **Fixture replay** (no key): read `fixtures/llm_replay.jsonl`, find the entry matching the failure's `signature_hash`, return the recorded response.
3. **Eval mode** (used by `evals.py`): always uses fixture replay regardless of env, so eval results are deterministic in CI.

```python
# agent.py (simplified)
async def propose_remediation(
    failure: DetectedFailure,
    *,
    force_replay: bool = False,
) -> ProposedRemediation:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if force_replay or not api_key:
        return _replay_from_fixture(failure)
    return await _call_claude(failure, api_key)
```

### Fixture format (`fixtures/llm_replay.jsonl`)

One JSON object per line:

```jsonc
{
  "signature_hash": "orphaned-baseline-run-123",  // stable hash of the failure signature
  "tool_name": "prune_orphaned",
  "tool_input": {"run_id": 123, "expected_count": 487}
}
```

The fixture file is committed verbatim. Tests that need novel inputs use the `force_replay=True` path with a matched signature.

### Anthropic SDK version pin

Use `anthropic>=0.34` (the version that introduced structured tool-use). Pin in `pyproject.toml` to avoid surprise upgrades.

---

## 8. Safety Gates — The Project's Defining Feature

Each remediation has exactly one gate function in `gate.py`. Gate signature:

```python
async def gate_<remediation>(pool: asyncpg.Pool, args: <ArgsType>) -> GateResult:
    """Re-validate the precondition immediately before applying.

    Returns GateResult(passed=True) only if it's still safe to apply NOW.
    The LLM-proposed action may be stale, wrong, or the world may have changed.
    """
```

### Three gates

**`gate_prune_orphaned`**
- Re-count current orphaned rows for `args.run_id`
- Pass only if count == `args.expected_count`
- Refuse if: count is 0 (already cleaned), count > expected (more orphans appeared — investigate before bulk-delete), or the run's status is no longer `failed` (someone restarted it)

**`gate_clear_stale_lock`**
- Re-query `pg_locks` for `args.lock_id`
- Pass only if: lock is still held AND the holding pid is no longer in `pg_stat_activity`
- Refuse if: lock is no longer held, or it's now held by an active session

**`gate_reset_watermark`**
- Look up current watermark + source_latest for `args.stream_name`
- Pass only if: `current_watermark <= args.to_value <= source_latest`
- Refuse if: proposed value would move backward (data loss), past the source-acknowledged latest (would skip data), or watermark has already been updated by something else

**Each gate has at least one explicit BLOCK test** in `test_gate.py`. The success criteria require this — gates that only have happy-path tests are not gates.

---

## 9. Eval Harness

`evals.py` implements a single function:

```python
@dataclass
class EvalResult:
    case_id: str
    expected_tool: str
    actual_tool: str
    passed: bool

async def run_golden_set(pool: asyncpg.Pool) -> list[EvalResult]:
    """Replay every case in golden_set/cases.jsonl through the agent.

    Uses force_replay=True so results are deterministic in CI.
    """
```

### Golden set (`golden_set/cases.jsonl`)

10 cases at launch:
- 2 per failure mode (variations in signature) = 6
- 1 ambiguous case the agent should escalate by picking the safest tool
- 1 false-positive case (signature looks like a failure but the data is actually correct — agent should refuse)
- 2 negative cases (no detected failure at all — verifies the agent doesn't hallucinate work)

Each case JSON:
```jsonc
{
  "case_id": "orphaned-baseline-001",
  "failure_signature": { /* full DetectedFailure object */ },
  "signature_hash": "orphaned-baseline-001",
  "expected_tool": "prune_orphaned",
  "expected_tool_input": {"run_id": 42, "expected_count": 100}
}
```

### CI gate

`pytest tests/test_evals.py` asserts the pass rate is ≥ 90% (9/10 cases). Below that, the CI build fails. This is what catches prompt regressions before deploy.

---

## 10. CLI Surface

```
python -m pipeline_guardian init-db
python -m pipeline_guardian seed-failure {orphaned|stuck-lock|stale-watermark}
python -m pipeline_guardian detect
python -m pipeline_guardian propose [--failure-id N]
python -m pipeline_guardian apply [--failure-id N] [--apply]
python -m pipeline_guardian eval
python -m pipeline_guardian --help
```

### Subcommand behaviors

- **`init-db`** — apply `schema.sql`
- **`seed-failure <mode>`** — insert the test data that triggers a failure of the named mode
- **`detect`** — scan and list all detected failures with their `failure_id` and `signature_hash`
- **`propose --failure-id N`** — show the agent's proposed tool call without applying
- **`apply --failure-id N`** — DEFAULT IS DRY-RUN. Shows what would happen. Add `--apply` to actually mutate.
- **`eval`** — run the golden set, print pass rate, exit non-zero if below 90%

The `apply` semantics — dry-run by default, explicit flag required to mutate — is **deliberately the opposite of typical Unix idioms**. This is intentional: this tool's job is to gate mutations, so making `--apply` explicit is the safest default.

---

## 11. Testing

| File | Responsibility | Coverage target |
|---|---|---|
| `test_detector.py` | Detector finds each known seeded failure; doesn't false-positive on healthy state | 100% of `detector.py` |
| `test_runbook.py` | Tool schemas match Anthropic's tool-use JSON spec; arg types valid | 100% of `runbook.py` |
| `test_gate.py` | Each gate has at least 1 PASS test and 1 BLOCK test; race conditions covered | ≥ 95% of `gate.py` |
| `test_apply.py` | Each apply mutates exactly the expected rows; idempotent where designed | ≥ 90% of `apply.py` |
| `test_agent_replay.py` | Fixture replay returns expected tool call per signature hash | ≥ 90% of `agent.py` |
| `test_evals.py` | Golden set runs end-to-end, asserts pass rate ≥ 90% | ≥ 95% of `evals.py` |
| `test_cli.py` | argparse + dispatch (mocked DB) | covers `cli.py` |

### Integration tests

`test_detector`, `test_gate`, `test_apply`, `test_evals` use `testcontainers-postgres` (same pattern as gmail-scraper + report-automation).

### `ANTHROPIC_API_KEY` in CI

CI does NOT set this. All eval runs use fixture replay (deterministic + free). The real-API path is exercised manually during development.

---

## 12. CI on the Public Repo

`.github/workflows/test.yml` — pytest + coverage with Postgres service container. No `ANTHROPIC_API_KEY` exposed (fixture replay only).

`.github/workflows/lint.yml` — ruff + mypy.

Both badges in README header. No daily-cron workflow (deferred to v0.2).

---

## 13. README Structure

1. Title + badges (Tests, Lint, License)
2. One-liner + paragraph framing the pattern: "LLM-driven remediation with deterministic safety gates"
3. **Why it exists** — the safety problem: LLMs propose, deterministic code disposes
4. **Architecture** — Mermaid diagram (mirrors §4)
5. **Quick start** — `git clone && docker compose up -d && pip install -e . && init-db && seed-failure orphaned && detect && apply --apply`
6. **The three patterns this repo demonstrates**
   - Structured tool calls (no free-form SQL or shell)
   - Per-tool deterministic safety gates (LLM is never the final authority)
   - Golden-set evals (catch prompt regressions before deploy)
7. **Failure modes** — table of 3 modes + their gates
8. **Without an API key** — explanation of fixture replay mode
9. **Tests** — table per file
10. **Background** — "I built this pattern at $WORK against private ETL infra. This is a clean-room implementation on synthetic data so the architecture is verifiable. Portfolio case study at https://..."
11. **License** — MIT

Target: ≤ 200 lines.

---

## 14. Success Criteria

1. `git clone && docker compose up -d && pip install -e . && python -m pipeline_guardian init-db && seed-failure orphaned && detect && apply --apply` works on a fresh machine **without** `ANTHROPIC_API_KEY` (uses fixture replay)
2. With `ANTHROPIC_API_KEY` set, the same flow works using real Claude API, producing structurally identical results for the seeded failure signatures
3. All 7 test files green via pytest
4. Coverage ≥ 80% on listed modules (detector/runbook/gate/apply/evals)
5. Each gate has at least one test that asserts the gate **blocks** a bad proposal (negative test required)
6. `pytest tests/test_evals.py` passes with golden-set pass rate ≥ 90%
7. GHA Tests + Lint workflows show green badges on README
8. README ≤ 200 lines
9. Sanitization clean — no employer / customer / Swift API / telecom terms anywhere
10. Portfolio case study at `/projects/pipeline-guardian` flips from `publicRepoStatus: "coming"` to `"live"`

---

## 15. Out of Scope (Future Versions)

| Feature | Why deferred |
|---|---|
| Real-time monitoring daemon | Belongs in v0.2; orthogonal to the safety-gate pattern |
| External alerting (Slack/PagerDuty) | Logging is the correct v1 stop |
| Multi-step remediation | Not the pattern this repo demonstrates |
| Anthropic Batch API for evals | Optimization; current eval cost is negligible |
| Self-improving / RLHF | Different category of system |
| Multi-tenant | Not the pattern |
| Web UI for inspecting reasoning | Future portfolio piece, not core to this one |
| Postgres replication awareness | Out of scope for a pattern demo |
| Real cost tracking | Belongs in production-ops repo, not pattern repo |

---

## 16. Open Questions (resolve during implementation)

- **Anthropic SDK version:** target `anthropic>=0.39` (current stable at spec time). Pin exactly to avoid surprise upgrades during the implementation window.
- **Fixture format:** JSONL chosen for line-by-line readability and easy appendability. JSON arrays would also work but make merge conflicts noisier.
- **Coverage badge service:** Codecov (free for OSS) — same as gmail-scraper.
- **`signature_hash` algorithm:** SHA-256 of the canonical-JSON of the failure signature, truncated to 16 hex chars. Same approach as gmail-scraper's message_id.

None of these block the plan.

---

## 17. Why This Spec Exists

This is the third clean-room. The pattern (synthetic-domain repo demonstrating a production pattern, sanitized, dogfooded with CI) is established. What's distinctive here is the **safety-gate story**: this is the only repo where "wrong action = real damage", so this is the only one where the gate pattern earns its keep. Recruiters who only browse one of the six clean-room repos should browse this one — it shows the deepest engineering thinking.
