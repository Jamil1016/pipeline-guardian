-- Schema for pipeline infrastructure tables. Must be created BEFORE the
-- CREATE TABLE statements that reference pipeline.<table>.
create schema if not exists pipeline;

create table if not exists pipeline.runs (
  id          bigserial primary key,
  started_at  timestamptz not null,
  ended_at    timestamptz,
  status      text not null check (status in ('running','success','failed'))
);

create table if not exists pipeline.watermarks (
  stream_name   text primary key,
  value         text not null,
  updated_at    timestamptz not null default now()
);

create table if not exists pipeline.source_status (
  stream_name      text primary key,
  source_latest    text not null,
  reported_at      timestamptz not null default now()
);

create table if not exists stg_events (
  id                bigserial primary key,
  pipeline_run_id   bigint not null references pipeline.runs(id),
  occurred_at       timestamptz not null,
  payload           jsonb not null default '{}'::jsonb,
  created_at        timestamptz not null default now()
);

create index if not exists stg_events_run_idx on stg_events (pipeline_run_id);
create index if not exists pipeline_runs_status_idx on pipeline.runs (status, started_at);
