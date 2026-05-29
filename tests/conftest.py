import asyncio
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    if os.environ.get("SKIP_INTEGRATION_TESTS") == "1":
        pytest.skip("integration tests skipped")
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        os.environ["DATABASE_URL"] = url  # so production code that reads env works
        yield url


@pytest_asyncio.fixture(scope="function")
async def db_pool(postgres_url: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=3)
    assert pool is not None
    schema_sql = Path("pipeline_guardian/schema.sql").read_text()
    async with pool.acquire() as conn:
        await conn.execute("drop schema if exists pipeline cascade")
        await conn.execute("drop table if exists stg_events cascade")
        await conn.execute(schema_sql)
    try:
        yield pool
    finally:
        await pool.close()
