import pytest
from httpx import ASGITransport, AsyncClient

from src.application import app
from src.core.database import AsyncSessionLocal, init_db, drop_db
from src.core.queue import TaskQueue

from src.core.config import NUM_WORKERS

EPS_TIME = 3


@pytest.fixture(scope="function")
async def client():
    await init_db()
    app.state.queue = TaskQueue(db_session_factory=AsyncSessionLocal)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.state.queue.shutdown()
    await drop_db()


@pytest.fixture
def anyio_backend():
    return "asyncio"
