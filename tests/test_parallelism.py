import asyncio
import random

import pytest

from .conftest import EPS_TIME


@pytest.mark.anyio
async def test_parallel_task_execution(client):
    delays = [random.randint(10, 20) for _ in range(4)]

    post_responses = await asyncio.gather(
        *[client.post("/tasks/mock", json={"delay": delay}) for delay in delays]
    )
    task_ids = [resp.json()["task_id"] for resp in post_responses]

    await asyncio.sleep(max(delays) + EPS_TIME)

    get_responses = await asyncio.gather(
        *[client.get(f"/tasks/{task_id}") for task_id in task_ids]
    )

    for resp in get_responses:
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ["Completed", "Run"]

    await asyncio.sleep(max(delays) + EPS_TIME)

    for resp in get_responses:
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "Completed"
