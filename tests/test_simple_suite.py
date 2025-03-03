import asyncio

import pytest

from .conftest import EPS_TIME


@pytest.mark.anyio
async def test_create_mock_task(client):
    response = await client.post("/tasks/mock", json={"delay": 0})
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data


@pytest.mark.anyio
async def test_create_mock_task_and_get_status(client):
    delay = 10
    response = await client.post("/tasks/mock", json={"delay": delay})
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    
    await asyncio.sleep(delay=delay + EPS_TIME)
    
    response = await client.get(f"/tasks/{data['task_id']}")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "Completed"


@pytest.mark.anyio
async def test_get_unknown_task(client):
    response = await client.get("/tasks/1234567890")
    assert response.status_code == 404
