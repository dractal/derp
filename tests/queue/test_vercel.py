"""Unit tests for Vercel queue client."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from derp.config import VercelQueueConfig
from derp.queue.base import TaskState
from derp.queue.exceptions import QueueNotConnectedError, QueueProviderError
from derp.queue.vercel import VercelQueueClient


def _config(**overrides: object) -> VercelQueueConfig:
    defaults = {
        "api_token": "tok_test_123",
        "team_id": "team_abc",
        "project_id": "prj_xyz",
        "default_queue": "default",
    }
    defaults.update(overrides)
    return VercelQueueConfig(**defaults)


def _connected_client(
    **config_overrides: object,
) -> tuple[VercelQueueClient, AsyncMock]:
    client = VercelQueueClient(_config(**config_overrides))
    mock_http = AsyncMock()
    client._client = mock_http
    return client, mock_http


# -- connect / disconnect --


@pytest.mark.asyncio
async def test_connect_creates_httpx_client() -> None:
    client = VercelQueueClient(_config())
    assert client._client is None

    with patch("derp.queue.vercel.httpx.AsyncClient") as mock_httpx:
        mock_instance = AsyncMock()
        mock_httpx.return_value = mock_instance

        await client.connect()

        mock_httpx.assert_called_once_with(
            base_url="https://api.vercel.com",
            headers={
                "Authorization": "Bearer tok_test_123",
                "Content-Type": "application/json",
            },
        )
        assert client._client is mock_instance


@pytest.mark.asyncio
async def test_connect_idempotent() -> None:
    client, mock_http = _connected_client()

    await client.connect()

    assert client._client is mock_http


@pytest.mark.asyncio
async def test_disconnect_closes_client() -> None:
    client, mock_http = _connected_client()

    await client.disconnect()

    mock_http.aclose.assert_awaited_once()
    assert client._client is None


@pytest.mark.asyncio
async def test_disconnect_idempotent() -> None:
    client = VercelQueueClient(_config())

    await client.disconnect()  # No error when already disconnected


# -- enqueue --


@pytest.mark.asyncio
async def test_enqueue_basic() -> None:
    client, mock_http = _connected_client()
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_http.post.return_value = mock_response

    task_id = await client.enqueue("send_email", payload={"to": "user@test.com"})

    assert isinstance(task_id, str)
    assert len(task_id) == 32  # uuid4 hex

    mock_http.post.assert_called_once()
    call_args = mock_http.post.call_args
    assert call_args[0][0] == "/v1/queues/default/messages"
    body = call_args[1]["json"]
    assert body["task_name"] == "send_email"
    assert body["payload"] == {"to": "user@test.com"}
    assert body["task_id"] == task_id
    assert "delay_seconds" not in body

    params = call_args[1]["params"]
    assert params["teamId"] == "team_abc"
    assert params["projectId"] == "prj_xyz"


@pytest.mark.asyncio
async def test_enqueue_with_queue_and_delay() -> None:
    client, mock_http = _connected_client()
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_http.post.return_value = mock_response

    await client.enqueue(
        "process_image",
        payload={"url": "https://example.com/img.png"},
        queue="heavy-jobs",
        delay=timedelta(minutes=10),
    )

    call_args = mock_http.post.call_args
    assert call_args[0][0] == "/v1/queues/heavy-jobs/messages"
    body = call_args[1]["json"]
    assert body["delay_seconds"] == 600


@pytest.mark.asyncio
async def test_enqueue_without_team_or_project() -> None:
    client, mock_http = _connected_client(team_id=None, project_id=None)
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_http.post.return_value = mock_response

    await client.enqueue("my_task")

    params = mock_http.post.call_args[1]["params"]
    assert "teamId" not in params
    assert "projectId" not in params


@pytest.mark.asyncio
async def test_enqueue_not_connected() -> None:
    client = VercelQueueClient(_config())

    with pytest.raises(QueueNotConnectedError):
        await client.enqueue("my_task")


@pytest.mark.asyncio
async def test_enqueue_http_error() -> None:
    client, mock_http = _connected_client()

    mock_response = MagicMock()
    mock_response.status_code = 403
    mock_response.text = "Forbidden"

    import httpx

    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Forbidden",
        request=MagicMock(),
        response=mock_response,
    )
    mock_http.post.return_value = mock_response

    with pytest.raises(QueueProviderError, match="Vercel API error"):
        await client.enqueue("my_task")


@pytest.mark.asyncio
async def test_enqueue_connection_error() -> None:
    client, mock_http = _connected_client()
    mock_http.post.side_effect = Exception("connection refused")

    with pytest.raises(QueueProviderError, match="connection refused"):
        await client.enqueue("my_task")


# -- get_status --


@pytest.mark.asyncio
async def test_get_status_always_unknown() -> None:
    client, _ = _connected_client()

    status = await client.get_status("task-abc")

    assert status.task_id == "task-abc"
    assert status.state == TaskState.UNKNOWN
    assert status.result is None
    assert status.error is None


# -- capabilities --


def test_capabilities() -> None:
    client = VercelQueueClient(_config())
    assert client.supports_result is False
    assert client.supports_revoke is False
    assert client.supports_delay is True
