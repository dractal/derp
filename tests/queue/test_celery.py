"""Unit tests for Celery queue client."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from derp.config import CeleryConfig
from derp.queue.base import TaskState
from derp.queue.celery import CeleryQueueClient
from derp.queue.exceptions import QueueNotConnectedError, QueueProviderError


def _config() -> CeleryConfig:
    return CeleryConfig(
        broker_url="redis://localhost:6379/0",
        result_backend="redis://localhost:6379/0",
        task_default_queue="default",
    )


def _connected_client() -> tuple[CeleryQueueClient, MagicMock]:
    client = CeleryQueueClient(_config())
    fake_app = MagicMock()
    client._app = fake_app
    return client, fake_app


# -- connect / disconnect --


@pytest.mark.asyncio
async def test_connect_creates_celery_app() -> None:
    client = CeleryQueueClient(_config())
    assert client._app is None

    with patch("derp.queue.celery.celery.Celery") as mock_celery:
        mock_app = MagicMock()
        mock_celery.return_value = mock_app

        await client.connect()

        mock_celery.assert_called_once_with("derp")
        mock_app.conf.update.assert_called_once_with(
            broker_url="redis://localhost:6379/0",
            result_backend="redis://localhost:6379/0",
            task_serializer="json",
            result_serializer="json",
            task_default_queue="default",
        )


@pytest.mark.asyncio
async def test_connect_idempotent() -> None:
    client, fake_app = _connected_client()

    await client.connect()

    assert client._app is fake_app


@pytest.mark.asyncio
async def test_disconnect_clears_app() -> None:
    client, fake_app = _connected_client()

    await client.disconnect()

    fake_app.close.assert_called_once()
    assert client._app is None


@pytest.mark.asyncio
async def test_disconnect_idempotent() -> None:
    client = CeleryQueueClient(_config())

    await client.disconnect()  # No error when already disconnected


# -- app property --


def test_app_property_creates_app_lazily() -> None:
    client = CeleryQueueClient(_config())

    with patch("derp.queue.celery.celery.Celery") as mock_celery:
        mock_app = MagicMock()
        mock_celery.return_value = mock_app

        app = client.app

        assert app is mock_app
        mock_celery.assert_called_once_with("derp")


def test_app_property_returns_existing() -> None:
    client, fake_app = _connected_client()

    assert client.app is fake_app


# -- enqueue --


@pytest.mark.asyncio
async def test_enqueue_basic() -> None:
    client, fake_app = _connected_client()
    fake_result = SimpleNamespace(id="task-123")
    fake_app.send_task.return_value = fake_result

    task_id = await client.enqueue("my_task", payload={"key": "value"})

    assert task_id == "task-123"
    fake_app.send_task.assert_called_once_with(
        "my_task",
        kwargs={"key": "value"},
    )


@pytest.mark.asyncio
async def test_enqueue_with_queue_and_delay() -> None:
    client, fake_app = _connected_client()
    fake_result = SimpleNamespace(id="task-456")
    fake_app.send_task.return_value = fake_result

    task_id = await client.enqueue(
        "my_task",
        payload={"x": 1},
        queue="high-priority",
        delay=timedelta(minutes=5),
    )

    assert task_id == "task-456"
    fake_app.send_task.assert_called_once_with(
        "my_task",
        kwargs={"x": 1},
        queue="high-priority",
        countdown=300.0,
    )


@pytest.mark.asyncio
async def test_enqueue_not_connected() -> None:
    client = CeleryQueueClient(_config())

    with pytest.raises(QueueNotConnectedError):
        await client.enqueue("my_task")


@pytest.mark.asyncio
async def test_enqueue_provider_error() -> None:
    client, fake_app = _connected_client()
    fake_app.send_task.side_effect = Exception("broker down")

    with pytest.raises(QueueProviderError, match="broker down"):
        await client.enqueue("my_task")


# -- get_status --


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("celery_state", "expected_state"),
    [
        ("PENDING", TaskState.PENDING),
        ("STARTED", TaskState.STARTED),
        ("SUCCESS", TaskState.SUCCESS),
        ("FAILURE", TaskState.FAILURE),
        ("REVOKED", TaskState.REVOKED),
        ("RETRY", TaskState.STARTED),
        ("RECEIVED", TaskState.PENDING),
    ],
)
async def test_get_status_state_mapping(
    celery_state: str, expected_state: TaskState
) -> None:
    client, fake_app = _connected_client()

    mock_result = MagicMock()
    mock_result.state = celery_state
    mock_result.result = "ok" if celery_state == "SUCCESS" else None

    with patch("derp.queue.celery.celery_result.AsyncResult", return_value=mock_result):
        status = await client.get_status("task-abc")

    assert status.task_id == "task-abc"
    assert status.state == expected_state


@pytest.mark.asyncio
async def test_get_status_success_includes_result() -> None:
    client, fake_app = _connected_client()

    mock_result = MagicMock()
    mock_result.state = "SUCCESS"
    mock_result.result = {"data": 42}

    with patch("derp.queue.celery.celery_result.AsyncResult", return_value=mock_result):
        status = await client.get_status("task-xyz")

    assert status.result == {"data": 42}
    assert status.error is None


@pytest.mark.asyncio
async def test_get_status_failure_includes_error() -> None:
    client, fake_app = _connected_client()

    mock_result = MagicMock()
    mock_result.state = "FAILURE"
    mock_result.result = ValueError("bad input")

    with patch("derp.queue.celery.celery_result.AsyncResult", return_value=mock_result):
        status = await client.get_status("task-fail")

    assert status.state == TaskState.FAILURE
    assert status.error == "bad input"
    assert status.result is None


@pytest.mark.asyncio
async def test_get_status_not_connected() -> None:
    client = CeleryQueueClient(_config())

    with pytest.raises(QueueNotConnectedError):
        await client.get_status("task-abc")


@pytest.mark.asyncio
async def test_get_status_unknown_state() -> None:
    client, fake_app = _connected_client()

    mock_result = MagicMock()
    mock_result.state = "SOME_CUSTOM_STATE"
    mock_result.result = None

    with patch("derp.queue.celery.celery_result.AsyncResult", return_value=mock_result):
        status = await client.get_status("task-custom")

    assert status.state == TaskState.UNKNOWN


# -- capabilities --


def test_capabilities() -> None:
    client = CeleryQueueClient(_config())
    assert client.supports_result is True
    assert client.supports_revoke is True
    assert client.supports_delay is True
