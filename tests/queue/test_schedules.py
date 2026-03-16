"""Tests for scheduled job support."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from derp.config import (
    CeleryConfig,
    QueueConfig,
    ScheduleConfig,
    VercelQueueConfig,
)
from derp.queue.base import Schedule, ScheduleType
from derp.queue.celery import CeleryQueueClient
from derp.queue.exceptions import QueueProviderError
from derp.queue.vercel import VercelQueueClient

# =============================================================================
# Schedule dataclass
# =============================================================================


class TestSchedule:
    def test_cron_schedule(self):
        s = Schedule(
            name="cleanup",
            task="tasks.cleanup",
            type=ScheduleType.CRON,
            cron="0 3 * * *",
        )
        assert s.name == "cleanup"
        assert s.type == ScheduleType.CRON
        assert s.cron == "0 3 * * *"
        assert s.interval is None

    def test_interval_schedule(self):
        s = Schedule(
            name="heartbeat",
            task="tasks.heartbeat",
            type=ScheduleType.INTERVAL,
            interval=timedelta(minutes=5),
        )
        assert s.type == ScheduleType.INTERVAL
        assert s.interval == timedelta(minutes=5)
        assert s.cron is None

    def test_schedule_with_payload(self):
        s = Schedule(
            name="sync",
            task="tasks.sync",
            type=ScheduleType.CRON,
            cron="*/10 * * * *",
            payload={"full": False},
        )
        assert s.payload == {"full": False}

    def test_schedule_with_queue(self):
        s = Schedule(
            name="heavy",
            task="tasks.heavy",
            type=ScheduleType.INTERVAL,
            interval=timedelta(hours=1),
            queue="high-priority",
        )
        assert s.queue == "high-priority"


# =============================================================================
# ScheduleConfig validation
# =============================================================================


class TestScheduleConfig:
    def test_cron_config(self):
        sc = ScheduleConfig(name="a", task="tasks.a", cron="0 3 * * *")
        assert sc.cron == "0 3 * * *"
        assert sc.interval_seconds is None

    def test_interval_config(self):
        sc = ScheduleConfig(name="a", task="tasks.a", interval_seconds=300)
        assert sc.interval_seconds == 300
        assert sc.cron is None

    def test_rejects_both_cron_and_interval(self):
        with pytest.raises(ValueError, match="cron.*interval_seconds"):
            ScheduleConfig(
                name="bad",
                task="tasks.bad",
                cron="0 * * * *",
                interval_seconds=60,
            )

    def test_rejects_neither_cron_nor_interval(self):
        with pytest.raises(ValueError, match="cron.*interval_seconds"):
            ScheduleConfig(name="bad", task="tasks.bad")


# =============================================================================
# QueueConfig with schedules
# =============================================================================


class TestQueueConfigSchedules:
    def test_config_with_schedules(self):
        config = QueueConfig(
            celery=CeleryConfig(broker_url="redis://localhost"),
            schedules=[
                ScheduleConfig(name="cleanup", task="tasks.cleanup", cron="0 3 * * *"),
            ],
        )
        assert len(config.schedules) == 1
        assert config.schedules[0].name == "cleanup"

    def test_config_empty_schedules(self):
        config = QueueConfig(
            celery=CeleryConfig(broker_url="redis://localhost"),
        )
        assert config.schedules == ()


# =============================================================================
# Celery Beat integration
# =============================================================================


class TestCelerySchedules:
    def _client(self) -> tuple[CeleryQueueClient, MagicMock]:
        config = CeleryConfig(broker_url="redis://localhost")
        client = CeleryQueueClient(config)
        fake_app = MagicMock()
        fake_app.conf = MagicMock()
        client._app = fake_app
        return client, fake_app

    def test_register_cron_schedule(self):
        client, app = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="cleanup",
                    task="tasks.cleanup",
                    type=ScheduleType.CRON,
                    cron="0 3 * * *",
                ),
            ]
        )
        beat = app.conf.beat_schedule
        assert "cleanup" in beat
        assert beat["cleanup"]["task"] == "tasks.cleanup"
        assert "schedule" in beat["cleanup"]

    def test_register_interval_schedule(self):
        client, app = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="heartbeat",
                    task="tasks.heartbeat",
                    type=ScheduleType.INTERVAL,
                    interval=timedelta(minutes=5),
                ),
            ]
        )
        beat = app.conf.beat_schedule
        assert "heartbeat" in beat
        assert beat["heartbeat"]["schedule"] == 300.0

    def test_register_schedule_with_payload(self):
        client, app = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="sync",
                    task="tasks.sync",
                    type=ScheduleType.CRON,
                    cron="*/10 * * * *",
                    payload={"full": False},
                ),
            ]
        )
        beat = app.conf.beat_schedule
        assert beat["sync"]["kwargs"] == {"full": False}

    def test_register_schedule_with_queue(self):
        client, app = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="heavy",
                    task="tasks.heavy",
                    type=ScheduleType.INTERVAL,
                    interval=timedelta(hours=1),
                    queue="high-priority",
                ),
            ]
        )
        beat = app.conf.beat_schedule
        assert beat["heavy"]["options"] == {"queue": "high-priority"}

    def test_register_multiple_schedules(self):
        client, app = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="a",
                    task="tasks.a",
                    type=ScheduleType.CRON,
                    cron="0 * * * *",
                ),
                Schedule(
                    name="b",
                    task="tasks.b",
                    type=ScheduleType.INTERVAL,
                    interval=timedelta(seconds=30),
                ),
            ]
        )
        beat = app.conf.beat_schedule
        assert "a" in beat
        assert "b" in beat

    def test_get_schedules(self):
        client, _ = self._client()
        schedules = [
            Schedule(
                name="a",
                task="tasks.a",
                type=ScheduleType.CRON,
                cron="0 * * * *",
            ),
        ]
        client.register_schedules(schedules)
        assert client.get_schedules() == schedules


# =============================================================================
# Vercel cron integration
# =============================================================================


class TestVercelSchedules:
    def _client(self) -> VercelQueueClient:
        config = VercelQueueConfig(api_token="test-token")
        return VercelQueueClient(config)

    def test_register_cron_schedule(self):
        client = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="cleanup",
                    task="tasks.cleanup",
                    type=ScheduleType.CRON,
                    cron="0 3 * * *",
                ),
            ]
        )
        assert len(client.get_schedules()) == 1

    def test_register_interval_rejects(self):
        client = self._client()
        with pytest.raises(QueueProviderError, match="cron expressions"):
            client.register_schedules(
                [
                    Schedule(
                        name="heartbeat",
                        task="tasks.heartbeat",
                        type=ScheduleType.INTERVAL,
                        interval=timedelta(minutes=5),
                    ),
                ]
            )

    def test_generate_vercel_cron_config(self):
        client = self._client()
        client.register_schedules(
            [
                Schedule(
                    name="cleanup",
                    task="tasks.cleanup",
                    type=ScheduleType.CRON,
                    cron="0 3 * * *",
                ),
                Schedule(
                    name="sync",
                    task="tasks.sync",
                    type=ScheduleType.CRON,
                    cron="*/10 * * * *",
                    path="/api/custom-sync",
                ),
            ]
        )
        crons = client.generate_vercel_cron_config()
        assert len(crons) == 2
        assert crons[0] == {
            "path": "/api/cron/cleanup",
            "schedule": "0 3 * * *",
        }
        assert crons[1] == {
            "path": "/api/custom-sync",
            "schedule": "*/10 * * * *",
        }
