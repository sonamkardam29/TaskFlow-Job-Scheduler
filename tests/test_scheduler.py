"""Tests for TaskFlow JobScheduler recurring jobs."""
from datetime import datetime, timezone
import pytest

from app.log_store import InMemoryLogStore
from app.models import Job, JobState
from app.repository import InMemoryJobRepository
from app.scheduler import JobScheduler, is_valid_cron


@pytest.fixture
def repo():
    return InMemoryJobRepository()


@pytest.fixture
def log_store():
    return InMemoryLogStore()


@pytest.fixture
def scheduler(repo, log_store):
    return JobScheduler(repository=repo, log_store=log_store)


def test_is_valid_cron():
    assert is_valid_cron("*/5 * * * *") is True
    assert is_valid_cron("0 12 * * 1-5") is True
    assert is_valid_cron("invalid cron expression") is False
    assert is_valid_cron("") is False


def test_schedule_job_success(scheduler, repo):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    job = Job(name="cron-job", job_type="echo", cron_expression="*/10 * * * *")
    repo.save(job)

    assert scheduler.schedule_job(job, base_time=base_time) is True
    assert scheduler.is_scheduled(job.id) is True


def test_schedule_job_invalid_cron(scheduler, repo):
    job_no_cron = Job(name="no-cron", job_type="echo", cron_expression=None)
    assert scheduler.schedule_job(job_no_cron) is False

    job_bad_cron = Job(name="bad-cron", job_type="echo", cron_expression="not a cron")
    assert scheduler.schedule_job(job_bad_cron) is False


def test_scheduler_tick_requeues_job_when_due(scheduler, repo, log_store):
    # Scheduled at 12:00:00 for every minute: "*/1 * * * *"
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    job = Job(name="recurring-backup", job_type="echo", cron_expression="*/1 * * * *", state=JobState.SUCCEEDED)
    repo.save(job)

    scheduler.schedule_job(job, base_time=base_time)

    # At 12:00:30, it is not due yet
    not_due_time = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
    triggered = scheduler.tick(current_time=not_due_time)
    assert len(triggered) == 0
    assert repo.get(job.id).state == JobState.SUCCEEDED

    # At 12:01:00, it becomes due!
    due_time = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    triggered = scheduler.tick(current_time=due_time)
    assert triggered == [job.id]

    updated_job = repo.get(job.id)
    assert updated_job.state == JobState.QUEUED
    assert updated_job.retry_count == 0

    logs = log_store.read_log(job.id)
    assert "[SCHEDULER] Job re-queued on cron schedule" in logs


def test_scheduler_does_not_requeue_running_job(scheduler, repo):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    job = Job(name="long-job", job_type="sleep", cron_expression="*/1 * * * *", state=JobState.RUNNING)
    repo.save(job)

    scheduler.schedule_job(job, base_time=base_time)

    # Even when due, a running job should not be forced into QUEUED
    due_time = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    triggered = scheduler.tick(current_time=due_time)
    assert len(triggered) == 0
    assert repo.get(job.id).state == JobState.RUNNING


def test_scheduler_removes_cancelled_job(scheduler, repo):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    job = Job(name="cancelled-cron", job_type="echo", cron_expression="*/1 * * * *", state=JobState.CANCELLED)
    repo.save(job)

    scheduler.schedule_job(job, base_time=base_time)

    due_time = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    triggered = scheduler.tick(current_time=due_time)
    assert len(triggered) == 0
    assert scheduler.is_scheduled(job.id) is False


def test_scheduler_manual_remove(scheduler, repo):
    job = Job(name="to-remove", job_type="echo", cron_expression="0 * * * *")
    repo.save(job)
    scheduler.schedule_job(job)
    assert scheduler.is_scheduled(job.id) is True

    scheduler.remove_job(job.id)
    assert scheduler.is_scheduled(job.id) is False


def test_scheduler_start_stop(scheduler):
    scheduler.start()
    assert scheduler._running is True
    # Idempotent start
    scheduler.start()
    scheduler.stop()
    assert scheduler._running is False
    # Idempotent stop
    scheduler.stop()
