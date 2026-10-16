"""Tests for TaskFlow WorkerPool, execution handlers, retries, and concurrency."""
import time
from unittest.mock import MagicMock
import pytest

from app.log_store import InMemoryLogStore
from app.models import Job, JobState
from app.repository import InMemoryJobRepository
from app.state_machine import transition_job
from app.worker import WorkerPool, echo_handler, fail_randomly_handler, sleep_handler


@pytest.fixture
def repo():
    return InMemoryJobRepository()


@pytest.fixture
def log_store():
    return InMemoryLogStore()


def test_echo_job_success(repo, log_store):
    worker = WorkerPool(repository=repo, log_store=log_store, max_workers=2)

    job = Job(name="echo-test", job_type="echo", payload={"message": "TaskFlow rocks!"}, state=JobState.QUEUED)
    repo.save(job)

    # Process queued job
    processed = worker.process_queued_jobs()
    assert processed == 1

    updated = repo.get(job.id)
    assert updated.state == JobState.SUCCEEDED
    assert updated.error_message is None

    logs = log_store.read_log(job.id)
    assert "TaskFlow rocks!" in logs
    assert "[SUCCESS]" in logs


def test_sleep_handler(repo, log_store):
    worker = WorkerPool(repository=repo, log_store=log_store, max_workers=2)

    job = Job(name="sleep-test", job_type="sleep", payload={"seconds": 0.01}, state=JobState.QUEUED)
    repo.save(job)

    worker.process_queued_jobs()

    updated = repo.get(job.id)
    assert updated.state == JobState.SUCCEEDED
    logs = log_store.read_log(job.id)
    assert "Woke up after" in logs


def test_unknown_job_type_fails(repo, log_store):
    mock_sleep = MagicMock()
    worker = WorkerPool(
        repository=repo,
        log_store=log_store,
        max_workers=2,
        sleep_fn=mock_sleep,
    )

    job = Job(name="unknown-type", job_type="nonexistent_type", state=JobState.QUEUED, max_retries=0)
    repo.save(job)

    worker.process_queued_jobs()

    updated = repo.get(job.id)
    assert updated.state == JobState.FAILED
    assert "Unknown job type" in updated.error_message


def test_retry_and_exponential_backoff_logic(repo, log_store):
    delays = []

    def mock_sleep(d: float):
        delays.append(d)

    worker = WorkerPool(
        repository=repo,
        log_store=log_store,
        max_workers=1,
        base_backoff_seconds=1.0,
        sleep_fn=mock_sleep,
    )

    # Create job that fails and has max_retries=3
    job = Job(
        name="retry-test",
        job_type="fail_randomly",
        payload={"should_fail": True, "error_message": "Network timeout"},
        state=JobState.QUEUED,
        max_retries=3,
    )
    repo.save(job)

    # 1st attempt: fails, backoff delay = 1.0 * (2 ** 0) = 1.0s, re-queued
    worker.process_queued_jobs()
    job_1 = repo.get(job.id)
    assert job_1.retry_count == 1
    assert job_1.state == JobState.QUEUED  # Re-queued after backoff
    assert delays == [1.0]

    # 2nd attempt: fails, backoff delay = 1.0 * (2 ** 1) = 2.0s, re-queued
    worker.process_queued_jobs()
    job_2 = repo.get(job.id)
    assert job_2.retry_count == 2
    assert job_2.state == JobState.QUEUED
    assert delays == [1.0, 2.0]

    # 3rd attempt: fails, backoff delay = 1.0 * (2 ** 2) = 4.0s, re-queued
    worker.process_queued_jobs()
    job_3 = repo.get(job.id)
    assert job_3.retry_count == 3
    assert job_3.state == JobState.QUEUED
    assert delays == [1.0, 2.0, 4.0]

    # 4th attempt: fails, retries exhausted -> stays FAILED
    worker.process_queued_jobs()
    job_4 = repo.get(job.id)
    assert job_4.retry_count == 3
    assert job_4.state == JobState.FAILED
    assert "Network timeout" in job_4.error_message
    assert len(delays) == 3  # No 4th delay because retries are exhausted

    logs = log_store.read_log(job.id)
    assert "[FAILED] Job exhausted all 3 retries" in logs


def test_concurrency_no_duplicate_job_execution(repo, log_store):
    """Ensure that with multiple concurrent worker threads, no job executes twice."""
    execution_counter = {}
    import threading
    counter_lock = threading.Lock()

    def counting_handler(payload: dict, store, jid: str) -> str:
        with counter_lock:
            execution_counter[jid] = execution_counter.get(jid, 0) + 1
        time.sleep(0.01)  # small pause to simulate work
        return "done"

    worker = WorkerPool(
        repository=repo,
        log_store=log_store,
        max_workers=8,
        poll_interval_seconds=0.05,
        handlers={"counter": counting_handler},
    )

    num_jobs = 30
    for i in range(num_jobs):
        job = Job(name=f"concurrent-job-{i}", job_type="counter", state=JobState.QUEUED)
        repo.save(job)

    worker.start()

    # Wait until all jobs complete
    timeout = 10.0
    start = time.time()
    while time.time() - start < timeout:
        jobs = repo.list_jobs()
        all_succeeded = all(j.state == JobState.SUCCEEDED for j in jobs)
        if all_succeeded:
            break
        time.sleep(0.1)

    worker.stop(wait=True)

    all_jobs = repo.list_jobs()
    assert len(all_jobs) == num_jobs
    for j in all_jobs:
        assert j.state == JobState.SUCCEEDED
        # Exactly one execution per job!
        assert execution_counter.get(j.id) == 1


def test_cancel_behaviour(repo, log_store):
    # Test cancelling a job in CREATED or QUEUED state
    job = Job(name="to-cancel", job_type="echo", state=JobState.QUEUED)
    repo.save(job)

    # Cancel the job
    transition_job(job, JobState.CANCELLED)
    repo.update(job)

    assert repo.get(job.id).state == JobState.CANCELLED

    # When worker processes queued jobs, it should not acquire it
    worker = WorkerPool(repository=repo, log_store=log_store)
    processed = worker.process_queued_jobs()
    assert processed == 0
    assert repo.get(job.id).state == JobState.CANCELLED


def test_worker_start_stop_idempotency(repo, log_store):
    worker = WorkerPool(repository=repo, log_store=log_store, max_workers=2)
    # Start worker
    worker.start()
    # Starting again should be a no-op
    worker.start()

    # Stop worker
    worker.stop(wait=True)
    # Stopping again should be safe
    worker.stop(wait=True)
