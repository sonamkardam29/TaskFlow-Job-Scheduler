"""Tests for TaskFlow FastAPI REST endpoints using TestClient."""
import pytest
from fastapi import status
from fastapi.testclient import TestClient

from app.log_store import InMemoryLogStore
from app.main import app, app_state, get_log_store, get_repository, get_worker_pool
from app.models import Job, JobState
from app.repository import InMemoryJobRepository
from app.worker import WorkerPool


@pytest.fixture
def test_repo():
    return InMemoryJobRepository()


@pytest.fixture
def test_log_store():
    return InMemoryLogStore()


@pytest.fixture
def client(test_repo, test_log_store):
    worker_pool = WorkerPool(
        repository=test_repo,
        log_store=test_log_store,
        max_workers=2,
    )
    # Override dependencies
    app.dependency_overrides[get_repository] = lambda: test_repo
    app.dependency_overrides[get_log_store] = lambda: test_log_store
    app.dependency_overrides[get_worker_pool] = lambda: worker_pool

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "ok"


def test_create_job(client):
    payload = {
        "name": "data-pipeline",
        "job_type": "echo",
        "payload": {"dataset": "sales_q3.csv"},
        "cron_expression": "0 0 * * *",
        "max_retries": 2,
    }
    response = client.post("/jobs", json=payload)
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == "data-pipeline"
    assert data["job_type"] == "echo"
    assert data["state"] == "QUEUED"
    assert data["max_retries"] == 2
    assert "id" in data


def test_create_job_validation_error(client):
    # Empty name should fail 422
    response = client.post("/jobs", json={"name": "", "job_type": "echo"})
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_get_job_by_id(client, test_repo):
    job = Job(name="report-job", job_type="echo", state=JobState.QUEUED)
    test_repo.save(job)

    response = client.get(f"/jobs/{job.id}")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == job.id
    assert data["name"] == "report-job"


def test_get_job_not_found(client):
    response = client.get("/jobs/nonexistent-id-999")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_list_jobs_and_filtering(client, test_repo):
    j1 = Job(name="job1", job_type="echo", state=JobState.QUEUED)
    j2 = Job(name="job2", job_type="echo", state=JobState.SUCCEEDED)
    j3 = Job(name="job3", job_type="echo", state=JobState.QUEUED)
    test_repo.save(j1)
    test_repo.save(j2)
    test_repo.save(j3)

    # List all
    all_res = client.get("/jobs")
    assert all_res.status_code == status.HTTP_200_OK
    assert len(all_res.json()) == 3

    # Filter by QUEUED
    queued_res = client.get("/jobs?state=QUEUED")
    assert queued_res.status_code == status.HTTP_200_OK
    queued_jobs = queued_res.json()
    assert len(queued_jobs) == 2
    assert all(j["state"] == "QUEUED" for j in queued_jobs)

    # Filter by SUCCEEDED
    succeeded_res = client.get("/jobs?state=SUCCEEDED")
    assert succeeded_res.status_code == status.HTTP_200_OK
    assert len(succeeded_res.json()) == 1


def test_cancel_job(client, test_repo, test_log_store):
    job = Job(name="cancel-me", job_type="echo", state=JobState.QUEUED)
    test_repo.save(job)

    response = client.post(f"/jobs/{job.id}/cancel")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["state"] == "CANCELLED"

    logs = test_log_store.read_log(job.id)
    assert "[CANCELLED]" in logs


def test_cancel_already_finished_job(client, test_repo):
    job = Job(name="finished", job_type="echo", state=JobState.SUCCEEDED)
    test_repo.save(job)

    response = client.post(f"/jobs/{job.id}/cancel")
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Cannot transition job from SUCCEEDED to CANCELLED" in response.json()["detail"]


def test_cancel_nonexistent_job(client):
    response = client.post("/jobs/missing-id/cancel")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_get_job_logs(client, test_repo, test_log_store):
    job = Job(name="logged-job", job_type="echo", state=JobState.SUCCEEDED)
    test_repo.save(job)
    test_log_store.write_log(job.id, "Step 1: start\nStep 2: finish")

    response = client.get(f"/jobs/{job.id}/logs")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["job_id"] == job.id
    assert "Step 1: start" in data["logs"]


def test_get_job_logs_not_found(client):
    response = client.get("/jobs/missing-job/logs")
    assert response.status_code == status.HTTP_404_NOT_FOUND
