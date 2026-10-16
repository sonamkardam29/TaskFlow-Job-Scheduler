"""Tests for TaskFlow data models."""
from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from app.models import Job, JobCreate, JobResponse, JobState, utc_now


def test_job_default_values():
    job = Job(name="test-job", job_type="echo")
    assert job.id is not None
    assert job.name == "test-job"
    assert job.job_type == "echo"
    assert job.state == JobState.CREATED
    assert job.retry_count == 0
    assert job.max_retries == 3
    assert job.error_message is None
    assert job.cron_expression is None
    assert isinstance(job.created_at, datetime)
    assert isinstance(job.updated_at, datetime)


def test_job_serialization_roundtrip():
    original = Job(
        id="job-123",
        name="email-summary",
        job_type="send_email",
        payload={"recipient": "user@example.com", "subject": "Hello"},
        state=JobState.RUNNING,
        retry_count=1,
        max_retries=5,
        error_message="Transient network glitch",
        cron_expression="0 * * * *",
    )

    data = original.to_dict()
    assert data["id"] == "job-123"
    assert data["state"] == "RUNNING"
    assert data["payload"]["recipient"] == "user@example.com"
    assert data["retry_count"] == 1
    assert data["max_retries"] == 5

    restored = Job.from_dict(data)
    assert restored.id == original.id
    assert restored.name == original.name
    assert restored.job_type == original.job_type
    assert restored.payload == original.payload
    assert restored.state == original.state
    assert restored.retry_count == original.retry_count
    assert restored.max_retries == original.max_retries
    assert restored.error_message == original.error_message
    assert restored.cron_expression == original.cron_expression


def test_job_from_dict_defaults():
    minimal_data = {"name": "minimal"}
    job = Job.from_dict(minimal_data)
    assert job.name == "minimal"
    assert job.job_type == "echo"
    assert job.state == JobState.CREATED
    assert job.retry_count == 0


def test_job_create_schema_validation():
    # Valid model creation
    job_create = JobCreate(
        name="valid-job",
        job_type="echo",
        payload={"msg": "hello"},
        cron_expression="*/5 * * * *",
        max_retries=2,
    )
    assert job_create.name == "valid-job"
    assert job_create.max_retries == 2

    # Empty name should fail validation
    with pytest.raises(ValidationError):
        JobCreate(name="", job_type="echo")

    # Negative max_retries should fail validation
    with pytest.raises(ValidationError):
        JobCreate(name="bad", job_type="echo", max_retries=-1)


def test_job_response_from_job():
    job = Job(name="report", job_type="pdf", payload={"page": 1})
    response = JobResponse.from_job(job)
    assert response.id == job.id
    assert response.name == "report"
    assert response.job_type == "pdf"
    assert response.state == JobState.CREATED
