"""Data models for TaskFlow."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
import uuid
from pydantic import BaseModel, Field


class JobState(str, Enum):
    """Lifecycle states of a TaskFlow job."""
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


# Final terminal states where no further transitions (except re-queuing recurring jobs) are allowed
FINAL_STATES = {JobState.SUCCEEDED, JobState.CANCELLED}


def utc_now() -> datetime:
    """Return current UTC timestamp with timezone."""
    return datetime.now(timezone.utc)


@dataclass
class Job:
    """Internal job entity used throughout the engine and repositories."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    job_type: str = "echo"
    payload: dict[str, Any] = field(default_factory=dict)
    state: JobState = JobState.CREATED
    retry_count: int = 0
    max_retries: int = 3
    error_message: str | None = None
    cron_expression: str | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        """Convert job to a dictionary suitable for storage or serialization."""
        return {
            "id": self.id,
            "name": self.name,
            "job_type": self.job_type,
            "payload": self.payload,
            "state": self.state.value if isinstance(self.state, JobState) else str(self.state),
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "cron_expression": self.cron_expression,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        """Create a Job entity from a dictionary."""
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif not isinstance(created_at, datetime):
            created_at = utc_now()

        updated_at = data.get("updated_at")
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        elif not isinstance(updated_at, datetime):
            updated_at = utc_now()

        raw_state = data.get("state", JobState.CREATED.value)
        state = JobState(raw_state) if isinstance(raw_state, str) else raw_state

        return cls(
            id=data.get("id", str(uuid.uuid4())),
            name=data.get("name", ""),
            job_type=data.get("job_type", "echo"),
            payload=data.get("payload", {}) or {},
            state=state,
            retry_count=int(data.get("retry_count", 0)),
            max_retries=int(data.get("max_retries", 3)),
            error_message=data.get("error_message"),
            cron_expression=data.get("cron_expression"),
            created_at=created_at,
            updated_at=updated_at,
        )


class JobCreate(BaseModel):
    """API request payload to create a new job."""
    name: str = Field(..., description="Human-readable job name", min_length=1)
    job_type: str = Field(..., description="Registered worker job type (e.g. echo, sleep, fail_randomly)")
    payload: dict[str, Any] = Field(default_factory=dict, description="Input parameters for the job")
    cron_expression: str | None = Field(default=None, description="Optional cron expression for recurring jobs (e.g. '*/5 * * * *')")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts on failure")


class JobResponse(BaseModel):
    """API response model representing job details."""
    id: str
    name: str
    job_type: str
    payload: dict[str, Any]
    state: JobState
    retry_count: int
    max_retries: int
    error_message: str | None = None
    cron_expression: str | None = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_job(cls, job: Job) -> JobResponse:
        """Helper to convert domain Job to API response model."""
        return cls(
            id=job.id,
            name=job.name,
            job_type=job.job_type,
            payload=job.payload,
            state=job.state,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            error_message=job.error_message,
            cron_expression=job.cron_expression,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
