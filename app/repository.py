"""Job repository abstraction and implementations for TaskFlow."""
from __future__ import annotations

from abc import ABC, abstractmethod
import copy
import threading
from typing import Any
import boto3
from botocore.exceptions import ClientError

from app.models import Job, JobState, utc_now


class JobRepository(ABC):
    """Abstract storage interface for Job domain entities."""

    @abstractmethod
    def save(self, job: Job) -> Job:
        """Create or completely persist a job."""
        pass

    @abstractmethod
    def get(self, job_id: str) -> Job | None:
        """Fetch a job by ID, returning None if not found."""
        pass

    @abstractmethod
    def list_jobs(self, state: JobState | None = None) -> list[Job]:
        """List jobs, optionally filtered by state."""
        pass

    @abstractmethod
    def acquire_job(self, job_id: str) -> bool:
        """Atomically transition a job from QUEUED to RUNNING.

        Returns:
            True if the job was successfully acquired and marked RUNNING,
            False if it was already acquired, cancelled, or not QUEUED.
        """
        pass

    @abstractmethod
    def update(self, job: Job) -> Job:
        """Update an existing job's state and attributes."""
        pass


class InMemoryJobRepository(JobRepository):
    """Thread-safe in-memory job repository for local development and unit tests."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def save(self, job: Job) -> Job:
        with self._lock:
            # Store a copy so callers modifying the instance outside don't cause races
            self._jobs[job.id] = copy.deepcopy(job)
            return copy.deepcopy(job)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return copy.deepcopy(job) if job else None

    def list_jobs(self, state: JobState | None = None) -> list[Job]:
        with self._lock:
            jobs = list(self._jobs.values())
            if state is not None:
                jobs = [j for j in jobs if j.state == state]
            # Return sorted by created_at descending
            jobs.sort(key=lambda j: j.created_at, reverse=True)
            return [copy.deepcopy(j) for j in jobs]

    def acquire_job(self, job_id: str) -> bool:
        """Atomically check if job is QUEUED and transition to RUNNING."""
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.state != JobState.QUEUED:
                return False
            job.state = JobState.RUNNING
            job.updated_at = utc_now()
            return True

    def update(self, job: Job) -> Job:
        with self._lock:
            job.updated_at = utc_now()
            self._jobs[job.id] = copy.deepcopy(job)
            return copy.deepcopy(job)


class DynamoDBJobRepository(JobRepository):
    """AWS DynamoDB job repository with optimistic concurrency control via conditional writes."""

    def __init__(
        self,
        table_name: str = "taskflow-jobs",
        region_name: str = "us-east-1",
        dynamodb_resource: Any = None,
    ) -> None:
        self.table_name = table_name
        self.region_name = region_name
        self.dynamodb = dynamodb_resource or boto3.resource("dynamodb", region_name=self.region_name)
        self.table = self.dynamodb.Table(self.table_name)

    def _to_item(self, job: Job) -> dict[str, Any]:
        item = job.to_dict()
        # DynamoDB partition key is job_id
        item["job_id"] = job.id
        return item

    def _from_item(self, item: dict[str, Any]) -> Job:
        data = dict(item)
        if "job_id" in data and "id" not in data:
            data["id"] = data["job_id"]
        return Job.from_dict(data)

    def save(self, job: Job) -> Job:
        item = self._to_item(job)
        self.table.put_item(Item=item)
        return job

    def get(self, job_id: str) -> Job | None:
        response = self.table.get_item(Key={"job_id": job_id})
        item = response.get("Item")
        return self._from_item(item) if item else None

    def list_jobs(self, state: JobState | None = None) -> list[Job]:
        if state is not None:
            # Query using Global Secondary Index (state-index)
            response = self.table.query(
                IndexName="state-index",
                KeyConditionExpression="#st = :val",
                ExpressionAttributeNames={"#st": "state"},
                ExpressionAttributeValues={":val": state.value},
            )
            items = response.get("Items", [])
        else:
            response = self.table.scan()
            items = response.get("Items", [])

        jobs = [self._from_item(item) for item in items]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs

    def acquire_job(self, job_id: str) -> bool:
        """Atomic conditional update: only set state to RUNNING if current state is QUEUED."""
        now_iso = utc_now().isoformat()
        try:
            self.table.update_item(
                Key={"job_id": job_id},
                UpdateExpression="SET #st = :running, #up = :now",
                ConditionExpression="#st = :queued",
                ExpressionAttributeNames={"#st": "state", "#up": "updated_at"},
                ExpressionAttributeValues={
                    ":running": JobState.RUNNING.value,
                    ":queued": JobState.QUEUED.value,
                    ":now": now_iso,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def update(self, job: Job) -> Job:
        job.updated_at = utc_now()
        item = self._to_item(job)
        self.table.put_item(Item=item)
        return job
