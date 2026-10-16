"""Log storage abstraction and implementations for TaskFlow."""
from __future__ import annotations

from abc import ABC, abstractmethod
import threading
from typing import Any
import boto3
from botocore.exceptions import ClientError


class LogStore(ABC):
    """Abstract interface for storing and retrieving job execution logs."""

    @abstractmethod
    def write_log(self, job_id: str, content: str) -> None:
        """Write or overwrite the log content for a given job."""
        pass

    @abstractmethod
    def append_log(self, job_id: str, message: str) -> None:
        """Append a log message line for a given job."""
        pass

    @abstractmethod
    def read_log(self, job_id: str) -> str | None:
        """Retrieve the log content for a job, or None if no log exists."""
        pass


class InMemoryLogStore(LogStore):
    """Thread-safe in-memory log store for local development and testing."""

    def __init__(self) -> None:
        self._logs: dict[str, str] = {}
        self._lock = threading.Lock()

    def write_log(self, job_id: str, content: str) -> None:
        with self._lock:
            self._logs[job_id] = content

    def append_log(self, job_id: str, message: str) -> None:
        with self._lock:
            current = self._logs.get(job_id, "")
            if current and not current.endswith("\n"):
                current += "\n"
            self._logs[job_id] = current + message + "\n"

    def read_log(self, job_id: str) -> str | None:
        with self._lock:
            return self._logs.get(job_id)


class S3LogStore(LogStore):
    """AWS S3 log store for production cloud deployments."""

    def __init__(self, bucket_name: str, region_name: str = "us-east-1", s3_client: Any = None) -> None:
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.s3_client = s3_client or boto3.client("s3", region_name=self.region_name)

    def _key_for_job(self, job_id: str) -> str:
        return f"logs/{job_id}.log"

    def write_log(self, job_id: str, content: str) -> None:
        key = self._key_for_job(job_id)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )

    def append_log(self, job_id: str, message: str) -> None:
        current = self.read_log(job_id) or ""
        if current and not current.endswith("\n"):
            current += "\n"
        self.write_log(job_id, current + message + "\n")

    def read_log(self, job_id: str) -> str | None:
        key = self._key_for_job(job_id)
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
