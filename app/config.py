"""Application configuration management for TaskFlow."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """TaskFlow configuration settings sourced from environment variables."""
    app_name: str = "TaskFlow"
    debug: bool = False
    use_aws: bool = os.getenv("USE_AWS", "false").lower() in ("true", "1", "yes")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    dynamodb_table_name: str = os.getenv("DYNAMODB_TABLE_NAME", "taskflow-jobs")
    s3_bucket_name: str = os.getenv("S3_BUCKET_NAME", "taskflow-logs")
    max_workers: int = int(os.getenv("MAX_WORKERS", "4"))
    poll_interval_seconds: float = float(os.getenv("POLL_INTERVAL_SECONDS", "0.5"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    base_backoff_seconds: float = float(os.getenv("BASE_BACKOFF_SECONDS", "1.0"))


# Global settings singleton
settings = Settings()
