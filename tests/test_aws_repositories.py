"""Tests for AWS DynamoDB and S3 implementations using moto."""
import os
import boto3
import pytest
from moto import mock_aws

from app.log_store import S3LogStore
from app.models import Job, JobState
from app.repository import DynamoDBJobRepository
from app.worker import WorkerPool


@pytest.fixture
def aws_env():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def dynamodb_table(aws_env):
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
            TableName="taskflow-jobs",
            KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "job_id", "AttributeType": "S"},
                {"AttributeName": "state", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "state-index",
                    "KeySchema": [{"AttributeName": "state", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table


@pytest.fixture
def s3_bucket(aws_env):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="taskflow-logs")
        yield s3


def test_dynamodb_repository_crud_and_query(dynamodb_table):
    repo = DynamoDBJobRepository(table_name="taskflow-jobs", region_name="us-east-1")

    # 1. Save jobs
    j1 = Job(id="job-1", name="job-one", job_type="echo", state=JobState.QUEUED)
    j2 = Job(id="job-2", name="job-two", job_type="echo", state=JobState.SUCCEEDED)
    repo.save(j1)
    repo.save(j2)

    # 2. Get job
    fetched = repo.get("job-1")
    assert fetched is not None
    assert fetched.id == "job-1"
    assert fetched.name == "job-one"
    assert fetched.state == JobState.QUEUED

    # 3. Get non-existent job
    assert repo.get("job-999") is None

    # 4. List all jobs
    all_jobs = repo.list_jobs()
    assert len(all_jobs) == 2

    # 5. List jobs filtered by state (using GSI state-index)
    queued_jobs = repo.list_jobs(state=JobState.QUEUED)
    assert len(queued_jobs) == 1
    assert queued_jobs[0].id == "job-1"

    succeeded_jobs = repo.list_jobs(state=JobState.SUCCEEDED)
    assert len(succeeded_jobs) == 1
    assert succeeded_jobs[0].id == "job-2"

    # 6. Update job
    fetched.error_message = "New error"
    repo.update(fetched)
    updated = repo.get("job-1")
    assert updated.error_message == "New error"


def test_dynamodb_atomic_acquire_prevents_duplicate_processing(dynamodb_table):
    repo = DynamoDBJobRepository(table_name="taskflow-jobs", region_name="us-east-1")
    job = Job(id="race-job", name="race", job_type="echo", state=JobState.QUEUED)
    repo.save(job)

    # Worker 1 attempts to acquire: should succeed
    acquired_1 = repo.acquire_job("race-job")
    assert acquired_1 is True

    # Job is now RUNNING
    current = repo.get("race-job")
    assert current.state == JobState.RUNNING

    # Worker 2 attempts to acquire the same job: should fail because state != QUEUED
    acquired_2 = repo.acquire_job("race-job")
    assert acquired_2 is False


def test_s3_log_store_operations(s3_bucket):
    log_store = S3LogStore(bucket_name="taskflow-logs", region_name="us-east-1")

    # Non-existent log returns None
    assert log_store.read_log("missing-job") is None

    # Write log
    log_store.write_log("test-job", "Initial log line\n")
    content = log_store.read_log("test-job")
    assert "Initial log line" in content

    # Append log
    log_store.append_log("test-job", "Second log line")
    updated = log_store.read_log("test-job")
    assert "Initial log line" in updated
    assert "Second log line" in updated


def test_worker_pool_end_to_end_with_aws(dynamodb_table, s3_bucket):
    repo = DynamoDBJobRepository(table_name="taskflow-jobs", region_name="us-east-1")
    log_store = S3LogStore(bucket_name="taskflow-logs", region_name="us-east-1")

    job = Job(
        id="aws-job-1",
        name="aws-echo",
        job_type="echo",
        payload={"message": "Running on AWS services (mocked)"},
        state=JobState.QUEUED,
    )
    repo.save(job)

    worker = WorkerPool(repository=repo, log_store=log_store, max_workers=2)
    submitted = worker.process_queued_jobs()
    assert submitted == 1

    # Check job completed in DynamoDB
    final_job = repo.get("aws-job-1")
    assert final_job.state == JobState.SUCCEEDED

    # Check logs stored in S3
    logs = log_store.read_log("aws-job-1")
    assert "Running on AWS services (mocked)" in logs
    assert "[SUCCESS]" in logs
