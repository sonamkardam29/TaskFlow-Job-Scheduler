"""FastAPI application entrypoint and routes for TaskFlow."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse

from app.config import Settings, settings
from app.log_store import InMemoryLogStore, LogStore, S3LogStore
from app.models import Job, JobCreate, JobResponse, JobState
from app.repository import DynamoDBJobRepository, InMemoryJobRepository, JobRepository
from app.state_machine import InvalidTransitionError, transition_job
from app.scheduler import JobScheduler
from app.worker import WorkerPool


class AppState:
    """Encapsulates runtime singletons for repositories, storage, and worker pools."""
    def __init__(self, current_settings: Settings = settings) -> None:
        self.settings = current_settings
        if self.settings.use_aws:
            self.repository: JobRepository = DynamoDBJobRepository(
                table_name=self.settings.dynamodb_table_name,
                region_name=self.settings.aws_region,
            )
            self.log_store: LogStore = S3LogStore(
                bucket_name=self.settings.s3_bucket_name,
                region_name=self.settings.aws_region,
            )
        else:
            self.repository = InMemoryJobRepository()
            self.log_store = InMemoryLogStore()

        self.worker_pool = WorkerPool(
            repository=self.repository,
            log_store=self.log_store,
            max_workers=self.settings.max_workers,
            poll_interval_seconds=self.settings.poll_interval_seconds,
            base_backoff_seconds=self.settings.base_backoff_seconds,
        )
        self.scheduler = JobScheduler(
            repository=self.repository,
            log_store=self.log_store,
        )


app_state = AppState()


def get_repository() -> JobRepository:
    return app_state.repository


def get_log_store() -> LogStore:
    return app_state.log_store


def get_worker_pool() -> WorkerPool:
    return app_state.worker_pool


def get_scheduler() -> JobScheduler:
    return app_state.scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to manage background worker lifecycle."""
    # Start worker pool
    app_state.worker_pool.start()
    if app_state.scheduler:
        app_state.scheduler.start()
    yield
    # Stop worker pool and scheduler gracefully
    if app_state.scheduler:
        app_state.scheduler.stop()
    app_state.worker_pool.stop(wait=True)


app = FastAPI(
    title="TaskFlow API",
    description="Scalable job scheduler and workflow execution engine.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok", "app": settings.app_name}


@app.post(
    "/jobs",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new job",
)
def create_job(
    payload: JobCreate,
    repo: JobRepository = Depends(get_repository),
    log_store: LogStore = Depends(get_log_store),
) -> JobResponse:
    """Create a new job in the system and queue it for execution."""
    job = Job(
        name=payload.name,
        job_type=payload.job_type,
        payload=payload.payload,
        cron_expression=payload.cron_expression,
        max_retries=payload.max_retries,
        state=JobState.CREATED,
    )

    # Transition from CREATED -> QUEUED
    transition_job(job, JobState.QUEUED)
    repo.save(job)
    log_store.write_log(job.id, f"[CREATED] Job '{job.name}' queued successfully.")

    # Register in scheduler if recurring
    if job.cron_expression and app_state.scheduler:
        app_state.scheduler.schedule_job(job)

    return JobResponse.from_job(job)


@app.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="Get job details by ID",
)
def get_job(
    job_id: str,
    repo: JobRepository = Depends(get_repository),
) -> JobResponse:
    """Retrieve details and status of a specific job."""
    job = repo.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found")
    return JobResponse.from_job(job)


@app.get(
    "/jobs",
    response_model=list[JobResponse],
    summary="List all jobs with optional state filter",
)
def list_jobs(
    state: JobState | None = Query(None, description="Filter jobs by state"),
    repo: JobRepository = Depends(get_repository),
) -> list[JobResponse]:
    """List jobs currently in the system, optionally filtered by state."""
    jobs = repo.list_jobs(state=state)
    return [JobResponse.from_job(j) for j in jobs]


@app.post(
    "/jobs/{job_id}/cancel",
    response_model=JobResponse,
    summary="Cancel a running or queued job",
)
def cancel_job(
    job_id: str,
    repo: JobRepository = Depends(get_repository),
    log_store: LogStore = Depends(get_log_store),
) -> JobResponse:
    """Cancel a job that has not already finished."""
    job = repo.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found")

    try:
        transition_job(job, JobState.CANCELLED)
    except InvalidTransitionError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    repo.update(job)
    log_store.append_log(job_id, "[CANCELLED] Job was cancelled by user request.")
    return JobResponse.from_job(job)


@app.get(
    "/jobs/{job_id}/logs",
    summary="Get execution logs for a job",
)
def get_job_logs(
    job_id: str,
    repo: JobRepository = Depends(get_repository),
    log_store: LogStore = Depends(get_log_store),
):
    """Retrieve execution output logs for a job."""
    job = repo.get(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found")

    logs = log_store.read_log(job_id) or ""
    return JSONResponse(content={"job_id": job_id, "logs": logs})
