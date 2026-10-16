"""Recurring job scheduler for TaskFlow using cron expressions."""
from __future__ import annotations

from datetime import datetime, timezone
import logging
import threading
import time
from typing import Any
from croniter import croniter

from app.log_store import LogStore
from app.models import Job, JobState, utc_now
from app.repository import JobRepository
from app.state_machine import InvalidTransitionError, transition_job

logger = logging.getLogger(__name__)


def is_valid_cron(expression: str) -> bool:
    """Validate a 5-part cron expression string."""
    try:
        return croniter.is_valid(expression)
    except Exception:
        return False


class JobScheduler:
    """Schedules and automatically re-queues recurring jobs based on cron expressions."""

    def __init__(
        self,
        repository: JobRepository,
        log_store: LogStore,
        check_interval_seconds: float = 1.0,
    ) -> None:
        self.repository = repository
        self.log_store = log_store
        self.check_interval = check_interval_seconds

        # Map of job_id -> {"cron": str, "next_run": float}
        self._schedules: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._running = False

    def schedule_job(self, job: Job, base_time: datetime | None = None) -> bool:
        """Register a job with a cron expression into the scheduler."""
        if not job.cron_expression or not is_valid_cron(job.cron_expression):
            logger.warning("Invalid or missing cron expression '%s' for job %s", job.cron_expression, job.id)
            return False

        now = base_time or utc_now()
        cron = croniter(job.cron_expression, now)
        next_run = cron.get_next(float)

        with self._lock:
            self._schedules[job.id] = {
                "cron": job.cron_expression,
                "next_run": next_run,
            }

        logger.info("Job %s scheduled with cron '%s', next run: %s", job.id, job.cron_expression, datetime.fromtimestamp(next_run, timezone.utc))
        return True

    def remove_job(self, job_id: str) -> None:
        """Remove a job from recurring schedule."""
        with self._lock:
            self._schedules.pop(job_id, None)

    def is_scheduled(self, job_id: str) -> bool:
        """Check whether a job is currently in the schedule table."""
        with self._lock:
            return job_id in self._schedules

    def tick(self, current_time: datetime | None = None) -> list[str]:
        """Evaluate scheduled jobs and re-queue any that are due.

        Returns:
            List of job_ids that were triggered and re-queued.
        """
        now_dt = current_time or utc_now()
        now_ts = now_dt.timestamp()
        triggered_jobs: list[str] = []

        with self._lock:
            items_to_check = list(self._schedules.items())

        for job_id, info in items_to_check:
            if now_ts >= info["next_run"]:
                job = self.repository.get(job_id)
                if not job:
                    self.remove_job(job_id)
                    continue

                if job.state == JobState.CANCELLED:
                    logger.info("Removing cancelled job %s from scheduler", job_id)
                    self.remove_job(job_id)
                    continue

                # Re-queue the job if it is not currently executing
                if job.state != JobState.RUNNING:
                    # Reset state directly to QUEUED and reset retry counter
                    job.state = JobState.QUEUED
                    job.retry_count = 0
                    job.error_message = None
                    job.updated_at = utc_now()
                    self.repository.update(job)
                    self.log_store.append_log(
                        job_id,
                        f"[SCHEDULER] Job re-queued on cron schedule '{info['cron']}' at {now_dt.isoformat()}."
                    )
                    triggered_jobs.append(job_id)

                # Compute next execution time
                cron = croniter(info["cron"], now_dt)
                with self._lock:
                    if job_id in self._schedules:
                        self._schedules[job_id]["next_run"] = cron.get_next(float)

        return triggered_jobs

    def start(self) -> None:
        """Start the background scheduler thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="taskflow-scheduler")
            self._thread.start()
            logger.info("JobScheduler started")

    def stop(self) -> None:
        """Stop the background scheduler thread."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            self._thread = None
        logger.info("JobScheduler stopped")

    def _run_loop(self) -> None:
        """Continuous ticker loop."""
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception as e:
                logger.error("Error in scheduler tick: %s", e)
            self._stop_event.wait(self.check_interval)
