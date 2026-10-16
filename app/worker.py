"""Worker pool and execution engine for TaskFlow."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import threading
import time
from typing import Any, Callable

from app.log_store import LogStore
from app.models import Job, JobState
from app.repository import JobRepository
from app.state_machine import InvalidTransitionError, transition_job

logger = logging.getLogger(__name__)

# Type alias for job handler functions
JobHandler = Callable[[dict[str, Any], LogStore, str], str]


def echo_handler(payload: dict[str, Any], log_store: LogStore, job_id: str) -> str:
    """Sample handler: echoes payload message."""
    message = payload.get("message", "Hello from TaskFlow!")
    log_store.append_log(job_id, f"[INFO] Echoing payload message: {message}")
    return f"Echo successful: {message}"


def sleep_handler(payload: dict[str, Any], log_store: LogStore, job_id: str) -> str:
    """Sample handler: sleeps for a given duration in seconds."""
    seconds = float(payload.get("seconds", 1.0))
    log_store.append_log(job_id, f"[INFO] Sleeping for {seconds:.2f} seconds...")
    time.sleep(seconds)
    log_store.append_log(job_id, f"[INFO] Woke up after {seconds:.2f} seconds.")
    return f"Slept for {seconds:.2f} seconds"


def fail_randomly_handler(payload: dict[str, Any], log_store: LogStore, job_id: str) -> str:
    """Sample handler: deterministically or randomly triggers an exception."""
    should_fail = payload.get("should_fail", True)
    if should_fail:
        error_msg = payload.get("error_message", "Simulated job failure")
        log_store.append_log(job_id, f"[ERROR] Task failed as requested: {error_msg}")
        raise RuntimeError(error_msg)
    log_store.append_log(job_id, "[INFO] Job chose not to fail.")
    return "Job executed successfully"


DEFAULT_HANDLERS: dict[str, JobHandler] = {
    "echo": echo_handler,
    "sleep": sleep_handler,
    "fail_randomly": fail_randomly_handler,
}


class WorkerPool:
    """Manages thread pool execution, job dispatch, and retry backoff."""

    def __init__(
        self,
        repository: JobRepository,
        log_store: LogStore,
        max_workers: int = 4,
        poll_interval_seconds: float = 0.5,
        base_backoff_seconds: float = 1.0,
        handlers: dict[str, JobHandler] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.repository = repository
        self.log_store = log_store
        self.max_workers = max_workers
        self.poll_interval = poll_interval_seconds
        self.base_backoff = base_backoff_seconds
        self.sleep_fn = sleep_fn

        self.handlers: dict[str, JobHandler] = dict(DEFAULT_HANDLERS)
        if handlers:
            self.handlers.update(handlers)

        self._executor: ThreadPoolExecutor | None = None
        self._poller_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._running = False
        self._lock = threading.Lock()

    def register_handler(self, job_type: str, handler: JobHandler) -> None:
        """Register a new job type handler."""
        self.handlers[job_type] = handler

    def start(self) -> None:
        """Start the background worker thread pool and polling loop."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="taskflow-worker")
            self._poller_thread = threading.Thread(target=self._poll_loop, daemon=True, name="taskflow-poller")
            self._poller_thread.start()
            logger.info("WorkerPool started with %d workers", self.max_workers)

    def stop(self, wait: bool = True) -> None:
        """Stop polling and wait for in-flight tasks to complete."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._poller_thread and self._poller_thread.is_alive():
            self._poller_thread.join(timeout=2.0)

        if self._executor:
            self._executor.shutdown(wait=wait, cancel_futures=False)
            self._executor = None
        logger.info("WorkerPool stopped")

    def _poll_loop(self) -> None:
        """Poll the repository for queued jobs and submit to the pool."""
        while not self._stop_event.is_set():
            try:
                self.process_queued_jobs()
            except Exception as e:
                logger.error("Error during poll loop: %s", e)
            self._stop_event.wait(self.poll_interval)

    def process_queued_jobs(self) -> int:
        """Scan for QUEUED jobs and submit them to executor.

        Returns count of jobs submitted.
        """
        queued_jobs = self.repository.list_jobs(state=JobState.QUEUED)
        submitted = 0

        for job in queued_jobs:
            if self._stop_event.is_set():
                break

            # Atomically acquire job: updates state from QUEUED -> RUNNING
            if self.repository.acquire_job(job.id):
                submitted += 1
                if self._executor and not self._stop_event.is_set():
                    self._executor.submit(self._execute_job, job.id)
                else:
                    # In synchronous or test mode when executor isn't started
                    self._execute_job(job.id)

        return submitted

    def _calculate_backoff(self, retry_count: int) -> float:
        """Compute exponential backoff: 1s, 2s, 4s for base 1.0."""
        return self.base_backoff * (2 ** retry_count)

    def _execute_job(self, job_id: str) -> None:
        """Execute a single job and handle transitions and retries."""
        job = self.repository.get(job_id)
        if not job:
            return

        # Check if job was cancelled right before execution
        if job.state == JobState.CANCELLED:
            self.log_store.append_log(job_id, "[WARN] Job cancelled before execution.")
            return

        handler = self.handlers.get(job.job_type)
        self.log_store.append_log(
            job_id,
            f"[START] Starting execution of job '{job.name}' (type={job.job_type}, attempt={job.retry_count + 1}/{job.max_retries + 1})"
        )

        try:
            if not handler:
                raise ValueError(f"Unknown job type: '{job.job_type}'. Registered types: {list(self.handlers.keys())}")

            result = handler(job.payload, self.log_store, job_id)
            self.log_store.append_log(job_id, f"[SUCCESS] Execution completed: {result}")

            # Re-fetch latest job state to ensure it wasn't cancelled mid-run
            current_job = self.repository.get(job_id)
            if current_job and current_job.state == JobState.CANCELLED:
                self.log_store.append_log(job_id, "[WARN] Job marked CANCELLED while running; discarding success transition.")
                return

            # Transition RUNNING -> SUCCEEDED
            transition_job(job, JobState.SUCCEEDED)
            job.error_message = None
            self.repository.update(job)

        except Exception as exc:
            error_msg = str(exc)
            logger.warning("Job %s execution failed: %s", job_id, error_msg)
            self.log_store.append_log(job_id, f"[ERROR] Execution failed: {error_msg}")

            # Transition RUNNING -> FAILED
            try:
                transition_job(job, JobState.FAILED, error_message=error_msg)
                self.repository.update(job)
            except InvalidTransitionError:
                # Could happen if cancelled
                return

            # Check retry eligibility
            if job.retry_count < job.max_retries:
                backoff_delay = self._calculate_backoff(job.retry_count)
                self.log_store.append_log(
                    job_id,
                    f"[RETRY] Scheduling retry in {backoff_delay:.2f}s (retry attempt {job.retry_count + 1} of {job.max_retries})"
                )

                # Transition FAILED -> RETRYING
                transition_job(job, JobState.RETRYING, increment_retry=True)
                self.repository.update(job)

                # Execute backoff wait (custom or mocked sleep)
                self._handle_retry_backoff(job.id, backoff_delay)
            else:
                self.log_store.append_log(
                    job_id,
                    f"[FAILED] Job exhausted all {job.max_retries} retries."
                )

    def _handle_retry_backoff(self, job_id: str, delay: float) -> None:
        """Wait backoff duration and re-queue job."""
        def re_queue():
            if delay > 0:
                self.sleep_fn(delay)
            latest = self.repository.get(job_id)
            if latest and latest.state == JobState.RETRYING:
                transition_job(latest, JobState.QUEUED)
                self.repository.update(latest)
                self.log_store.append_log(job_id, "[QUEUED] Job re-queued for execution.")

        # If executor is available and running, schedule or execute re-queue
        if self._executor and self._running:
            self._executor.submit(re_queue)
        else:
            re_queue()
