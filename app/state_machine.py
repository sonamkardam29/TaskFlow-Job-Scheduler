"""Job lifecycle state machine for TaskFlow."""
from __future__ import annotations

from app.models import Job, JobState, utc_now


class InvalidTransitionError(Exception):
    """Raised when an invalid job state transition is attempted."""
    def __init__(self, current_state: JobState, target_state: JobState, reason: str = ""):
        message = f"Cannot transition job from {current_state.value} to {target_state.value}"
        if reason:
            message += f": {reason}"
        super().__init__(message)
        self.current_state = current_state
        self.target_state = target_state
        self.reason = reason


# Terminal states from which no further normal progression is permitted
TERMINAL_STATES = {JobState.SUCCEEDED, JobState.CANCELLED}

# Explicit valid direct transitions
ALLOWED_TRANSITIONS: dict[JobState, set[JobState]] = {
    JobState.CREATED: {JobState.QUEUED, JobState.CANCELLED},
    JobState.QUEUED: {JobState.RUNNING, JobState.CANCELLED},
    JobState.RUNNING: {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED},
    JobState.FAILED: {JobState.RETRYING, JobState.CANCELLED},
    JobState.RETRYING: {JobState.QUEUED, JobState.CANCELLED},
    JobState.SUCCEEDED: set(),
    JobState.CANCELLED: set(),
}


def can_transition(current: JobState, target: JobState) -> bool:
    """Check whether a transition between two states is permitted."""
    return target in ALLOWED_TRANSITIONS.get(current, set())


def transition_job(
    job: Job,
    target_state: JobState,
    *,
    error_message: str | None = None,
    increment_retry: bool = False,
) -> Job:
    """Validate and transition a job to a target state, updating timestamps and metadata.

    Args:
        job: The Job instance to transition.
        target_state: The desired destination JobState.
        error_message: Optional error message to attach (useful on FAILED).
        increment_retry: If True, increments the job's retry_count (useful on RETRYING).

    Returns:
        The updated Job instance.

    Raises:
        InvalidTransitionError: If the transition is not allowed by the state machine.
    """
    if not can_transition(job.state, target_state):
        raise InvalidTransitionError(
            current_state=job.state,
            target_state=target_state,
            reason=f"Transition from {job.state.value} to {target_state.value} is not allowed"
        )

    job.state = target_state
    job.updated_at = utc_now()

    if error_message is not None:
        job.error_message = error_message

    if increment_retry:
        job.retry_count += 1

    return job
