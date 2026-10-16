"""Tests for TaskFlow state machine and transition rules."""
import pytest
from app.models import Job, JobState
from app.state_machine import (
    ALLOWED_TRANSITIONS,
    InvalidTransitionError,
    can_transition,
    transition_job,
)


@pytest.mark.parametrize(
    "current_state, target_state",
    [
        (JobState.CREATED, JobState.QUEUED),
        (JobState.QUEUED, JobState.RUNNING),
        (JobState.RUNNING, JobState.SUCCEEDED),
        (JobState.RUNNING, JobState.FAILED),
        (JobState.FAILED, JobState.RETRYING),
        (JobState.RETRYING, JobState.QUEUED),
        # Cancellation from every non-final state
        (JobState.CREATED, JobState.CANCELLED),
        (JobState.QUEUED, JobState.CANCELLED),
        (JobState.RUNNING, JobState.CANCELLED),
        (JobState.FAILED, JobState.CANCELLED),
        (JobState.RETRYING, JobState.CANCELLED),
    ],
)
def test_valid_state_transitions(current_state: JobState, target_state: JobState):
    assert can_transition(current_state, target_state) is True
    job = Job(name="test", state=current_state)
    updated = transition_job(job, target_state)
    assert updated.state == target_state


@pytest.mark.parametrize(
    "current_state, target_state",
    [
        (JobState.CREATED, JobState.SUCCEEDED),
        (JobState.CREATED, JobState.RUNNING),
        (JobState.CREATED, JobState.RETRYING),
        (JobState.QUEUED, JobState.SUCCEEDED),
        (JobState.QUEUED, JobState.FAILED),
        (JobState.RUNNING, JobState.QUEUED),
        (JobState.SUCCEEDED, JobState.RUNNING),
        (JobState.SUCCEEDED, JobState.CANCELLED),
        (JobState.SUCCEEDED, JobState.QUEUED),
        (JobState.CANCELLED, JobState.RUNNING),
        (JobState.CANCELLED, JobState.QUEUED),
        (JobState.FAILED, JobState.SUCCEEDED),
        (JobState.RETRYING, JobState.SUCCEEDED),
    ],
)
def test_invalid_state_transitions_raise_error(current_state: JobState, target_state: JobState):
    assert can_transition(current_state, target_state) is False
    job = Job(name="test", state=current_state)
    with pytest.raises(InvalidTransitionError) as exc_info:
        transition_job(job, target_state)

    err = exc_info.value
    assert err.current_state == current_state
    assert err.target_state == target_state
    assert f"Cannot transition job from {current_state.value} to {target_state.value}" in str(err)


def test_transition_job_metadata_updates():
    job = Job(name="test", state=JobState.RUNNING)
    original_updated_at = job.updated_at

    # Transition to FAILED with error message
    transition_job(job, JobState.FAILED, error_message="Database connection timed out")
    assert job.state == JobState.FAILED
    assert job.error_message == "Database connection timed out"
    assert job.updated_at >= original_updated_at

    # Transition from FAILED to RETRYING with increment_retry
    assert job.retry_count == 0
    transition_job(job, JobState.RETRYING, increment_retry=True)
    assert job.state == JobState.RETRYING
    assert job.retry_count == 1
