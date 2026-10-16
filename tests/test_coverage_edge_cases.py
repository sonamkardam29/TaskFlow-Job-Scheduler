"""Additional unit tests to verify AppState factory and edge cases."""
import pytest
from app.config import Settings
from app.main import AppState, app, get_log_store, get_repository, get_scheduler, get_worker_pool
from app.worker import fail_randomly_handler
from app.log_store import InMemoryLogStore
from moto import mock_aws


def test_app_state_in_memory_initialization():
    state = AppState(Settings(use_aws=False))
    assert state.repository is not None
    assert state.log_store is not None
    assert state.worker_pool is not None
    assert state.scheduler is not None


def test_app_state_aws_initialization():
    with mock_aws():
        state = AppState(Settings(use_aws=True, aws_region="us-east-1"))
        assert state.repository is not None
        assert state.log_store is not None


def test_direct_dependency_callers():
    assert get_repository() is not None
    assert get_log_store() is not None
    assert get_worker_pool() is not None
    assert get_scheduler() is not None


def test_fail_randomly_handler_when_not_failing():
    store = InMemoryLogStore()
    res = fail_randomly_handler({"should_fail": False}, store, "job-xyz")
    assert res == "Job executed successfully"
    logs = store.read_log("job-xyz")
    assert "chose not to fail" in logs
