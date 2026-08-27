import asyncio
import os
import time

import pytest
from fastapi.testclient import TestClient
from waymark import list_schedules
from waymark.bridge import wait_for_instance

from example_app.web import app


def _enable_real_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.environ.get("WAYMARK_RUN_REAL_CLUSTER") == "1":
        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)


def _require_real_cluster() -> None:
    if os.environ.get("WAYMARK_RUN_REAL_CLUSTER") != "1":
        pytest.skip("requires WAYMARK_RUN_REAL_CLUSTER=1")


def test_run_task_endpoint_executes_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post("/api/parallel", json={"number": 5})
    assert response.status_code == 200
    payload = response.json()

    assert payload["factorial"] == 120
    assert payload["fibonacci"] == 5
    assert payload["summary"] == "5! is larger, but Fibonacci is 5"


def test_early_return_loop_workflow_with_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the early return + loop workflow when session exists (should execute loop)."""
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    # Provide comma-separated items - should create session and loop over items
    response = client.post(
        "/api/early-return-loop", json={"input_text": "apple, banana, cherry"}
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["had_session"] is True
    assert payload["processed_count"] == 3
    assert payload["all_items"] == ["apple", "banana", "cherry"]


def test_early_return_loop_workflow_early_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the early return + loop workflow when no session (should return early)."""
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    # Use no_session: prefix - should trigger early return without executing loop
    response = client.post(
        "/api/early-return-loop", json={"input_text": "no_session:test"}
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["had_session"] is False
    assert payload["processed_count"] == 0
    assert payload["all_items"] == []


def test_while_loop_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test the while loop workflow executes until the limit."""
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post("/api/while-loop", json={"limit": 4})
    assert response.status_code == 200
    payload = response.json()

    assert payload["limit"] == 4
    assert payload["final"] == 4
    assert payload["iterations"] == 4


def test_zero_division_workflow_catches_the_vm_raised_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A zero denominator raises `ZeroDivisionError` from the VM; the workflow catches it."""
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post("/api/zero-division", json={"denominator": 0})
    assert response.status_code == 200
    payload = response.json()

    assert payload["caught"] is True
    assert payload["quotient"] == -1


def test_zero_division_workflow_divides_normally_when_denominator_is_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A nonzero denominator takes the ordinary division path."""
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post("/api/zero-division", json={"denominator": 5})
    assert response.status_code == 200
    payload = response.json()

    assert payload["caught"] is False
    assert payload["quotient"] == 2


def test_retry_counter_workflow_eventual_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry workflow should succeed when threshold is within max attempts."""
    _require_real_cluster()
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post(
        "/api/retry-counter",
        json={
            "succeed_on_attempt": 3,
            "max_attempts": 4,
            "counter_slot": 901,
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["succeeded"] is True
    assert payload["final_attempt"] == 3
    assert payload["succeed_on_attempt"] == 3
    assert payload["max_attempts"] == 4


def test_retry_counter_workflow_eventual_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry workflow should fail when threshold exceeds max attempts."""
    _require_real_cluster()
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post(
        "/api/retry-counter",
        json={
            "succeed_on_attempt": 5,
            "max_attempts": 3,
            "counter_slot": 902,
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["succeeded"] is False
    assert payload["final_attempt"] == 3
    assert payload["succeed_on_attempt"] == 5
    assert payload["max_attempts"] == 3


def test_timeout_probe_workflow_eventual_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout probe should always fail with timeout after configured attempts."""
    _require_real_cluster()
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post(
        "/api/timeout-probe",
        json={
            "max_attempts": 3,
            "counter_slot": 903,
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["timed_out"] is True
    assert payload["final_attempt"] == 3
    assert payload["timeout_seconds"] == 1
    assert payload["max_attempts"] == 3


def test_timeout_probe_workflow_eventual_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout probe should honor lower max attempts."""
    _require_real_cluster()
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post(
        "/api/timeout-probe",
        json={
            "max_attempts": 2,
            "counter_slot": 904,
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["timed_out"] is True
    assert payload["final_attempt"] == 2
    assert payload["timeout_seconds"] == 1
    assert payload["max_attempts"] == 2


def _wait_for_spawned_instance(schedule_name: str, timeout_seconds: float) -> str:
    """Poll the schedule listing until it reports a spawned instance."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        schedules = asyncio.run(list_schedules())
        for schedule in schedules:
            if (
                schedule.schedule_name == schedule_name
                and schedule.last_instance_id is not None
            ):
                return schedule.last_instance_id
        time.sleep(0.5)
    raise AssertionError("the schedule never spawned an instance")


def test_schedule_fires_and_completes(monkeypatch: pytest.MonkeyPatch) -> None:
    """An interval schedule spawns a run that executes to completion."""
    _require_real_cluster()
    _enable_real_cluster(monkeypatch)

    client = TestClient(app)
    response = client.post(
        "/api/schedule",
        json={
            "workflow_name": "NoOpWorkflow",
            "schedule_type": "interval",
            "interval_seconds": 10,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True, payload
    schedule_name = payload["schedule_name"]

    try:
        instance_id = _wait_for_spawned_instance(schedule_name, timeout_seconds=60.0)
        payload = asyncio.run(wait_for_instance(instance_id))
        assert payload is not None, "the spawned instance never completed"
    finally:
        response = client.post(
            "/api/schedule/delete", json={"workflow_name": "NoOpWorkflow"}
        )
        assert response.status_code == 200
        assert response.json()["success"] is True
