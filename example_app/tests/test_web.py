import os

import pytest
from fastapi.testclient import TestClient

from example_app.web import app


def _enable_real_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.environ.get("RAPPEL_RUN_REAL_CLUSTER") == "1":
        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)


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
