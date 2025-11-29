

import pytest
from fastapi.testclient import TestClient

from example_app.web import app


def test_run_task_endpoint_executes_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    # Disable pytest shortcut mode to actually test the real cluster logic from
    # within the docker container.
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    client = TestClient(app)
    response = client.post("/api/parallel", json={"number": 5})
    assert response.status_code == 200
    payload = response.json()

    assert payload["factorial"] == 120
    assert payload["fibonacci"] == 5
    assert payload["summary"] == "5! is larger, but Fibonacci is 5"
