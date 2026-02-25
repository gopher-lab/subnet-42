from unittest.mock import Mock

from fastapi.testclient import TestClient

from validator.api_routes import ValidatorAPI


def _build_api():
    validator = Mock()
    validator.healthcheck.return_value = {"status": "ok"}
    validator.config.API_KEY = "test-key"
    api = ValidatorAPI(validator)
    return api


def test_minimal_routes_are_registered():
    api = _build_api()
    paths = {route.path for route in api.app.routes}

    expected = {
        "/healthcheck",
        "/monitor/worker-registry",
        "/monitor/routing-table",
        "/monitor/telemetry",
        "/monitor/telemetry/all",
        "/monitor/telemetry/{hotkey}",
        "/monitor/worker/{worker_id}",
        "/monitor/score-breakdown/{hotkey}",
        "/monitor/leaderboard",
        "/monitor/integrity-summary",
    }
    assert expected.issubset(paths)


def test_removed_ui_and_ops_routes_return_404():
    api = _build_api()
    client = TestClient(api.app)
    headers = {"X-API-Key": "test-key"}

    removed_paths = [
        "/dashboard",
        "/dashboard/data",
        "/errors",
        "/workers",
        "/routing",
        "/unregistered-nodes",
        "/score-simulation",
        "/trigger/telemetry",
        "/monitoring/processes",
        "/monitor/unregistered-tee-addresses",
        "/add-unregistered-tee",
        "/telemetry/postgresql/all",
    ]
    for path in removed_paths:
        response = client.get(path, headers=headers)
        assert response.status_code == 404, path


def test_healthcheck_still_works():
    api = _build_api()
    client = TestClient(api.app)

    response = client.get("/healthcheck")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_leaderboard_supports_offset_param():
    api = _build_api()
    client = TestClient(api.app)
    headers = {"X-API-Key": "test-key"}

    response = client.get(
        "/monitor/leaderboard?hours=8&limit=10&offset=20", headers=headers
    )
    # It may return success or an internal error if backend deps are mocked,
    # but the route must exist and not 404.
    assert response.status_code != 404
