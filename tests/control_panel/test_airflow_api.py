"""HTTP and Airflow 3.3.2 request-contract tests for the control-panel client."""

import json
from urllib.parse import unquote, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter

from airflow.api_fastapi.core_api.datamodels.dag_run import TriggerDAGRunPostBody
from airflow.api_fastapi.core_api.datamodels.task_instances import ClearTaskInstancesBody
from risk_pipeline.control_panel.airflow_api import AirflowAPI, PanelError


class RecordingAdapter(BaseAdapter):
    """Exercise real Requests URL/header/body preparation against scripted API responses."""

    def __init__(self, replies):
        """Record ordered HTTP exchanges, including authentication retries and mutation requests."""
        self.replies, self.calls = list(replies), []

    def send(self, request, **kwargs):
        """Return the next HTTP response or raise an injected transport failure."""
        self.calls.append((request, kwargs))
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        status, payload = reply
        response = requests.Response()
        response.status_code, response.request = status, request
        response._content = json.dumps(payload).encode()
        return response

    def close(self):
        """Match Requests' adapter lifecycle without owning external resources."""


def client(replies, **credentials):
    """Create an isolated HTTP session that can never contact a real Airflow deployment."""
    adapter = RecordingAdapter(replies)
    session = requests.Session()
    session.mount("https://", adapter)
    return AirflowAPI("https://airflow.test", session=session, **credentials), adapter


def body(call):
    """Decode a prepared JSON request for validation against Airflow's installed API models."""
    return json.loads(call[0].body)


def test_login_and_token_refresh_keep_password_out_of_api_requests():
    """Authenticate once, renew an expired JWT, and keep credentials on the auth endpoint only."""
    api, adapter = client([(201, {"access_token": "first"}), (401, {}), (201, {"access_token": "renewed"}),
                           (200, {"dag_id": "xva_inputs"})], username="operator", password="private-password")
    assert api.dag("xva_inputs")["dag_id"] == "xva_inputs"
    assert [urlsplit(call[0].url).path for call in adapter.calls] == ["/auth/token", "/api/v2/dags/xva_inputs", "/auth/token", "/api/v2/dags/xva_inputs"]
    assert body(adapter.calls[0]) == {"username": "operator", "password": "private-password"}
    assert adapter.calls[1][0].headers["Authorization"] == "Bearer first"
    assert adapter.calls[-1][0].headers["Authorization"] == "Bearer renewed"
    assert all(call[1]["timeout"] == (5, 30) for call in adapter.calls)


def test_trigger_uses_airflow_3_null_logical_date_and_recovers_duplicate_request():
    """Recover a timed-out launch via its exact request ID without creating another batch."""
    configuration = {"valuation_date": "2026-10-14", "generation": {"seed": 42}}
    api, adapter = client([(200, {"is_paused": False}), (409, {}),
                           (200, {"dag_run_id": "fixed", "conf": configuration})], token="secret")
    assert api.trigger("xva_generate_inputs", configuration, "fixed")["dag_run_id"] == "fixed"
    payload = body(adapter.calls[1])
    TriggerDAGRunPostBody.model_validate(payload)
    assert payload["logical_date"] is None and payload["conf"] == configuration
    assert len([call for call in adapter.calls if call[0].method == "POST"]) == 1


def test_duplicate_id_with_other_configuration_rejected():
    """Never treat another request's run as the accepted result of this submission."""
    api, _ = client([(200, {}), (409, {}), (200, {"conf": {"seed": 99}})], token="secret")
    with pytest.raises(PanelError, match="different configuration"):
        api.trigger("xva_inputs", {}, "same")


def test_paused_dag_is_not_triggered():
    """Keep pause/scheduling changes explicit instead of silently enabling workflows on launch."""
    api, adapter = client([(200, {"is_paused": True})], token="secret")
    with pytest.raises(PanelError, match="paused"):
        api.trigger("xva_inputs", {}, "run")
    assert len(adapter.calls) == 1


def test_task_retry_keeps_original_run_and_scopes_downstream_clear():
    """Validate failed-task retry bodies against the installed Airflow 3 model."""
    run = "manual__2026-10-14T10:00:00+01:00"
    api, adapter = client([(200, {"task_instances": [{"task_id": "calculate", "state": "failed", "map_index": -1}], "total_entries": 1}),
                           (200, {"task_instances": []})], token="secret")
    api.retry_task("xva_calculations", run, "calculate")
    payload = body(adapter.calls[-1])
    ClearTaskInstancesBody.model_validate(payload)
    assert payload["dag_run_id"] == run and payload["task_ids"] == ["calculate"]
    assert payload["include_downstream"] and not payload["include_upstream"]
    assert not payload["include_future"] and not payload["include_past"]
    assert not payload["run_on_latest_version"] and payload["prevent_running_task"]
    assert all("/dagRuns" not in call[0].url for call in adapter.calls if call[0].method == "POST")
    assert run in unquote(adapter.calls[0][0].url)


def test_retry_refuses_running_or_missing_task():
    """Avoid clearing tasks that changed state since the dashboard last refreshed."""
    api, adapter = client([(200, {"task_instances": [{"task_id": "calculate", "state": "running"}], "total_entries": 1})], token="secret")
    with pytest.raises(PanelError, match="currently failed"):
        api.retry_task("xva_calculations", "run", "calculate")
    assert len(adapter.calls) == 1


def test_paginated_tasks_logs_and_json_handoffs():
    """Read complete mapped-task lists, native TaskFlow values, and paginated structured logs."""
    api, adapter = client([(200, {"task_instances": [{"task_id": "first"}], "total_entries": 2}),
                           (200, {"task_instances": [{"task_id": "second"}], "total_entries": 2}),
                           (200, {"value": json.dumps({"batch_id": "frozen"})}),
                           (200, {"content": [{"event": "start"}], "continuation_token": "next"}),
                           (200, {"content": ["done"], "continuation_token": None})], token="secret")
    assert len(api.tasks("xva_inputs", "run")) == 2
    assert "offset=1" in adapter.calls[1][0].url
    assert api.xcom("xva_inputs", "run", "freeze") == {"batch_id": "frozen"}
    assert "start" in api.logs("xva_inputs", "run", "freeze", 1)
    assert "token=next" in adapter.calls[-1][0].url


@pytest.mark.parametrize("status", [401, 403, 404, 422, 500, 302])
def test_api_errors_do_not_expose_remote_response_or_bearer_token(status):
    """Render useful errors while excluding sensitive server bodies and request credentials."""
    api, _ = client([(status, {"detail": "secret-server-content"})], token="bearer-secret")
    with pytest.raises(PanelError) as error:
        api.dag("xva_inputs")
    assert "secret" not in str(error.value)


def test_transport_failure_and_missing_credentials_are_actionable():
    """Translate connection errors without exposing raw URLs, credentials, or stack traces."""
    api, _ = client([requests.Timeout("secret-connection-detail")], token="private")
    with pytest.raises(PanelError, match="same form"):
        api.dag("xva_inputs")
    api, _ = client([])
    with pytest.raises(PanelError, match="Configure"):
        api.dag("xva_inputs")


@pytest.mark.parametrize("url", ["file:///tmp", "https://user:password@host", "https://host?token=secret"])
def test_unsafe_configuration_rejected(url):
    """Exclude credentials from configured URLs and allow only the supported HTTP transport."""
    with pytest.raises(PanelError):
        AirflowAPI(url)


def test_graph_scope_can_include_local_utility_dags_without_expanding_streamlit_controls():
    """Keep Streamlit's default scope while allowing a trusted graph catalog to supply its own."""
    api, adapter = client([], token="secret")
    with pytest.raises(PanelError, match="Unknown"):
        api.dag("xcom_demo")
    assert not adapter.calls
    api, adapter = client([(200, {"is_paused": True})], token="secret", allowed_dags={"xcom_demo"})
    with pytest.raises(PanelError, match="xcom_demo is paused"):
        api.trigger("xcom_demo", {}, "one")
    assert len(adapter.calls) == 1
    with pytest.raises(PanelError, match="Unknown"):
        api.dag("xva_inputs")
