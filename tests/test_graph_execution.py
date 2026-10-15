"""Exercise the graph execution API without launching a user's Airflow jobs."""

from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from risk_pipeline.control_panel.airflow_api import PanelError
from risk_pipeline.graph_demo.api import create_app
from risk_pipeline.graph_demo.execution import validate_configuration


@pytest.fixture
def viewer(tmp_path, monkeypatch):
    """Inject a scoped API double while retaining actual route validation and lineage logic."""
    ids = ["xva_generate_inputs", "xva_inputs", "xva_calculations", "xva_reports", "xva_feed_ingestion", "xcom_demo"]
    monkeypatch.setattr("risk_pipeline.graph_demo.api.read_graph", lambda root: {"dags": [{"id": dag} for dag in ids]})
    api = Mock()
    api.dag.return_value = {"is_paused": False}
    client = TestClient(create_app(tmp_path, api_factory=lambda **kwargs: api))
    client.get("/api/graph")
    return client, api


def test_launch_preflights_linked_dags_and_excludes_configuration(viewer):
    """Check dependency availability and send the browser's stable request ID unchanged."""
    client, api = viewer
    body = {"dag_id": "xva_generate_inputs", "run_id": "riskflow__same-request", "configuration": {"valuation_date": "2026-10-15"}}
    api.trigger.return_value = {**body, "dag_run_id": body["run_id"], "state": "queued", "conf": {"private": "detail"}}
    result = client.post("/api/runs", json=body)
    assert result.status_code == 200
    assert "conf" not in result.json()
    assert [call.args[0] for call in api.dag.call_args_list] == ["xva_generate_inputs", "xva_inputs", "xva_calculations", "xva_reports"]
    api.trigger.assert_called_once_with(body["dag_id"], body["configuration"], body["run_id"])


def test_paused_child_blocks_launch_without_enabling_schedule(viewer):
    """Refuse a partial execution rather than silently unpausing a scheduled DAG."""
    client, api = viewer
    api.dag.side_effect = [{"is_paused": False}, {"is_paused": True}]
    result = client.post("/api/runs", json={"dag_id": "xva_generate_inputs", "run_id": "one", "configuration": {"valuation_date": "2026-10-15"}})
    assert result.status_code == 400
    assert "xva_inputs is paused" in result.json()["detail"]
    api.trigger.assert_not_called()
    api.unpause.assert_not_called()


def test_lineage_reads_exact_child_and_omits_handoff_payload(viewer):
    """Resolve reports from the published batch XCom even if recent runs contain other jobs."""
    client, api = viewer
    api.run.side_effect = [
        {"state": "success", "conf": {"secret": "not exposed"}},
        {"state": "running"},
    ]
    api.tasks.side_effect = [
        [{"task_id": "reconcile_and_publish", "state": "success", "try_number": 1}],
        [{"task_id": "create_reports", "state": "running", "try_number": 2, "map_index": -1}],
    ]
    api.xcom.return_value = {"batch_id": "exact-batch", "publication_path": "/worker/private"}
    result = client.get("/api/execution", params={"dag_id": "xva_calculations", "run_id": "manual__2026-10-15T00:00:00+00:00"})
    assert result.status_code == 200
    stages = result.json()["stages"]
    assert stages[1]["dag_run_id"] == "xva_reports_postgres__exact-batch"
    assert stages[1]["tasks"][0]["try_number"] == 2
    assert "private" not in result.text and "secret" not in result.text
    api.runs.assert_not_called()


def test_retry_passes_exact_run_task_and_mapped_instance(viewer):
    """Delegate the actual failed-state recheck and downstream clearing to the shared client."""
    client, api = viewer
    body = {"dag_id": "xva_calculations", "run_id": "original-run", "task_id": "calculate_pv_exposure_cva", "map_index": 4}
    assert client.post("/api/retry", json=body).status_code == 200
    api.retry_task.assert_called_once_with(body["dag_id"], body["run_id"], body["task_id"], 4)
    api.retry_task.side_effect = PanelError("Only a currently failed task can be retried here.")
    assert client.post("/api/retry", json=body).status_code == 400


def test_logs_validate_actual_instance_and_attempt(viewer):
    """Reject nonexistent attempts instead of forwarding arbitrary task/log paths."""
    client, api = viewer
    api.tasks.return_value = [{"task_id": "calculate", "try_number": 2, "map_index": 3}]
    api.logs.return_value = "calculation issue"
    query = {"dag_id": "xva_calculations", "run_id": "original", "task_id": "calculate", "map_index": 3, "attempt": 2}
    assert client.get("/api/logs", params=query).json() == {"content": "calculation issue"}
    api.logs.assert_called_once_with("xva_calculations", "original", "calculate", 2, 3)
    assert client.get("/api/logs", params={**query, "attempt": 3}).status_code == 404
    assert client.get("/api/logs", params={**query, "map_index": -1}).status_code == 404


def test_scope_and_browser_origin_are_checked_before_writes(viewer):
    """Do not expose arbitrary remote DAGs or accept execution submissions from other sites."""
    client, api = viewer
    assert client.get("/api/runs", params={"dag_id": "other-project"}).status_code == 404
    body = {"dag_id": "xcom_demo", "run_id": "one"}
    assert client.post("/api/runs", json=body, headers={"Origin": "https://another.site"}).status_code == 403
    assert client.post("/api/runs", json=body, headers={"Content-Type": "text/plain"}).status_code in (415, 422)
    api.trigger.assert_not_called()
    assert client.post("/api/runs", json={**body, "run_id": "../bad"}).status_code == 422


def test_run_list_is_sanitized_and_api_errors_are_actionable(viewer):
    """Keep configuration out of list responses while preserving sanitized API failures."""
    client, api = viewer
    api.runs.return_value = [{"dag_run_id": "exact", "state": "success", "conf": {"password": "private"}}]
    result = client.get("/api/runs", params={"dag_id": "xcom_demo"})
    assert result.json()["runs"][0]["dag_run_id"] == "exact"
    assert "private" not in result.text
    api.runs.side_effect = PanelError("Airflow could not be reached.", 503)
    assert client.get("/api/runs", params={"dag_id": "xcom_demo"}).status_code == 503


@pytest.mark.parametrize("dag,conf", [
    ("xva_calculations", {}), ("xva_reports", {"batch_id": "one"}),
    ("xva_inputs", {"valuation_date": "not a date"}),
    ("xva_generate_inputs", {"valuation_date": "2026-10-15", "generation": []}),
    ("xva_generate_inputs", {"valuation_date": "2026-10-15", "generation": {"trade_count": 1}}),
    ("xva_counterparty_rerun", {"valuation_date": "2026-10-15", "counterparty_code": "1234567", "feeds": {}}),
    ("xva_feed_ingestion", {"valuation_date": "2026-10-15", "feed_type": "trades"}),
])
def test_incomplete_launch_configuration_is_rejected(dag, conf):
    """Catch common incomplete direct-run configurations before Airflow allocates a run."""
    with pytest.raises(PanelError):
        validate_configuration(dag, conf)
