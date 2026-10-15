"""Verify that the browser demo exposes actual DAG structure without allowing definition edits."""

from pathlib import Path

from fastapi.testclient import TestClient

from risk_pipeline.graph_demo.api import create_app


REPOSITORY = Path(__file__).resolve().parents[1]


def test_graph_matches_repository_task_and_cross_dag_dependencies(tmp_path):
    """Read the real calculation branches and literal TriggerDagRunOperator targets."""
    client = TestClient(create_app(REPOSITORY, tmp_path / "unbuilt"))
    response = client.get("/api/graph")
    assert response.status_code == 200
    graph = response.json()
    assert graph["read_only"] is True
    dags = {dag["id"]: dag for dag in graph["dags"]}
    assert set(dags) == {"xcom_demo", "xva_calculations", "xva_inputs", "xva_reports", "xva_generate_inputs",
                         "xva_feed_ingestion", "xva_counterparty_rerun"}
    calculation = dags["xva_calculations"]
    tasks = {task["id"]: task for task in calculation["tasks"]}
    assert set(tasks["calculate_pv_exposure_cva"]["upstream"]) == {"validate_batch", "generate_market_scenarios"}
    assert tasks["reconcile_and_publish"]["retries"] == 2
    assert tasks["trigger_reports"]["target_dag"] == "xva_reports"
    assert len(calculation["edges"]) == 6
    assert {tuple(link[key] for key in ("source", "target")) for link in graph["links"]} == {
        ("xva_generate_inputs", "xva_inputs"), ("xva_inputs", "xva_calculations"),
        ("xva_calculations", "xva_reports"), ("xva_counterparty_rerun", "xva_reports")}


def test_definitions_are_get_only_and_build_instructions_are_available(tmp_path):
    """Reject mutations and provide startup guidance without an installed frontend bundle."""
    client = TestClient(create_app(REPOSITORY, tmp_path / "unbuilt"))
    assert client.get("/api/health").json() == {"status": "ok", "definitions_read_only": True, "execution_enabled": True}
    assert "npm run build" in client.get("/").json()["message"]
    for method in ("post", "put", "patch", "delete"):
        assert client.request(method.upper(), "/api/graph", json={"nodes": []}).status_code == 405


def test_import_errors_are_reported_without_exposing_source_tracebacks(tmp_path):
    """Surface broken local definitions as a recoverable response instead of a fake graph."""
    directory = tmp_path / "dags"
    directory.mkdir()
    (directory / "broken.py").write_text("from airflow.sdk import DAG\nraise RuntimeError('private import detail')\n")
    response = TestClient(create_app(tmp_path)).get("/api/graph")
    assert response.status_code == 503
    assert "private import detail" not in response.text


def test_built_frontend_is_served_from_same_origin(tmp_path):
    """Serve the browser entry point while preserving the API and refusing writes to assets."""
    assets = tmp_path / "dist"
    assets.mkdir()
    (assets / "index.html").write_text("<html><title>Riskflow</title></html>")
    client = TestClient(create_app(REPOSITORY, assets))
    assert "Riskflow" in client.get("/").text
    assert client.get("/api/health").status_code == 200
    assert client.post("/").status_code == 405
