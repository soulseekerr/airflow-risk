"""Explicit execution controls for locally defined DAGs, using Airflow's stable API."""

from datetime import date
from threading import RLock
from urllib.parse import urlsplit

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from risk_pipeline.control_panel.airflow_api import AirflowAPI, PanelError
from risk_pipeline.control_panel.service import HANDOFFS, read_execution


class Launch(BaseModel):
    """Keep browser launch requests separate from immutable DAG definitions."""

    model_config = ConfigDict(extra="forbid")
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250, pattern=r"^[A-Za-z0-9_.:+~-]+$")
    configuration: dict = Field(default_factory=dict)


class Retry(BaseModel):
    """Identify a single failed task instance in its original run."""

    model_config = ConfigDict(extra="forbid")
    dag_id: str = Field(min_length=1, max_length=250)
    run_id: str = Field(min_length=1, max_length=250)
    task_id: str = Field(min_length=1, max_length=250)
    map_index: int = Field(default=-1, ge=-1)


def public_run(run):
    """Exclude run configuration and other unnecessary API fields from the browser response."""
    return {key: run.get(key) for key in (
        "dag_id", "dag_run_id", "state", "logical_date", "run_after", "start_date", "end_date",
    )}


def validate_configuration(dag_id, conf):
    """Reject incomplete direct launches before they create predictably failing runs."""
    if dag_id in {"xva_generate_inputs", "xva_inputs", "xva_counterparty_rerun", "xva_feed_ingestion"}:
        try:
            date.fromisoformat(conf.get("valuation_date", ""))
        except (ValueError, TypeError):
            raise PanelError("Supply valuation_date as YYYY-MM-DD.") from None
    required = {
        "xva_calculations": ("batch_id", "snapshot_path"),
        "xva_reports": ("batch_id", "publication_path"),
        "xva_feed_ingestion": ("feed_type", "input_path"),
    }.get(dag_id, ())
    if any(not isinstance(conf.get(key), str) or not conf[key].strip() for key in required):
        raise PanelError("This workflow requires: " + ", ".join(required) + ". Reuse an existing upstream handoff.")
    if dag_id == "xva_counterparty_rerun":
        if not isinstance(conf.get("counterparty_code"), str) or len(conf["counterparty_code"]) != 7 or not isinstance(conf.get("feeds"), dict) or not conf["feeds"]:
            raise PanelError("Supply a seven-character counterparty_code and corrected feeds (paths or feed IDs).")
    if dag_id == "xva_generate_inputs":
        options = conf.get("generation", {})
        if not isinstance(options, dict):
            raise PanelError("generation must be an object.")
        counts = [options.get("counterparty_count", 2000), options.get("trade_count", 10000), options.get("simulation_paths", 20)]
        if any(type(value) is not int or value < 1 for value in counts) or counts[1] < counts[0]:
            raise PanelError("Generation counts must be positive integers, with at least one trade per counterparty.")


def install_execution(app, catalog, api_factory=None):
    """Register scoped operations; serialize the shared authenticated HTTP session."""
    lock = RLock()
    client = None

    def api(dag_id):
        """Allow only DAGs present in the trusted local catalog, never arbitrary browser paths."""
        nonlocal client
        ids = {dag["id"] for dag in catalog().get("dags", [])}
        if dag_id not in ids:
            raise PanelError("Unknown local workflow.", 404)
        if client is None:
            client = (api_factory or AirflowAPI.from_environment)(allowed_dags=ids)
        client.allowed_dags = frozenset(ids)
        return client

    def mutation(request):
        """Reject cross-origin writes and non-JSON form submissions to local execution controls."""
        origin = request.headers.get("origin")
        if origin and urlsplit(origin).netloc != request.headers.get("host"):
            raise HTTPException(403, "Cross-origin execution requests are not allowed.")
        if request.headers.get("content-type", "").split(";")[0] != "application/json":
            raise HTTPException(415, "Execution requests require application/json.")

    @app.get("/api/runs")
    def runs(dag_id: str):
        """List recent exact run identifiers for a selected local workflow."""
        with lock:
            return {"runs": [public_run(run) for run in api(dag_id).runs(dag_id)]}

    @app.get("/api/execution")
    def execution(dag_id: str, run_id: str):
        """Read exact linked stages and task instances without returning handoff payloads."""
        with lock:
            stages = read_execution(api(dag_id), dag_id, run_id)
            return {"stages": [{**public_run(stage), "tasks": [
                {key: task.get(key) for key in ("task_id", "state", "try_number", "map_index", "start_date", "end_date", "duration")}
                for task in stage["tasks"]]} for stage in stages]}

    @app.get("/api/logs")
    def logs(dag_id: str, run_id: str, task_id: str, attempt: int = 1, map_index: int = -1):
        """Read logs only for an actual task instance and a valid recorded attempt."""
        with lock:
            service = api(dag_id)
            task = next((row for row in service.tasks(dag_id, run_id)
                         if row["task_id"] == task_id and row.get("map_index", -1) == map_index), None)
            if not task or attempt < 1 or attempt > (task.get("try_number") or 0):
                raise PanelError("Task instance or log attempt not found.", 404)
            return {"content": service.logs(dag_id, run_id, task_id, attempt, map_index)}

    @app.post("/api/runs")
    def launch(body: Launch, request: Request):
        """Launch an existing workflow with an idempotent browser-generated request ID."""
        mutation(request)
        with lock:
            service = api(body.dag_id)
            validate_configuration(body.dag_id, body.configuration)
            current = body.dag_id
            while True:
                api(current)
                if service.dag(current).get("is_paused"):
                    raise PanelError(f"{current} is paused. Unpause it in Airflow first; this may also enable its schedule.")
                if current not in HANDOFFS:
                    break
                current = HANDOFFS[current][1]
            return public_run(service.trigger(body.dag_id, body.configuration, body.run_id))

    @app.post("/api/retry")
    def retry(body: Retry, request: Request):
        """Clear a currently failed task and downstream tasks while preserving the run configuration."""
        mutation(request)
        with lock:
            api(body.dag_id).retry_task(body.dag_id, body.run_id, body.task_id, body.map_index)
            return {"status": "accepted"}
