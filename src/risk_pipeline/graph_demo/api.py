"""Expose code-defined DAGs and explicit Airflow execution controls to the browser."""

from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
import os
import logging

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

from risk_pipeline.control_panel.airflow_api import PanelError
from risk_pipeline.graph_demo.execution import install_execution


TITLES = {
    "xva_generate_inputs": "Generate portfolio",
    "xva_inputs": "Inputs & validation",
    "xva_calculations": "PV, exposure & CVA",
    "xva_reports": "Reports",
    "xva_counterparty_rerun": "Counterparty correction",
    "xva_feed_ingestion": "Stage a source feed",
    "xcom_demo": "XCom demo",
}
PARSE_LOCK = Lock()


def read_graph(repository):
    """Parse trusted local DAG files and extract tasks and explicit trigger dependencies."""
    from airflow.dag_processing.dagbag import DagBag

    with PARSE_LOCK:
        bag = DagBag(dag_folder=str(Path(repository) / "dags"))
    if bag.import_errors:
        raise ValueError("One or more repository DAG files could not be parsed")
    dags, links = [], []
    for dag_id, dag in sorted(bag.dags.items()):
        tasks = []
        for task in sorted(dag.tasks, key=lambda value: value.task_id):
            target = getattr(task, "trigger_dag_id", None)
            function = getattr(task, "python_callable", None)
            description = getattr(function, "__doc__", None) or task.doc_md or ""
            tasks.append({"id": task.task_id, "label": task.task_id.replace("_", " "),
                          "kind": "trigger" if target else "python" if function else "task",
                          "operator": task.task_type, "description": description.strip(),
                          "upstream": sorted(task.upstream_task_ids), "downstream": sorted(task.downstream_task_ids),
                          "retries": task.retries, "trigger_rule": getattr(task.trigger_rule, "value", str(task.trigger_rule)),
                          "target_dag": target if isinstance(target, str) else None})
            if isinstance(target, str):
                links.append({"source": dag_id, "target": target, "task_id": task.task_id})
        schedule = dag.schedule
        schedule_label = "Triggered" if schedule is None else getattr(schedule, "summary", str(schedule))
        if schedule_label in ("@daily", "0 0 * * *"):
            schedule_label = "Daily"
        dags.append({"id": dag_id, "label": TITLES.get(dag_id, dag_id.replace("_", " ").title()),
                     "description": dag.description or "", "file": f"dags/{Path(dag.fileloc).name}",
                     "schedule": schedule_label, "tags": sorted(dag.tags), "tasks": tasks,
                     "edges": [{"source": source, "target": task["id"]} for task in tasks for source in task["upstream"]]})
    return {"project": "airflow-risk", "read_only": True, "source": "Local DAG definitions",
            "loaded_at": datetime.now(timezone.utc).isoformat(), "dags": dags, "links": links}


def create_app(repository=None, frontend=None, api_factory=None):
    """Serve immutable definitions and explicit execution controls from the same origin."""
    root = Path(repository or os.getenv("XVA_PROJECT_ROOT", Path(__file__).resolve().parents[3])).resolve()
    assets = Path(frontend or root / "web/dag-viewer/dist")
    app = FastAPI(title="XVA DAG viewer", docs_url=None, redoc_url=None, openapi_url=None)

    catalog = {}

    @app.exception_handler(PanelError)
    async def operational_error(request, error):
        """Return only sanitized operational messages from the Airflow client."""
        return JSONResponse(status_code=error.status or 400, content={"detail": str(error)})

    install_execution(app, lambda: catalog, api_factory)

    @app.get("/api/health")
    def health():
        """Report that the viewer backend is running without contacting Airflow or PostgreSQL."""
        return {"status": "ok", "definitions_read_only": True, "execution_enabled": True}

    @app.get("/api/graph")
    async def graph():
        """Return the actual repository graph or an actionable installation error."""
        try:
            result = read_graph(root)
            catalog.clear()
            catalog.update(result)
            return result
        except Exception:
            logging.getLogger(__name__).exception("Could not load local DAG definitions")
            raise HTTPException(503, "DAG definitions could not be loaded. Check the Airflow installation and DAG import errors.") from None

    if assets.is_dir():
        app.mount("/", StaticFiles(directory=assets, html=True), name="viewer")
    else:
        @app.get("/")
        def setup():
            """Explain how to build the optional frontend when its local bundle is absent."""
            return {"message": "Build the viewer first: cd web/dag-viewer && npm ci && npm run build"}
    return app


app = create_app()
