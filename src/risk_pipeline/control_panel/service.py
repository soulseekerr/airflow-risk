"""Business operations and exact cross-DAG execution tracking for the control panel."""

from dataclasses import dataclass
from datetime import date

from risk_pipeline.control_panel.airflow_api import AirflowAPI, PanelError, WORKFLOWS
from risk_pipeline.control_panel.results import ResultsStore


# These identifiers are the same explicit trigger contracts used by the repository DAGs.
HANDOFFS = {
    "xva_generate_inputs": ("generate_source_feeds", "xva_inputs", "generation_id", "xva_inputs_generated__"),
    "xva_inputs": ("validate_and_freeze", "xva_calculations", "batch_id", "xva_calc_postgres__"),
    "xva_calculations": ("reconcile_and_publish", "xva_reports", "batch_id", "xva_reports_postgres__"),
    "xva_counterparty_rerun": ("publish_correction", "xva_reports", "rerun_id", "xva_reports_counterparty__"),
}


@dataclass
class PanelServices:
    """Keep API writes separate from read-only financial data queries."""

    api: AirflowAPI
    results: ResultsStore

    def launch(self, dag_id, configuration, run_id):
        """Validate a request and check its dependent workflows before creating the first run."""
        if dag_id == "xva_generate_inputs":
            options = configuration.get("generation", {})
            if options.get("trade_count", 10000) < options.get("counterparty_count", 2000):
                raise PanelError("Trade count must be at least the counterparty count.")
        if dag_id == "xva_counterparty_rerun" and (not configuration.get("feeds") or len(configuration.get("counterparty_code", "")) != 7):
            raise PanelError("Choose a counterparty and supply at least one corrected feed.")
        if not configuration.get("valuation_date"):
            raise PanelError("Choose a valuation date.")
        date.fromisoformat(configuration["valuation_date"])
        current = dag_id
        while True:
            if self.api.dag(current).get("is_paused"):
                raise PanelError(f"{WORKFLOWS[current]} is paused. Enable it in Workflow controls first.")
            if current not in HANDOFFS:
                break
            current = HANDOFFS[current][1]
        return self.api.trigger(dag_id, configuration, run_id)

    def execution(self, dag_id, run_id):
        """Follow frozen handoffs to exact child runs, including children outside recent-run windows."""
        return read_execution(self.api, dag_id, run_id)


def read_execution(api, dag_id, run_id):
    """Follow exact frozen run handoffs for both browser control panels."""
    stages = []
    while True:
        try:
            run = api.run(dag_id, run_id)
        except PanelError as error:
            if error.status != 404 or not stages:
                raise
            stages.append({"dag_id": dag_id, "dag_run_id": run_id, "state": "waiting", "tasks": []})
            break
        tasks = api.tasks(dag_id, run_id)
        stages.append({**run, "dag_id": dag_id, "dag_run_id": run_id, "tasks": tasks})
        if dag_id not in HANDOFFS:
            break
        task_id, child_dag, field, prefix = HANDOFFS[dag_id]
        completed = any(task["task_id"] == task_id and task.get("state") == "success" for task in tasks)
        if not completed:
            stages.append({"dag_id": child_dag, "dag_run_id": None, "state": "waiting", "tasks": []})
            break
        try:
            handoff = api.xcom(dag_id, run_id, task_id)
        except PanelError as error:
            if error.status != 404:
                raise
            stages.append({"dag_id": child_dag, "dag_run_id": None, "state": "waiting", "tasks": []})
            break
        identifier = handoff.get(field)
        if not isinstance(identifier, str) or not identifier:
            raise PanelError("A completed workflow has an incomplete downstream handoff.")
        dag_id, run_id = child_dag, prefix + identifier
    return stages


def execution_state(stages):
    """Require the report run to succeed before calling the whole business execution complete."""
    if any(stage.get("state") == "failed" for stage in stages):
        return "Failed"
    if stages and stages[-1]["dag_id"] == "xva_reports" and all(stage.get("state") == "success" for stage in stages):
        return "Complete"
    return "In progress"


def create_services():
    """Build one server-side service pair for a Streamlit browser session."""
    return PanelServices(AirflowAPI.from_environment(), ResultsStore.from_environment())
