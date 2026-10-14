"""Recalculate one counterparty using corrected feeds and refresh the full-date reports."""

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task

from risk_pipeline.xva.batch import read_json
from risk_pipeline.xva.reruns import calculate_counterparty, prepare_counterparty_rerun, publish_counterparty


@dag(dag_id="xva_counterparty_rerun", schedule=None,
     start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"), catchup=False,
     max_active_runs=1, is_paused_upon_creation=False, render_template_as_native_obj=True,
     tags=["xva", "counterparty", "rerun"])
def xva_counterparty_rerun():
    """Build the scoped input-validation, calculation, publication, and report-trigger workflow."""
    @task
    def validate_updated_inputs():
        """Stage corrected feed versions and verify only the chosen counterparty changed."""
        run = get_current_context()["dag_run"]
        return prepare_counterparty_rerun(run.conf or {}, Path(__file__).resolve().parents[1],
                                         request_key=f"xva_counterparty_rerun:{run.run_id}")

    @task
    def calculate_updated_counterparty(handoff):
        """Recompute this counterparty's PV, exposures, and CVA using the frozen corrected inputs."""
        return calculate_counterparty(handoff)

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def publish_correction(handoff, result_path):
        """Commit all counterparty outputs together and return the revised reporting configuration."""
        publication = publish_counterparty(handoff, result_path)
        manifest = read_json(publication)
        return {"batch_id": manifest["batch_id"], "publication_path": publication, "rerun_id": handoff["rerun_id"]}

    handoff = validate_updated_inputs()
    corrected = publish_correction(handoff, calculate_updated_counterparty(handoff))
    TriggerDagRunOperator(task_id="trigger_reports", trigger_dag_id="xva_reports", conf=corrected,
                         trigger_run_id="xva_reports_counterparty__{{ ti.xcom_pull(task_ids='publish_correction')['rerun_id'] }}",
                         logical_date=None, skip_when_already_exists=True, fail_when_dag_is_paused=True)


xva_counterparty_rerun()
