"""Calculate and publish PV, exposure, and CVA for a frozen batch."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task

from risk_pipeline.xva.batch import publish, simulate, value_batch
from risk_pipeline.xva.workflow import calculation_handoff, require_snapshot


@dag(
    dag_id="xva_calculations",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["xva", "calculations"],
)
def xva_calculations():
    """Build the triggered DAG that simulates, values, publishes, and requests batch reports."""
    @task
    def validate_batch():
        """Verify the batch ID and snapshot path supplied in the calculation run configuration."""
        return require_snapshot(get_current_context()["dag_run"].conf or {})

    @task
    def generate_market_scenarios(snapshot_path):
        """Write scenario paths for the frozen batch and return their artifact path through XCom."""
        return simulate(snapshot_path)

    @task
    def calculate_pv_exposure_cva(snapshot_path, scenario_path):
        """Calculate PV, exposure, and CVA from the saved batch artifacts and return the result path."""
        return value_batch(snapshot_path, scenario_path)

    @task(retries=2, retry_delay=timedelta(seconds=10))
    def reconcile_and_publish(snapshot_path, result_path):
        """Publish reconciled results and return the validated handoff for the report DAG."""
        manifest = publish(snapshot_path, result_path)
        return calculation_handoff(snapshot_path, manifest)

    snapshot = validate_batch()
    scenarios = generate_market_scenarios(snapshot)
    results = calculate_pv_exposure_cva(snapshot, scenarios)
    published = reconcile_and_publish(snapshot, results)
    TriggerDagRunOperator(
        task_id="trigger_reports",
        trigger_dag_id="xva_reports",
        conf=published,
        trigger_run_id="xva_reports_postgres__{{ ti.xcom_pull(task_ids='reconcile_and_publish')['batch_id'] }}",
        logical_date=None,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    )


xva_calculations()
