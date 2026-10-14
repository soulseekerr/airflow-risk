"""Daily synthetic input generation and ingestion, followed by a calculation trigger."""

from pathlib import Path

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from risk_pipeline.xva.workflow import ingest_inputs
from risk_pipeline.xva.ingestion import validate_ingestion


@dag(
    dag_id="xva_inputs",
    schedule=CronDataIntervalTimetable("@daily", timezone="Europe/London"),
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    tags=["xva", "inputs"],
)
def xva_inputs():
    """Build the daily input DAG that freezes a batch and triggers its calculation run."""
    @task
    def ingest_raw_inputs():
        """Commit raw submissions to PostgreSQL before business validation, preserving retry identity."""
        context = get_current_context()
        run = context["dag_run"]
        run_type = getattr(run.run_type, "value", run.run_type)
        scheduled_date = None
        if run_type in ("scheduled", "backfill"):
            scheduled_date = context["data_interval_start"].in_timezone("Europe/London").date().isoformat()
        configuration = dict(run.conf or {})
        if scheduled_date is None and (configuration.get("generate_demo") or not any(
                field in configuration for field in ("feeds", "input_path", "ingestion_id"))):
            # Anchor generation to this run, even if a retry executes on a later day.
            configuration.setdefault("valuation_date", pendulum.instance(run.run_after)
                                     .in_timezone("Europe/London").date().isoformat())
        return ingest_inputs(configuration, Path(__file__).resolve().parents[1], scheduled_date,
                             request_key=f"{context['dag'].dag_id}:{run.run_id}")

    @task
    def validate_and_freeze(ingestion):
        """Record accepted/rejected outcomes and freeze the selected database snapshot for calculations."""
        return validate_ingestion(ingestion)

    frozen = validate_and_freeze(ingest_raw_inputs())
    TriggerDagRunOperator(
        task_id="trigger_calculations",
        trigger_dag_id="xva_calculations",
        conf=frozen,
        trigger_run_id="xva_calc_postgres__{{ ti.xcom_pull(task_ids='validate_and_freeze')['batch_id'] }}",
        logical_date=None,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    )


xva_inputs()
