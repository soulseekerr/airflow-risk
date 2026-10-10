"""Daily input ingestion, followed by a batch-specific calculation trigger."""

from pathlib import Path

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from risk_pipeline.xva.workflow import prepare_inputs


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
    @task
    def validate_and_freeze():
        context = get_current_context()
        run = context["dag_run"]
        run_type = getattr(run.run_type, "value", run.run_type)
        scheduled_date = None
        if run_type in ("scheduled", "backfill"):
            scheduled_date = context["data_interval_start"].in_timezone("Europe/London").date().isoformat()
        return prepare_inputs(run.conf or {}, Path(__file__).resolve().parents[1], scheduled_date)

    frozen = validate_and_freeze()
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
