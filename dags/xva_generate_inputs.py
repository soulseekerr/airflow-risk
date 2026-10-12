"""Generate a scalable synthetic portfolio, then run the existing input/calculation/report chain."""

from pathlib import Path

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, get_current_context, task

from risk_pipeline.xva.workflow import generate_input_sources


@dag(
    dag_id="xva_generate_inputs",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["xva", "generation"],
)
def xva_generate_inputs():
    """Build the intermediate generation layer without sending portfolio rows through XCom."""
    @task
    def generate_source_feeds():
        """Create deterministic source files using the run's London calendar date unless explicitly overridden."""
        run = get_current_context()["dag_run"]
        run_date = pendulum.instance(run.run_after).in_timezone("Europe/London").date().isoformat()
        return generate_input_sources(run.conf or {}, Path(__file__).resolve().parents[1],
                                      generation_key=f"xva_generate_inputs:{run.run_id}", run_date=run_date)

    generated = generate_source_feeds()
    TriggerDagRunOperator(
        task_id="trigger_inputs",
        trigger_dag_id="xva_inputs",
        conf=generated,
        trigger_run_id="xva_inputs_generated__{{ ti.xcom_pull(task_ids='generate_source_feeds')['generation_id'] }}",
        logical_date=None,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    )


xva_generate_inputs()
