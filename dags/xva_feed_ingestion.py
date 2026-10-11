"""Stage one independent XVA source; portfolio assembly references its explicit feed ID later."""

from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task

from risk_pipeline.xva.workflow import ingest_one_feed


@dag(
    dag_id="xva_feed_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["xva", "feeds"],
)
def xva_feed_ingestion():
    """Build a reusable ingestion DAG run separately for each independently arriving feed."""
    @task
    def ingest_source():
        """Persist one feed version and expose its ID for explicit input-set assembly."""
        context = get_current_context()
        run = context["dag_run"]
        return ingest_one_feed(run.conf or {}, Path(__file__).resolve().parents[1],
                               request_key=f"xva_feed_ingestion:{run.run_id}")

    ingest_source()


xva_feed_ingestion()
