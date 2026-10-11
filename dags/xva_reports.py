"""Reporting can be triggered independently for an already published batch."""

import pendulum
from airflow.sdk import dag, get_current_context, task

from risk_pipeline.xva.reports import generate_reports
from risk_pipeline.xva.workflow import require_publication


@dag(
    dag_id="xva_reports",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/London"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["xva", "reports"],
)
def xva_reports():
    """Build the triggered DAG that generates reports for an already published batch."""
    @task
    def validate_published_batch():
        """Verify the reporting run configuration refers to a published manifest for the requested batch."""
        return require_publication(get_current_context()["dag_run"].conf or {})

    @task
    def create_reports(publication_path):
        """Generate reports from the publication manifest and return the report manifest path."""
        return generate_reports(publication_path)

    create_reports(validate_published_batch())


xva_reports()
