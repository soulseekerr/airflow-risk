from pathlib import Path
from unittest.mock import Mock

import pytest

from airflow.dag_processing.dagbag import DagBag
from airflow.sdk.exceptions import DagRunTriggerException
from airflow.timetables.interval import CronDataIntervalTimetable


DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"


@pytest.mark.parametrize("dag_id,trigger_id,upstream,path_key", [
    ("xva_inputs", "trigger_calculations", "validate_and_freeze", "snapshot_path"),
    ("xva_calculations", "trigger_reports", "reconcile_and_publish", "publication_path"),
])
def test_cross_dag_trigger_renders_complete_configuration(dag_id, trigger_id, upstream, path_key):
    """Verify rendered XCom handoffs reach the Airflow trigger request with the correct run ID."""
    dag = DagBag(dag_folder=str(DAGS_FOLDER)).dags[dag_id]
    trigger = dag.get_task(trigger_id)
    handoff = {"batch_id": "2026-10-10-example", path_key: "/tmp/xva/example.json"}
    ti = Mock()
    ti.xcom_pull.return_value = handoff
    ti.get_dag.return_value.is_paused = False
    trigger.render_template_fields({"ti": ti})
    assert trigger.conf == handoff
    assert trigger.trigger_run_id.endswith(handoff["batch_id"])
    assert all(call.kwargs["task_ids"] == upstream for call in ti.xcom_pull.call_args_list)
    with pytest.raises(DagRunTriggerException) as request:
        trigger.execute({"ti": ti})
    assert request.value.conf == handoff
    assert request.value.dag_run_id == trigger.trigger_run_id


def test_no_dag_import_errors():
    """Check that all repository DAGs load without import errors."""
    dag_bag = DagBag(
        dag_folder=str(DAGS_FOLDER),
    )

    assert dag_bag.import_errors == {}


def test_xcom_demo_dag_exists():
    """Check that Airflow discovers the original XCom demo DAG."""
    dag_bag = DagBag(
        dag_folder=str(DAGS_FOLDER),
    )

    # dag = dag_bag.dags.get("xcom_demo")

    # assert dag is not None

    assert "xcom_demo" in dag_bag.dag_ids


def test_xva_dag_chain_and_dependencies():
    """Verify the three XVA DAGs, their schedules, trigger settings, and task dependencies."""
    dag_bag = DagBag(dag_folder=str(DAGS_FOLDER))
    assert "xva_daily_batch" not in dag_bag.dag_ids
    inputs = dag_bag.dags["xva_inputs"]
    calculations = dag_bag.dags["xva_calculations"]
    reports = dag_bag.dags["xva_reports"]
    assert isinstance(inputs.schedule, CronDataIntervalTimetable)
    assert calculations.schedule is None and reports.schedule is None
    assert set(inputs.task_ids) == {"ingest_raw_inputs", "validate_and_freeze", "trigger_calculations"}
    assert inputs.get_task("validate_and_freeze").upstream_task_ids == {"ingest_raw_inputs"}
    assert set(calculations.task_ids) == {"validate_batch", "generate_market_scenarios",
                                          "calculate_pv_exposure_cva", "reconcile_and_publish", "trigger_reports"}
    assert set(reports.task_ids) == {"validate_published_batch", "create_reports"}
    for dag, trigger_id, target, upstream in (
        (inputs, "trigger_calculations", "xva_calculations", "validate_and_freeze"),
        (calculations, "trigger_reports", "xva_reports", "reconcile_and_publish"),
    ):
        trigger = dag.get_task(trigger_id)
        assert trigger.trigger_dag_id == target
        assert trigger.upstream_task_ids == {upstream}
        assert trigger.trigger_rule == "all_success"
        assert trigger.skip_when_already_exists and trigger.fail_when_dag_is_paused
        assert "postgres__" in trigger.trigger_run_id
    assert calculations.get_task("calculate_pv_exposure_cva").upstream_task_ids == {"validate_batch", "generate_market_scenarios"}
    assert reports.get_task("create_reports").upstream_task_ids == {"validate_published_batch"}


def test_independent_feed_dag_is_triggered_only():
    """Verify independent feed arrivals can be ingested without starting a calculation run."""
    dag = DagBag(dag_folder=str(DAGS_FOLDER)).dags["xva_feed_ingestion"]
    assert dag.schedule is None
    assert set(dag.task_ids) == {"ingest_source"}
