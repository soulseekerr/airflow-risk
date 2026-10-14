"""Business-level execution tracking and launch validation."""

from unittest.mock import Mock

import pytest

from risk_pipeline.control_panel.airflow_api import PanelError
from risk_pipeline.control_panel.service import PanelServices, execution_state


def test_track_exact_generated_chain_instead_of_latest_same_date_run():
    """Follow content-based handoffs even when unrelated newer runs share the same valuation date."""
    api = Mock()
    api.run.side_effect = lambda dag, run: {"dag_id": dag, "dag_run_id": run, "state": "success"}
    tasks = {"xva_generate_inputs": "generate_source_feeds", "xva_inputs": "validate_and_freeze", "xva_calculations": "reconcile_and_publish"}
    api.tasks.side_effect = lambda dag, run: [{"task_id": tasks[dag], "state": "success"}] if dag in tasks else []
    api.xcom.side_effect = [{"generation_id": "exact-generation"}, {"batch_id": "exact-batch"}, {"batch_id": "exact-batch"}]
    stages = PanelServices(api, Mock()).execution("xva_generate_inputs", "root")
    assert [row["dag_run_id"] for row in stages] == ["root", "xva_inputs_generated__exact-generation", "xva_calc_postgres__exact-batch", "xva_reports_postgres__exact-batch"]
    assert execution_state(stages) == "Complete"
    api.runs.assert_not_called()


def test_root_success_is_not_complete_before_report_run_exists():
    """Keep the whole business execution pending while a successful parent awaits its child."""
    api = Mock()
    api.run.side_effect = [{"state": "success"}, PanelError("not created", 404)]
    api.tasks.return_value = [{"task_id": "publish_correction", "state": "success"}]
    api.xcom.return_value = {"rerun_id": "correction"}
    stages = PanelServices(api, Mock()).execution("xva_counterparty_rerun", "root")
    assert stages[-1]["dag_run_id"] == "xva_reports_counterparty__correction"
    assert execution_state(stages) == "In progress"


def test_failed_upstream_is_shown_without_requesting_missing_handoff():
    """Stop at an unfinished frozen-input task and surface the parent failure."""
    api = Mock()
    api.run.return_value = {"state": "failed"}
    api.tasks.return_value = [{"task_id": "validate_and_freeze", "state": "failed"}]
    stages = PanelServices(api, Mock()).execution("xva_inputs", "root")
    assert execution_state(stages) == "Failed"
    api.xcom.assert_not_called()


def test_missing_handoff_does_not_invent_a_child_run():
    """Show waiting when the API has not yet exposed the committed TaskFlow handoff."""
    api = Mock()
    api.run.return_value = {"state": "running"}
    api.tasks.return_value = [{"task_id": "validate_and_freeze", "state": "success"}]
    api.xcom.side_effect = PanelError("missing", 404)
    stages = PanelServices(api, Mock()).execution("xva_inputs", "root")
    assert stages[-1]["dag_run_id"] is None


def test_launch_rejects_paused_dependency_and_invalid_size_before_trigger():
    """Validate the whole dependency chain without submitting a partially launchable workflow."""
    api = Mock()
    api.dag.side_effect = lambda dag: {"is_paused": dag == "xva_reports"}
    services = PanelServices(api, Mock())
    with pytest.raises(PanelError, match="Trade count"):
        services.launch("xva_generate_inputs", {"valuation_date": "2026-10-14", "generation": {"counterparty_count": 5, "trade_count": 2}}, "id")
    with pytest.raises(PanelError, match="Reports is paused"):
        services.launch("xva_generate_inputs", {"valuation_date": "2026-10-14"}, "id")
    api.trigger.assert_not_called()
