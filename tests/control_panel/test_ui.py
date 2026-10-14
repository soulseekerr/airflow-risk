"""Exercise Streamlit navigation and forms without launching real Airflow runs."""

from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
from streamlit.testing.v1 import AppTest

from risk_pipeline.control_panel import ui
from risk_pipeline.control_panel.airflow_api import PanelError


APP = Path(__file__).resolve().parents[2] / "streamlit_app.py"


@pytest.fixture
def panel(monkeypatch):
    """Provide published values and stubbed API operations for real Streamlit widget interactions."""
    backend = Mock()
    backend.results.dates.return_value = [{"valuation_date": date(2026, 10, 14), "batch_id": "batch",
        "currency": "EUR", "counterparty_count": 2, "trade_count": 3, "pv_t0": 50,
        "cva_calculated": 10, "cva_adjusted": 12}]
    backend.results.counterparties.return_value = [{"counterparty_code": "0000001", "counterparty_name": "Synthetic Bank",
        "trade_count": 3, "pv_t0": 50, "value_calc": 10, "value_adj": 12, "updated_at": None}]
    backend.results.details.return_value = ([{"trade_id": "IRS1", "ma_id": "MA1", "pv": 50}], [])
    backend.results.corrections.return_value = []
    backend.results.feeds.return_value = []
    backend.results.downloads.return_value = {"summary.md": b"# XVA report"}
    backend.results.publication.return_value = {"batch_id": "batch", "publication_path": "/shared/batch/published.json"}
    backend.api.runs.return_value = []
    backend.launch.return_value = {"dag_run_id": "accepted"}
    monkeypatch.setattr(ui, "create_services", lambda: backend)
    return AppTest.from_file(str(APP), default_timeout=15), backend


def navigate(app, page):
    """Select a business page through the rendered sidebar widget."""
    app.sidebar.radio[0].set_value(page).run()
    assert not app.exception


def test_overview_and_read_pages_never_submit_computations(panel):
    """Render published financial values, navigate, and download without hidden workflow writes."""
    app, backend = panel
    app.run()
    assert not app.exception
    assert any(metric.value == "10.00" for metric in app.metric)
    for page in ("Counterparty results", "Correct and rerun", "Reports", "Executions"):
        navigate(app, page)
    backend.launch.assert_not_called()
    backend.api.trigger.assert_not_called()
    backend.api.unpause.assert_not_called()


def test_launch_form_passes_synthetic_parameters_once(panel):
    """Launch only on explicit submission and retain the accepted root for linked monitoring."""
    app, backend = panel
    app.run()
    navigate(app, "Launch batch")
    next(widget for widget in app.number_input if widget.label == "Counterparties").set_value(5)
    next(widget for widget in app.number_input if widget.label == "Trades").set_value(20)
    next(button for button in app.button if button.label == "Launch batch").click().run()
    assert not app.exception
    dag_id, configuration, run_id = backend.launch.call_args.args
    assert dag_id == "xva_generate_inputs" and configuration["generation"]["counterparty_count"] == 5
    assert configuration["generation"]["trade_count"] == 20 and run_id.startswith("xva_ui__")
    app.run()
    backend.launch.assert_called_once()


def test_uncertain_launch_retry_uses_same_run_id(panel):
    """Prevent a network timeout from producing duplicate independent batches on the next click."""
    app, backend = panel
    backend.launch.side_effect = [PanelError("Airflow could not be reached"), {"dag_run_id": "accepted"}]
    app.run()
    navigate(app, "Launch batch")
    next(button for button in app.button if button.label == "Launch batch").click().run()
    first_id = backend.launch.call_args.args[2]
    assert app.error and not app.exception
    next(button for button in app.button if button.label == "Launch batch").click().run()
    assert backend.launch.call_args.args[2] == first_id and app.success


def test_counterparty_correction_submits_exact_feed_id(panel):
    """Select an existing counterparty and submit a corrected feed reference without editing the database."""
    app, backend = panel
    app.run()
    navigate(app, "Correct and rerun")
    next(widget for widget in app.text_input if widget.label == "Trades feed id").set_value("corrected-trades-feed")
    next(button for button in app.button if button.label == "Rerun counterparty").click().run()
    assert not app.exception
    dag_id, configuration, _ = backend.launch.call_args.args
    assert dag_id == "xva_counterparty_rerun"
    assert configuration == {"valuation_date": "2026-10-14", "counterparty_code": "0000001", "feeds": {"trades": {"feed_id": "corrected-trades-feed"}}}


def test_backend_errors_are_visible_without_traceback_or_credentials(panel):
    """Keep unavailable database and missing reports recoverable within the UI."""
    app, backend = panel
    backend.results.dates.side_effect = PanelError("Results database unavailable")
    app.run()
    assert not app.exception and app.error[0].value == "Results database unavailable"
    navigate(app, "Launch batch")
    assert any(button.label == "Launch batch" for button in app.button)


def test_execution_monitor_renders_linked_failure_and_retries_original_task(panel):
    """Expose task diagnostics while keeping retry tied to the selected stage's original run."""
    app, backend = panel
    backend.api.runs.return_value = [{"dag_run_id": "original", "state": "failed", "run_after": "2026-10-14T10:00:00Z", "conf": {"valuation_date": "2026-10-14"}}]
    backend.execution.return_value = [{"dag_id": "xva_calculations", "dag_run_id": "calculation-original", "state": "failed", "tasks": [
        {"task_id": "calculate_pv_exposure_cva", "state": "failed", "try_number": 1, "map_index": -1}]}]
    backend.api.logs.return_value = "example task log"
    app.run()
    navigate(app, "Executions")
    assert any(metric.value == "Failed" for metric in app.metric)
    next(button for button in app.button if button.label == "Read task logs").click().run()
    assert any("example task log" in code.value for code in app.code)
    app.run()
    assert any("example task log" in code.value for code in app.code)
    backend.api.logs.assert_called_once()
    next(button for button in app.button if button.label == "Retry task and downstream").click().run()
    backend.api.retry_task.assert_called_once_with("xva_calculations", "calculation-original", "calculate_pv_exposure_cva", -1)
    assert not app.exception


def test_report_refresh_uses_current_publication(panel):
    """Request new reports via Airflow rather than calculating or writing results in the UI."""
    app, backend = panel
    app.run()
    navigate(app, "Reports")
    next(button for button in app.button if button.label == "Request fresh reports").click().run()
    dag, configuration, _ = backend.launch.call_args.args
    assert dag == "xva_reports" and configuration["batch_id"] == "batch"
    assert configuration["publication_path"] == "/shared/batch/published.json"


def test_date_time_display_uses_london_offset():
    """Keep valuation dates unchanged and render execution timestamps in the user's London timezone."""
    assert ui.london_time("2026-10-14T10:00:00Z") == "2026-10-14T11:00:00+01:00"
    assert ui.london_time(date(2026, 10, 14)) == date(2026, 10, 14)
