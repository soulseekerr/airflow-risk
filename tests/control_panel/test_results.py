"""Validate the panel's read-only data views against real PostgreSQL and report artifacts."""

from pathlib import Path

import pytest

from risk_pipeline.control_panel.airflow_api import PanelError
from risk_pipeline.control_panel.results import ResultsStore
from risk_pipeline.xva.batch import read_json, run_batch, write_json
from risk_pipeline.xva.generation import generate_portfolio
from risk_pipeline.xva.postgres import transaction
from risk_pipeline.xva.reports import generate_reports
from risk_pipeline.xva.reruns import prepare_counterparty_rerun, calculate_counterparty, publish_counterparty


@pytest.fixture
def store(tmp_path, pg_connection):
    """Publish and report a small synthetic portfolio in the disposable database."""
    inputs = generate_portfolio("2026-10-14", 3, 9, horizon_days=8, simulation_paths=1)
    source = write_json(tmp_path / "source.json", inputs)
    publication = run_batch(source, tmp_path / "outputs", pg_connection)
    generate_reports(publication)
    return ResultsStore(pg_connection, tmp_path / "outputs"), inputs, publication


def test_dashboard_totals_names_and_counterparty_scope(store, pg_connection):
    """Aggregate each CVA once, exclude other XVA types, and show adjusted values independently."""
    panel, inputs, publication = store
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.counterparty_xva SET value_adj=value_calc+10 WHERE xva_type='CVA'")
        cursor.execute("""INSERT INTO airflow_xva.counterparty_xva
            (valuation_date,counterparty_code,currency,xva_type,value_calc,value_adj)
            VALUES ('2026-10-14','0000001','EUR','DVA',999,999)""")
    totals = panel.dates()[0]
    assert totals["counterparty_count"] == 3 and totals["trade_count"] == 9
    assert totals["cva_calculated"] == pytest.approx(read_json(publication)["total_cva"])
    assert totals["cva_adjusted"] == pytest.approx(totals["cva_calculated"] + 30)
    rows = panel.counterparties("2026-10-14")
    assert len(rows) == 3 and sum(row["trade_count"] for row in rows) == 9
    assert {row["counterparty_name"] for row in rows} == {row["name"] for row in inputs["counterparties"]}
    trades, exposures = panel.details("2026-10-14", "0000001")
    assert {row["trade_id"] for row in trades} == {row["trade_id"] for row in inputs["trades"] if row["counterparty_code"] == "0000001"}
    assert all(row["ma_id"] == "MA_0000001" for row in exposures)
    assert {row["feed_type"] for row in panel.feeds("2026-10-14")} == {"settings", "trades", "counterparties", "csas", "master_agreements", "credit_curves"}
    assert panel.corrections("2026-10-14") == []


def test_queries_cannot_write_and_do_not_leak_database_errors(store):
    """Enforce read-only transactions even if an accidental write reaches the repository query helper."""
    panel, _, _ = store
    before = panel.dates()
    with pytest.raises(PanelError, match="database unavailable"):
        panel.query("DELETE FROM airflow_xva.trade_pv RETURNING *")
    assert panel.dates() == before


def test_current_downloads_match_report_and_reject_external_paths(store, tmp_path):
    """Download the published artifacts and prevent a modified manifest from reading arbitrary server files."""
    panel, _, publication = store
    files = panel.downloads("2026-10-14")
    assert "summary.md" in files and b"XVA batch report" in files["summary.md"]
    assert len(files) == 5
    manifest_path = Path(publication).parent / "reports/v2/report_manifest.json"
    manifest = read_json(manifest_path)
    secret = tmp_path / "summary.md"
    secret.write_text("private-server-file")
    manifest["outputs"]["summary"] = str(secret)
    write_json(manifest_path, manifest)
    with pytest.raises(PanelError, match="outside"):
        panel.downloads("2026-10-14")


def test_symlink_report_escape_is_rejected(store, tmp_path):
    """Resolve symlinks before allowing report bytes to leave the Streamlit server."""
    panel, _, publication = store
    directory = Path(publication).parent / "reports/v2"
    target = tmp_path / "private.md"
    target.write_text("private")
    summary = directory / "summary.md"
    summary.unlink()
    summary.symlink_to(target)
    with pytest.raises(PanelError, match="outside"):
        panel.downloads("2026-10-14")


def test_downloads_follow_corrected_current_revision(store, tmp_path, pg_connection):
    """Do not offer an old report after a counterparty correction publishes a new input revision."""
    panel, inputs, _ = store
    inputs["counterparties"][0]["recovery_rate"] = 0.9
    corrected = write_json(tmp_path / "corrected.json", {"valuation_date": "2026-10-14", "rows": inputs["counterparties"]})
    config = {"valuation_date": "2026-10-14", "counterparty_code": "0000001",
              "feeds": {"counterparties": corrected}, "output_root": str(tmp_path / "outputs")}
    handoff = prepare_counterparty_rerun(config, tmp_path, "ui-correction", pg_connection)
    publication = publish_counterparty(handoff, calculate_counterparty(handoff))
    with pytest.raises(PanelError, match="Reports are not available"):
        panel.downloads("2026-10-14")
    generate_reports(publication)
    assert read_json(publication)["batch_id"].encode() in panel.downloads("2026-10-14")["summary.md"]
    assert panel.corrections("2026-10-14")[0]["status"] == "published"


def test_empty_database_and_missing_artifacts_are_actionable(tmp_path, pg_connection):
    """Permit the UI to start before the first publication and explain unavailable shared files."""
    panel = ResultsStore(pg_connection, tmp_path)
    assert panel.dates() == [] and panel.feeds("2026-10-14") == []
    with pytest.raises(PanelError, match="No current publication"):
        panel.publication("2026-10-14")
