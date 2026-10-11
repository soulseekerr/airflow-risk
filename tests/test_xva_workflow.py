import csv
import json
from pathlib import Path
import sys

import pytest

from risk_pipeline.xva.batch import read_json, run_batch, write_json
from risk_pipeline.xva.reports import generate_reports, load_published_results, main
from risk_pipeline.xva.workflow import calculation_handoff, prepare_inputs, require_publication, require_snapshot
from risk_pipeline.xva.postgres import transaction
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import USE_PSYCOPG3

if USE_PSYCOPG3:
    from psycopg import Error as DatabaseError
else:
    from psycopg2 import Error as DatabaseError


REPOSITORY = Path(__file__).resolve().parents[1]


@pytest.fixture
def source(tmp_path):
    inputs = read_json(REPOSITORY / "examples/xva/inputs.json")
    inputs["simulation"]["paths"] = 2
    return write_json(tmp_path / "input.json", inputs)


@pytest.fixture
def publication(source, tmp_path, pg_connection):
    return run_batch(source, tmp_path / "output", pg_connection)


def test_input_handoff_and_manual_defaults(tmp_path):
    handoff = prepare_inputs({"output_root": str(tmp_path)}, REPOSITORY)
    assert require_snapshot(handoff) == handoff["snapshot_path"]
    assert handoff["batch_id"].startswith("2026-10-10-")
    with pytest.raises(ValueError, match="does not match"):
        require_snapshot({**handoff, "batch_id": "other"})


@pytest.mark.parametrize("selection", ["airflow_home", "user_home", "environment", "configuration", "relative_output"])
def test_portable_output_paths(tmp_path, monkeypatch, selection):
    monkeypatch.delenv("XVA_OUTPUT_ROOT", raising=False)
    monkeypatch.delenv("XVA_PROJECT_ROOT", raising=False)
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    configuration = {}
    expected = tmp_path / "airflow/xva"
    if selection == "user_home":
        monkeypatch.delenv("AIRFLOW_HOME", raising=False)
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
    elif selection == "environment":
        expected = tmp_path / "shared-artifacts"
        monkeypatch.setenv("XVA_OUTPUT_ROOT", str(expected))
    elif selection == "configuration":
        monkeypatch.setenv("XVA_OUTPUT_ROOT", str(tmp_path / "unused"))
        expected = tmp_path / "run-artifacts"
        configuration["output_root"] = str(expected)
    elif selection == "relative_output":
        expected = tmp_path / "airflow/relative-artifacts"
        configuration["output_root"] = "relative-artifacts"
    handoff = prepare_inputs(configuration, REPOSITORY)
    assert Path(handoff["snapshot_path"]).parent.parent == expected
    assert Path(handoff["snapshot_path"]).is_file()
    assert require_snapshot(handoff) == handoff["snapshot_path"]


def test_project_root_override_for_copied_dags(source, tmp_path, monkeypatch):
    project = tmp_path / "checkout"
    write_json(project / "examples/xva/inputs.json", read_json(source))
    monkeypatch.setenv("XVA_PROJECT_ROOT", str(project))
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    monkeypatch.delenv("XVA_OUTPUT_ROOT", raising=False)
    monkeypatch.chdir(tmp_path)
    handoff = prepare_inputs({"input_path": "examples/xva/inputs.json"}, tmp_path / "copied-dag-bundle")
    assert Path(handoff["snapshot_path"]).is_file()


def test_relative_scheduled_input_root(source, tmp_path, monkeypatch):
    write_json(tmp_path / "feed/2026-10-10.json", read_json(source))
    monkeypatch.setenv("XVA_INPUT_ROOT", "feed")
    monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path / "airflow"))
    monkeypatch.delenv("XVA_PROJECT_ROOT", raising=False)
    monkeypatch.delenv("XVA_OUTPUT_ROOT", raising=False)
    handoff = prepare_inputs({}, tmp_path, "2026-10-10")
    assert Path(handoff["snapshot_path"]).is_file()


def test_scheduled_input_feed_and_date_validation(source, tmp_path, monkeypatch):
    root = tmp_path / "feed"
    inputs = read_json(source)
    write_json(root / "2026-10-10.json", inputs)
    monkeypatch.setenv("XVA_INPUT_ROOT", str(root))
    result = prepare_inputs({"output_root": str(tmp_path / "scheduled")}, REPOSITORY, "2026-10-10")
    assert result["batch_id"].startswith("2026-10-10-")
    with pytest.raises(ValueError, match="valuation date"):
        prepare_inputs({"input_path": source, "output_root": str(tmp_path / "bad")}, REPOSITORY, "2026-10-11")
    assert not (tmp_path / "bad").exists()
    with pytest.raises(FileNotFoundError):
        prepare_inputs({}, REPOSITORY, "2026-10-12")


@pytest.mark.parametrize("configuration", [{}, {"batch_id": "one"}, {"snapshot_path": "file"}])
def test_calculation_requires_complete_handoff(configuration):
    with pytest.raises(ValueError, match="requires.*dag_run.conf.*trigger xva_inputs"):
        require_snapshot(configuration)


@pytest.mark.parametrize("configuration", [{}, {"batch_id": "one"}, {"publication_path": "file"}])
def test_reports_require_complete_handoff(configuration):
    with pytest.raises(ValueError, match="requires"):
        require_publication(configuration)


def test_publication_handoff_and_identity_checks(publication):
    snapshot = str(Path(publication).parent / "inputs.json")
    handoff = calculation_handoff(snapshot, publication)
    assert require_publication(handoff) == publication
    with pytest.raises(ValueError, match="published manifest"):
        require_publication({**handoff, "batch_id": "other"})
    manifest = read_json(publication)
    manifest["status"] = "failed"
    write_json(publication, manifest)
    with pytest.raises(ValueError, match="published manifest"):
        require_publication(handoff)


def test_reports_reproduce_published_results_without_calculation(publication, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Reporting must not calculate or simulate")
    monkeypatch.setattr("risk_pipeline.xva.batch.simulate", forbidden)
    monkeypatch.setattr("risk_pipeline.xva.batch.value_batch", forbidden)
    manifest = read_json(publication)
    database_before = load_published_results(publication)
    results_before = (Path(publication).parent / "results.json").read_bytes()
    report_path = generate_reports(publication)
    report = read_json(report_path)
    assert report["status"] == "reported" and report["batch_id"] == manifest["batch_id"]
    assert report["total_cva"] == pytest.approx(manifest["total_cva"])
    with open(report["outputs"]["counterparty_summary"], newline="") as handle:
        summaries = list(csv.DictReader(handle))
    assert len(summaries) == 2
    assert summaries[0]["counterparty_code"] == "0000001"
    assert summaries[0]["counterparty_name"] == "Demo Bank"
    for row in summaries:
        assert float(row["cva_adjusted_pv"]) == pytest.approx(float(row["pv_t0"]) - float(row["cva"]))
    with open(report["outputs"]["exposures"], newline="") as handle:
        exposures = list(csv.DictReader(handle))
    assert len(exposures) == 52
    assert {row["batch_id"] for row in exposures} == {manifest["batch_id"]}
    previous = {p: Path(p).read_bytes() for p in report["outputs"].values()}
    assert generate_reports(publication) == report_path
    assert all(Path(p).read_bytes() == data for p, data in previous.items())
    assert load_published_results(publication) == database_before
    assert (Path(publication).parent / "results.json").read_bytes() == results_before


def test_reports_reject_superseded_batch(publication, source, tmp_path, pg_connection):
    first_snapshot, first = load_published_results(publication)
    inputs = read_json(source)
    inputs["simulation"]["seed"] += 1
    write_json(source, inputs)
    other = run_batch(source, tmp_path / "output", pg_connection)
    with pytest.raises(ValueError, match="no longer current"):
        load_published_results(publication)
    second_snapshot, second = load_published_results(other)
    assert first_snapshot["batch_id"] != second_snapshot["batch_id"]
    assert len(first["trade_pv"]) == len(second["trade_pv"]) == 3
    assert first["counterparty_cva"] != second["counterparty_cva"]


@pytest.mark.parametrize("change", ["missing_batch", "missing_connection", "wrong_snapshot", "wrong_total", "bad_exposure", "legacy_sqlite"])
def test_unpublished_or_inconsistent_results_not_reported(publication, tmp_path, change, pg_connection):
    manifest = read_json(publication)
    if change == "missing_batch":
        manifest["batch_id"] = "absent"
    elif change == "missing_connection":
        manifest["connection_id"] = "xva_test_missing"
    elif change == "legacy_sqlite":
        manifest.pop("storage")
        manifest.pop("connection_id")
    elif change == "wrong_total":
        manifest["total_cva"] += 10
    else:
        with transaction(pg_connection) as cursor:
            if change == "bad_exposure":
                # Corrupt a valid nonnegative value so reconciliation still detects it.
                cursor.execute("UPDATE airflow_xva.exposures SET cva_increment = cva_increment + 1 WHERE valuation_date = %s", (manifest["valuation_date"],))
            else:
                snapshot = read_json(Path(publication).parent / "inputs.json")
                # A valid snapshot for another seed/batch, stored under the old identity.
                inputs = snapshot["inputs"]
                inputs["simulation"]["seed"] += 1
                other_source = write_json(tmp_path / "other.json", inputs)
                handoff = prepare_inputs({"input_path": other_source, "output_root": str(tmp_path / "other")}, REPOSITORY)
                other_snapshot = read_json(handoff["snapshot_path"])
                cursor.execute("UPDATE airflow_xva.batches SET input_snapshot = %s::jsonb WHERE batch_id = %s", (json.dumps(other_snapshot), manifest["batch_id"]))
    write_json(publication, manifest)
    with pytest.raises((ValueError, DatabaseError, AirflowNotFoundException)):
        generate_reports(publication)
    assert not (Path(publication).parent / "reports").exists()


def test_reports_cli(publication, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["reports", "--publication", publication])
    main()
    assert json.loads(capsys.readouterr().out)["status"] == "reported"
