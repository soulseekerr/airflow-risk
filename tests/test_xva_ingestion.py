import copy
import json
from pathlib import Path

import pytest

from risk_pipeline.xva.batch import read_json, write_json
from risk_pipeline.xva.batch import run_batch, simulate, value_batch, publish
from risk_pipeline.xva.ingestion import ingest_submissions, validate_ingestion
from risk_pipeline.xva.postgres import transaction
from risk_pipeline.xva.reports import generate_reports
from risk_pipeline.xva.workflow import ingest_inputs, require_snapshot


REPOSITORY = Path(__file__).resolve().parents[1]


@pytest.fixture
def template():
    """Provide small sample inputs for database ingestion and validation tests."""
    payload = read_json(REPOSITORY / "examples/xva/inputs.json")
    payload["simulation"]["paths"] = 2
    return payload


def rows(connection_id, ingestion_id):
    """Read persisted submission state in source order from the disposable database."""
    with transaction(connection_id, readonly=True) as cursor:
        cursor.execute("""SELECT submission_id, status, raw_payload, validation_errors, batch_id,
            validated_snapshot, selected_for_calculation, validated_at, snapshot_path
            FROM airflow_xva.input_submissions WHERE ingestion_id=%s ORDER BY submission_index""", (ingestion_id,))
        return cursor.fetchall()


def test_raw_inputs_commit_before_validation_and_calculation(template, tmp_path, pg_connection):
    """Verify raw database records precede validation and accepted snapshots precede any result publication."""
    source = write_json(tmp_path / "source.json", template)
    handoff = ingest_inputs({"input_path": source, "output_root": str(tmp_path / "output")}, REPOSITORY,
                            request_key="run-before-validation", connection_id=pg_connection)
    before = rows(pg_connection, handoff["ingestion_id"])
    assert before[0][1:6] == ("pending", template, [], None, None)
    assert not (tmp_path / "output").exists()
    # The database is authoritative even if the source file changes after ingestion.
    write_json(source, {"bad": "changed file"})
    result = validate_ingestion(handoff)
    snapshot = read_json(require_snapshot(result))
    assert snapshot["inputs"] == template
    after = rows(pg_connection, handoff["ingestion_id"])[0]
    assert after[1] == "accepted" and after[4] == result["batch_id"]
    assert after[5] == snapshot and after[6] is True and after[8] == result["snapshot_path"]
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT to_regclass('airflow_xva.batches')")
        assert cursor.fetchone()[0] is None
        cursor.execute("SELECT valuation_date, request_key FROM airflow_xva.input_submissions")
        date, request = cursor.fetchone()
        assert date.isoformat() == template["valuation_date"] and request == "run-before-validation"


def test_generated_ingestion_preserves_rejections_and_selects_last_accepted(tmp_path, pg_connection):
    """Retain every generated submission while sending only the final accepted portfolio to calculations."""
    handoff = ingest_inputs({"generate_demo": True, "generation": {"count": 8, "seed": 42,
                            "noise": 0.1, "rejection_rate": 0.25}, "output_root": str(tmp_path)},
                            REPOSITORY, request_key="generated-run", connection_id=pg_connection)
    assert len(rows(pg_connection, handoff["ingestion_id"])) == 8
    assert all(row[1] == "pending" for row in rows(pg_connection, handoff["ingestion_id"]))
    result = validate_ingestion(handoff)
    assert (result["accepted_count"], result["rejected_count"]) == (6, 2)
    persisted = rows(pg_connection, handoff["ingestion_id"])
    accepted = [row for row in persisted if row[1] == "accepted"]
    rejected = [row for row in persisted if row[1] == "rejected"]
    assert all(row[3] and row[4] is None and row[5] is None for row in rejected)
    assert accepted[-1][0] == result["submission_id"]
    assert sum(row[6] for row in persisted) == 1
    assert read_json(result["snapshot_path"]) == accepted[-1][5]


def test_all_rejected_ingestion_commits_errors_before_failing(template, tmp_path, pg_connection):
    """Persist malformed JSON and business-rule failures even though validation prevents the downstream trigger."""
    invalid = copy.deepcopy(template)
    invalid["trades"][0]["notional"] = -1
    texts = ['{"broken":', json.dumps(invalid), '{"amount": NaN}', '[]']
    handoff = {**ingest_submissions(texts, "test-invalid", pg_connection), "output_root": str(tmp_path)}
    with pytest.raises(ValueError, match="All 4.*rejected"):
        validate_ingestion(handoff)
    persisted = rows(pg_connection, handoff["ingestion_id"])
    assert all(row[1] == "rejected" and row[3] and row[7] is not None for row in persisted)
    assert persisted[0][2] is None and persisted[2][2] is None
    assert not any(row[6] for row in persisted)
    with pytest.raises(ValueError, match="All 4.*rejected"):
        validate_ingestion(handoff)
    assert rows(pg_connection, handoff["ingestion_id"]) == persisted


def test_retries_reuse_immutable_inputs_and_restore_missing_artifact(template, tmp_path, pg_connection):
    """Retry ingestion without duplicate audit records and recreate artifacts from the stored accepted snapshot."""
    source = write_json(tmp_path / "source.json", template)
    configuration = {"input_path": source, "output_root": str(tmp_path / "output")}
    handoff = ingest_inputs(configuration, REPOSITORY, request_key="retry-run", connection_id=pg_connection)
    assert ingest_inputs(configuration, REPOSITORY, request_key="retry-run", connection_id=pg_connection) == handoff
    result = validate_ingestion(handoff)
    before = rows(pg_connection, handoff["ingestion_id"])
    Path(result["snapshot_path"]).unlink()
    assert validate_ingestion(handoff) == result
    assert rows(pg_connection, handoff["ingestion_id"]) == before
    template["market"]["flat_rate"] += 0.01
    write_json(source, template)
    with pytest.raises(ValueError, match="start a new input run"):
        ingest_inputs(configuration, REPOSITORY, request_key="retry-run", connection_id=pg_connection)
    assert rows(pg_connection, handoff["ingestion_id"]) == before


def test_revalidate_by_database_reference_without_source_file(template, tmp_path, pg_connection):
    """Support a new input DAG run referencing a stored ingestion after the source file is removed."""
    source = write_json(tmp_path / "source.json", template)
    handoff = ingest_inputs({"input_path": source, "output_root": str(tmp_path)}, REPOSITORY, connection_id=pg_connection)
    Path(source).unlink()
    resumed = ingest_inputs({"ingestion_id": handoff["ingestion_id"], "output_root": str(tmp_path)},
                            tmp_path / "missing-project", connection_id=pg_connection)
    assert resumed == handoff
    assert require_snapshot(validate_ingestion(resumed))


def test_scheduled_date_mismatch_is_recorded(template, tmp_path, pg_connection):
    """Store a rejected daily submission instead of losing the audit trail on date mismatch."""
    source = write_json(tmp_path / "source.json", template)
    handoff = ingest_inputs({"input_path": source, "output_root": str(tmp_path / "output")}, REPOSITORY,
                            scheduled_date="2026-10-11", connection_id=pg_connection)
    with pytest.raises(ValueError, match="rejected"):
        validate_ingestion(handoff)
    assert "data interval" in rows(pg_connection, handoff["ingestion_id"])[0][3][0]
    assert not (tmp_path / "output").exists()


def test_scheduled_generator_shifts_sample_without_daily_feed(tmp_path, pg_connection):
    """Use the sample template for scheduled generation and shift all related dates coherently."""
    handoff = ingest_inputs({"generate_demo": True, "generation": {"count": 2, "rejection_rate": 0},
                            "output_root": str(tmp_path)}, REPOSITORY,
                            scheduled_date="2026-10-12", connection_id=pg_connection)
    result = validate_ingestion(handoff)
    assert read_json(result["snapshot_path"])["inputs"]["valuation_date"] == "2026-10-12"
    with pytest.raises(ValueError, match="scheduled date"):
        validate_ingestion({**handoff, "scheduled_date": "2026-10-13"})


def test_submission_envelope_ingestion(template, tmp_path, pg_connection):
    """Accept generated-file envelopes and preserve each portfolio as a separate submission."""
    invalid = copy.deepcopy(template)
    invalid["counterparties"][0]["recovery_rate"] = 2
    source = write_json(tmp_path / "submissions.json", {"submissions": [template, invalid]})
    handoff = ingest_inputs({"input_path": source, "output_root": str(tmp_path / "output")},
                            REPOSITORY, connection_id=pg_connection)
    result = validate_ingestion(handoff)
    assert (result["accepted_count"], result["rejected_count"]) == (1, 1)


def test_missing_ingestion_and_empty_source_rejected(tmp_path, pg_connection):
    """Reject empty requests and database references that do not identify a stored ingestion."""
    with pytest.raises(ValueError, match="at least one"):
        ingest_submissions([], "empty", pg_connection)
    ingest_submissions(['{}'], "bootstrap", pg_connection)
    with pytest.raises(ValueError, match="missing from the database"):
        validate_ingestion({"ingestion_id": "missing", "connection_id": pg_connection, "output_root": str(tmp_path)})


def test_malformed_file_is_ingested_before_rejection(tmp_path, pg_connection):
    """Retain the exact malformed source text and a rejection reason when file parsing fails."""
    source = tmp_path / "bad.json"
    source.write_text('{"unfinished":')
    handoff = ingest_inputs({"input_path": str(source), "output_root": str(tmp_path)},
                            REPOSITORY, connection_id=pg_connection)
    assert rows(pg_connection, handoff["ingestion_id"])[0][1] == "pending"
    with pytest.raises(ValueError, match="rejected"):
        validate_ingestion(handoff)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT raw_text, raw_payload, validation_errors FROM airflow_xva.input_submissions")
        raw_text, raw_payload, errors = cursor.fetchone()
        assert raw_text == source.read_text() and raw_payload is None and errors


def test_standalone_batch_preserves_rejected_input_history(tmp_path, pg_connection):
    """Check the full batch CLI path also records raw inputs before business validation."""
    source = write_json(tmp_path / "invalid.json", {"valuation_date": "2026-10-10"})
    with pytest.raises(ValueError, match="rejected"):
        run_batch(source, tmp_path / "outputs", pg_connection)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT status, raw_payload, validation_errors FROM airflow_xva.input_submissions")
        status, payload, errors = cursor.fetchone()
        assert status == "rejected" and payload == {"valuation_date": "2026-10-10"} and errors
        cursor.execute("SELECT to_regclass('airflow_xva.batches')")
        assert cursor.fetchone()[0] is None


def test_generated_accepted_snapshot_runs_through_publication_and_reports(template, tmp_path, pg_connection):
    """Exercise the full generated intake-to-report workflow against real PostgreSQL."""
    source = write_json(tmp_path / "template.json", template)
    ingestion = ingest_inputs({"generate_demo": True, "input_path": source,
                               "generation": {"count": 4, "rejection_rate": 0.5},
                               "output_root": str(tmp_path / "artifacts")}, REPOSITORY,
                              request_key="generated-full-chain", connection_id=pg_connection)
    frozen = validate_ingestion(ingestion)
    assert (frozen["accepted_count"], frozen["rejected_count"]) == (2, 2)
    scenario_path = simulate(frozen["snapshot_path"])
    result_path = value_batch(frozen["snapshot_path"], scenario_path)
    publication = publish(frozen["snapshot_path"], result_path, pg_connection)
    report = read_json(generate_reports(publication))
    assert report["status"] == "reported" and report["batch_id"] == frozen["batch_id"]
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT batch_id FROM airflow_xva.batches WHERE is_current")
        assert cursor.fetchall() == [(frozen["batch_id"],)]
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.input_submissions WHERE status='rejected'")
        assert cursor.fetchone()[0] == 2
