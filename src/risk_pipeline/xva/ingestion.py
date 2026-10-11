"""Persist raw submissions before business validation and retain rejected payloads."""

import json
from pathlib import Path
import uuid

from risk_pipeline.xva.batch import build_snapshot, validate_snapshot, write_json
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID, transaction


INPUT_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS airflow_xva;
CREATE TABLE IF NOT EXISTS airflow_xva.input_submissions (
 submission_id TEXT PRIMARY KEY,
 ingestion_id TEXT NOT NULL,
 request_key TEXT,
 submission_index INTEGER NOT NULL CHECK (submission_index >= 0),
 source TEXT NOT NULL,
 received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 raw_text TEXT NOT NULL,
 raw_payload JSONB,
 status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'accepted', 'rejected')),
 validation_errors JSONB NOT NULL DEFAULT '[]'::jsonb,
 validated_at TIMESTAMPTZ,
 valuation_date DATE,
 batch_id TEXT,
 validated_snapshot JSONB,
 snapshot_path TEXT,
 selected_for_calculation BOOLEAN NOT NULL DEFAULT FALSE,
 UNIQUE (ingestion_id, submission_index),
 CHECK (NOT selected_for_calculation OR status = 'accepted'),
 CHECK ((status = 'pending' AND validated_at IS NULL AND valuation_date IS NULL AND batch_id IS NULL AND validated_snapshot IS NULL)
     OR (status = 'accepted' AND validated_at IS NOT NULL AND valuation_date IS NOT NULL AND batch_id IS NOT NULL AND validated_snapshot IS NOT NULL)
     OR (status = 'rejected' AND validated_at IS NOT NULL AND valuation_date IS NULL AND batch_id IS NULL AND validated_snapshot IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS selected_input_per_ingestion
 ON airflow_xva.input_submissions (ingestion_id) WHERE selected_for_calculation;
"""


def reject_constant(value):
    """Reject nonstandard JSON constants such as NaN while retaining the original raw text."""
    raise ValueError(f"Nonstandard JSON constant: {value}")


def parse_payload(raw_text):
    """Decode strict JSON without applying the business input contract."""
    return json.loads(raw_text, parse_constant=reject_constant)


def ingest_submissions(raw_texts, source, connection_id=DEFAULT_CONNECTION_ID, request_key=None):
    """Commit raw submissions before validation, reusing an unchanged ingestion on task retry.

    A request key identifies an Airflow run. Reusing it with changed contents is
    rejected so retries cannot overwrite the submission history.
    """
    if not raw_texts:
        raise ValueError("An ingestion requires at least one submission")
    ingestion_id = str(uuid.uuid5(uuid.NAMESPACE_URL, request_key)) if request_key else str(uuid.uuid4())
    with transaction(connection_id) as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("airflow_xva_ingestion_schema",))
        cursor.execute(INPUT_SCHEMA)
        cursor.execute("SELECT source, raw_text FROM airflow_xva.input_submissions WHERE ingestion_id=%s ORDER BY submission_index", (ingestion_id,))
        previous = cursor.fetchall()
        if previous:
            if previous != [(source, text) for text in raw_texts]:
                raise ValueError("Ingestion retry changed its source or contents; start a new input run")
        else:
            for index, raw_text in enumerate(raw_texts):
                try:
                    parsed = json.dumps(parse_payload(raw_text), allow_nan=False)
                except ValueError:
                    parsed = None
                cursor.execute("""INSERT INTO airflow_xva.input_submissions
                    (submission_id, ingestion_id, request_key, submission_index, source, raw_text, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)""",
                    (str(uuid.uuid4()), ingestion_id, request_key, index, source, raw_text, parsed))
    return {"ingestion_id": ingestion_id, "connection_id": connection_id}


def validate_ingestion(handoff):
    """Validate database submissions, persist their outcomes, and freeze the last accepted portfolio.

    Rejection reasons and accepted snapshots commit before any exception is
    raised for an entirely rejected ingestion. Successful retries recreate the
    selected artifact from its database snapshot without revalidating history.
    """
    ingestion_id, connection_id = handoff["ingestion_id"], handoff["connection_id"]
    accepted, rejected = [], 0
    with transaction(connection_id) as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"xva_validate:{ingestion_id}",))
        cursor.execute("""SELECT submission_id, raw_text, status, validated_snapshot
            FROM airflow_xva.input_submissions WHERE ingestion_id=%s ORDER BY submission_index FOR UPDATE""", (ingestion_id,))
        rows = cursor.fetchall()
        if not rows:
            raise ValueError("Input ingestion is missing from the database")
        for submission_id, raw_text, status, saved_snapshot in rows:
            if status == "rejected":
                rejected += 1
                continue
            if status == "accepted":
                snapshot = validate_snapshot(saved_snapshot)
                if handoff.get("scheduled_date") and snapshot["inputs"]["valuation_date"] != handoff["scheduled_date"]:
                    raise ValueError("Accepted ingestion valuation date does not match the requested scheduled date")
                accepted.append((submission_id, snapshot))
                continue
            try:
                payload = parse_payload(raw_text)
                if not isinstance(payload, dict):
                    raise ValueError("Submission must be a JSON object")
                if handoff.get("scheduled_date") and payload.get("valuation_date") != handoff["scheduled_date"]:
                    raise ValueError("Scheduled input valuation date must match the data interval start date")
                snapshot = build_snapshot(payload)
            except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
                reason = f"{type(error).__name__}: {error}"
                cursor.execute("""UPDATE airflow_xva.input_submissions
                    SET status='rejected', validation_errors=%s::jsonb, validated_at=clock_timestamp()
                    WHERE submission_id=%s""", (json.dumps([reason]), submission_id))
                rejected += 1
                continue
            cursor.execute("""UPDATE airflow_xva.input_submissions
                SET status='accepted', valuation_date=%s, batch_id=%s, validated_snapshot=%s::jsonb,
                    validated_at=clock_timestamp() WHERE submission_id=%s""",
                (snapshot["inputs"]["valuation_date"], snapshot["batch_id"], json.dumps(snapshot, allow_nan=False), submission_id))
            accepted.append((submission_id, snapshot))
        if accepted:
            submission_id, snapshot = accepted[-1]
            cursor.execute("UPDATE airflow_xva.input_submissions SET selected_for_calculation=FALSE WHERE ingestion_id=%s AND selected_for_calculation", (ingestion_id,))
            cursor.execute("UPDATE airflow_xva.input_submissions SET selected_for_calculation=TRUE WHERE submission_id=%s", (submission_id,))
    if not accepted:
        raise ValueError(f"All {rejected} input submissions were rejected; inspect airflow_xva.input_submissions.validation_errors")
    snapshot_path = write_json(Path(handoff["output_root"]) / snapshot["batch_id"] / "inputs.json", snapshot)
    with transaction(connection_id) as cursor:
        cursor.execute("UPDATE airflow_xva.input_submissions SET snapshot_path=%s WHERE submission_id=%s", (snapshot_path, submission_id))
    return {"batch_id": snapshot["batch_id"], "snapshot_path": snapshot_path,
            "ingestion_id": ingestion_id, "submission_id": submission_id,
            "accepted_count": len(accepted), "rejected_count": rejected}
