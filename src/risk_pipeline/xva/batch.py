"""File-based batch stages and transactional, repeatable result publication."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

from risk_pipeline.xva.calculations import calculate, generate_scenarios, reconcile
from risk_pipeline.xva.inputs import MODEL_VERSION, validate
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID, OUTPUT_COLUMNS, ensure_schema, transaction


def read_json(path):
    """Read an artifact from disk and decode its JSON payload."""
    return json.loads(Path(path).read_text())


def write_json(path, value):
    """Serialize finite JSON values deterministically and return the atomically written file path."""
    payload = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    return write_text(path, payload)


def write_text(path, payload):
    """Replace a UTF-8 artifact atomically and return its absolute path."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        temporary = handle.name
        handle.write(payload)
    try:
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return str(path.resolve())


def freeze_inputs(input_path, output_root):
    """Validate inputs, derive their content-based batch ID, and write the frozen snapshot."""
    snapshot = build_snapshot(read_json(input_path))
    return write_json(Path(output_root).resolve() / snapshot["batch_id"] / "inputs.json", snapshot)


def build_snapshot(inputs):
    """Validate an input payload and return its deterministic snapshot without writing files."""
    inputs = validate(inputs)
    snapshot = {"model_version": MODEL_VERSION, "inputs": inputs}
    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True, allow_nan=False).encode()).hexdigest()
    batch_id = f"{inputs['valuation_date']}-{digest}"
    snapshot["batch_id"] = batch_id
    return snapshot


def load_snapshot(path):
    """Read a frozen snapshot and verify its input contract, model version, and batch identity."""
    return validate_snapshot(read_json(path))


def validate_snapshot(snapshot):
    """Reject altered snapshots or unsupported model versions and return the validated snapshot."""
    identity = {"model_version": snapshot["model_version"], "inputs": snapshot["inputs"]}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, allow_nan=False).encode()).hexdigest()
    if snapshot["model_version"] != MODEL_VERSION or snapshot["batch_id"] != f"{snapshot['inputs']['valuation_date']}-{digest}":
        raise ValueError("Input snapshot identity or model version mismatch")
    validate(snapshot["inputs"])
    return snapshot


def simulate(snapshot_path):
    """Generate daily rate paths from a frozen snapshot and write the batch-tagged scenario artifact."""
    snapshot = load_snapshot(snapshot_path)
    scenarios = generate_scenarios(snapshot["inputs"])
    return write_json(Path(snapshot_path).parent / "scenarios.json", {"batch_id": snapshot["batch_id"], "scenarios": scenarios})


def value_batch(snapshot_path, scenario_path):
    """Check that scenarios belong to the snapshot, calculate results, and write the result artifact."""
    snapshot = load_snapshot(snapshot_path)
    scenarios = read_json(scenario_path)
    if scenarios["batch_id"] != snapshot["batch_id"]:
        raise ValueError("Scenarios belong to another batch")
    result = calculate(snapshot["inputs"], scenarios["scenarios"])
    return write_json(Path(snapshot_path).parent / "results.json", {"batch_id": snapshot["batch_id"], "results": result})


def publish(snapshot_path, result_path, connection_id=DEFAULT_CONNECTION_ID):
    """Reconcile results and transactionally replace this date's CVA outputs in PostgreSQL.

    Retain other XVA types, reset adjusted CVA to calculated CVA, and write the
    publication manifest only after the database transaction commits.
    """
    snapshot = load_snapshot(snapshot_path)
    payload = read_json(result_path)
    if payload["batch_id"] != snapshot["batch_id"]:
        raise ValueError("Results belong to another batch")
    results = payload["results"]
    batch_id, inputs = snapshot["batch_id"], snapshot["inputs"]
    reconcile(results, inputs)
    with transaction(connection_id) as cursor:
        # Serialize bootstrap and publication, including simultaneous CLI reruns.
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", ("airflow_xva_publication",))
        ensure_schema(cursor)
        valuation_date = inputs["valuation_date"]
        for table in OUTPUT_COLUMNS:
            type_filter = " AND xva_type = 'CVA'" if table == "counterparty_xva" else ""
            cursor.execute(f"DELETE FROM airflow_xva.{table} WHERE valuation_date = %s{type_filter}", (valuation_date,))
        cursor.execute("UPDATE airflow_xva.batches SET is_current = FALSE WHERE valuation_date = %s", (valuation_date,))
        cursor.execute("""INSERT INTO airflow_xva.batches
                           (batch_id, valuation_date, model_version, currency, input_snapshot, is_current)
                           VALUES (%s, %s, %s, %s, %s::jsonb, TRUE)
                           ON CONFLICT (batch_id) DO UPDATE SET is_current = TRUE""",
                           (batch_id, inputs["valuation_date"], MODEL_VERSION, inputs["reporting_currency"], json.dumps(snapshot, sort_keys=True)))
        cursor.executemany("""INSERT INTO airflow_xva.trade_pv
                              (valuation_date, trade_id, ma_id, counterparty_code, pv)
                              VALUES (%s, %s, %s, %s, %s)""",
                               [(valuation_date, r["trade_id"], r["ma_id"], r["counterparty_code"], r["pv"]) for r in results["trade_pv"]])
        cursor.executemany("INSERT INTO airflow_xva.exposures VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               [(valuation_date, r["ma_id"], r["date"], r["counterparty_code"], r["currency"], r["expected_positive_exposure"],
                                 r["discount_factor"], r["survival_probability"], r["cva_increment"]) for r in results["exposures"]])
        cursor.executemany("INSERT INTO airflow_xva.netting_set_cva VALUES (%s, %s, %s, %s, %s)",
                               [(valuation_date, r["ma_id"], r["counterparty_code"], r["currency"], r["cva"]) for r in results["netting_set_cva"]])
        cursor.executemany("""INSERT INTO airflow_xva.counterparty_xva
                              (valuation_date, counterparty_code, currency, xva_type, value_calc, value_adj)
                              VALUES (%s, %s, %s, 'CVA', %s, %s)""",
                               [(valuation_date, r["counterparty_code"], r["currency"], r["cva"], r["cva"]) for r in results["counterparty_cva"]])
    manifest = {"batch_id": batch_id, "valuation_date": valuation_date, "status": "published", "storage": "postgres", "connection_id": connection_id,
                "trade_count": len(results["trade_pv"]), "netting_set_count": len(results["netting_set_cva"]),
                "total_cva": sum(r["cva"] for r in results["counterparty_cva"])}
    return write_json(Path(snapshot_path).parent / "published.json", manifest)


def run_batch(input_path, output_root, connection_id=DEFAULT_CONNECTION_ID):
    """Ingest raw inputs, validate and freeze the selected portfolio, then calculate and publish results."""
    from risk_pipeline.xva.workflow import ingest_inputs
    from risk_pipeline.xva.ingestion import validate_ingestion

    ingestion = ingest_inputs({"input_path": str(Path(input_path).expanduser().resolve()),
                               "output_root": str(Path(output_root).expanduser().resolve())},
                              Path.cwd(), connection_id=connection_id)
    snapshot = validate_ingestion(ingestion)["snapshot_path"]
    scenarios = simulate(snapshot)
    results = value_batch(snapshot, scenarios)
    return publish(snapshot, results, connection_id)


def main():
    """Run the complete batch from CLI arguments and print its committed publication manifest."""
    parser = argparse.ArgumentParser(description="Run the educational exposure/CVA batch")
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--connection-id", default=DEFAULT_CONNECTION_ID)
    args = parser.parse_args()
    print(Path(run_batch(args.inputs, args.output, args.connection_id)).read_text(), end="")


if __name__ == "__main__":
    main()
