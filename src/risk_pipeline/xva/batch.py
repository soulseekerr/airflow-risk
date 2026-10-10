"""File-based batch stages and transactional, repeatable result publication."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

from risk_pipeline.xva.calculations import calculate, generate_scenarios, reconcile
from risk_pipeline.xva.inputs import MODEL_VERSION, validate
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID, SCHEMA, transaction


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    payload = json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n"
    return write_text(path, payload)


def write_text(path, payload):
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
    inputs = validate(read_json(input_path))
    snapshot = {"model_version": MODEL_VERSION, "inputs": inputs}
    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True, allow_nan=False).encode()).hexdigest()
    batch_id = f"{inputs['valuation_date']}-{digest}"
    directory = Path(output_root).resolve() / batch_id
    snapshot["batch_id"] = batch_id
    return write_json(directory / "inputs.json", snapshot)


def load_snapshot(path):
    return validate_snapshot(read_json(path))


def validate_snapshot(snapshot):
    identity = {"model_version": snapshot["model_version"], "inputs": snapshot["inputs"]}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, allow_nan=False).encode()).hexdigest()
    if snapshot["model_version"] != MODEL_VERSION or snapshot["batch_id"] != f"{snapshot['inputs']['valuation_date']}-{digest}":
        raise ValueError("Input snapshot identity or model version mismatch")
    validate(snapshot["inputs"])
    return snapshot


def simulate(snapshot_path):
    snapshot = load_snapshot(snapshot_path)
    scenarios = generate_scenarios(snapshot["inputs"])
    return write_json(Path(snapshot_path).parent / "scenarios.json", {"batch_id": snapshot["batch_id"], "scenarios": scenarios})


def value_batch(snapshot_path, scenario_path):
    snapshot = load_snapshot(snapshot_path)
    scenarios = read_json(scenario_path)
    if scenarios["batch_id"] != snapshot["batch_id"]:
        raise ValueError("Scenarios belong to another batch")
    result = calculate(snapshot["inputs"], scenarios["scenarios"])
    return write_json(Path(snapshot_path).parent / "results.json", {"batch_id": snapshot["batch_id"], "results": result})


def publish(snapshot_path, result_path, connection_id=DEFAULT_CONNECTION_ID):
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
        cursor.execute(SCHEMA)
        cursor.execute("DELETE FROM airflow_xva.batches WHERE batch_id = %s", (batch_id,))
        cursor.execute("INSERT INTO airflow_xva.batches VALUES (%s, %s, %s, %s, %s::jsonb)",
                           (batch_id, inputs["valuation_date"], MODEL_VERSION, inputs["reporting_currency"], json.dumps(snapshot, sort_keys=True)))
        cursor.executemany("INSERT INTO airflow_xva.trade_pv VALUES (%s, %s, %s, %s, %s, %s)",
                               [(batch_id, r["trade_id"], r["ma_id"], r["counterparty_code"], r["currency"], r["pv"]) for r in results["trade_pv"]])
        cursor.executemany("INSERT INTO airflow_xva.exposures VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               [(batch_id, r["ma_id"], r["date"], r["counterparty_code"], r["currency"], r["expected_positive_exposure"],
                                 r["discount_factor"], r["survival_probability"], r["cva_increment"]) for r in results["exposures"]])
        cursor.executemany("INSERT INTO airflow_xva.netting_set_cva VALUES (%s, %s, %s, %s, %s)",
                               [(batch_id, r["ma_id"], r["counterparty_code"], r["currency"], r["cva"]) for r in results["netting_set_cva"]])
        cursor.executemany("INSERT INTO airflow_xva.counterparty_cva VALUES (%s, %s, %s, %s)",
                               [(batch_id, r["counterparty_code"], r["currency"], r["cva"]) for r in results["counterparty_cva"]])
    manifest = {"batch_id": batch_id, "status": "published", "storage": "postgres", "connection_id": connection_id,
                "trade_count": len(results["trade_pv"]), "netting_set_count": len(results["netting_set_cva"]),
                "total_cva": sum(r["cva"] for r in results["counterparty_cva"])}
    return write_json(Path(snapshot_path).parent / "published.json", manifest)


def run_batch(input_path, output_root, connection_id=DEFAULT_CONNECTION_ID):
    snapshot = freeze_inputs(input_path, output_root)
    scenarios = simulate(snapshot)
    results = value_batch(snapshot, scenarios)
    return publish(snapshot, results, connection_id)


def main():
    parser = argparse.ArgumentParser(description="Run the educational exposure/CVA batch")
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--connection-id", default=DEFAULT_CONNECTION_ID)
    args = parser.parse_args()
    print(Path(run_batch(args.inputs, args.output, args.connection_id)).read_text(), end="")


if __name__ == "__main__":
    main()
