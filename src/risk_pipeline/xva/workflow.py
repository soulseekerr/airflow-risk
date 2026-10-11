"""Validate identities at cross-DAG boundaries."""

import os
import json
from pathlib import Path

from risk_pipeline.xva.batch import freeze_inputs, load_snapshot, read_json
from risk_pipeline.xva.generation import generate_submissions
from risk_pipeline.xva.ingestion import ingest_submissions, parse_payload
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID


def input_locations(configuration, repository, scheduled_date=None):
    """Resolve portable project, input-file, and artifact locations without reading or validating inputs."""
    repository = Path(os.environ.get("XVA_PROJECT_ROOT", repository)).expanduser().resolve()
    if scheduled_date:
        input_root = Path(os.environ.get("XVA_INPUT_ROOT", str(repository / "data/xva"))).expanduser()
        if not input_root.is_absolute():
            input_root = repository / input_root
        default = input_root / f"{scheduled_date}.json"
    else:
        default = repository / "examples/xva/inputs.json"
    input_path = Path(configuration.get("input_path", str(default))).expanduser()
    if not input_path.is_absolute():
        input_path = repository / input_path
    airflow_home = Path(os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))).expanduser().resolve()
    default_output = os.environ.get("XVA_OUTPUT_ROOT", str(airflow_home / "xva"))
    output_root = Path(configuration.get("output_root", default_output)).expanduser()
    if not output_root.is_absolute():
        output_root = airflow_home / output_root
    return repository, input_path, output_root


def prepare_inputs(configuration, repository, scheduled_date=None):
    """Freeze local file inputs for standalone use; the Airflow DAG uses database ingestion instead."""
    _, input_path, output_root = input_locations(configuration, repository, scheduled_date)
    if scheduled_date and read_json(input_path)["valuation_date"] != scheduled_date:
        raise ValueError("Scheduled input valuation date must match the data interval start date")
    snapshot_path = freeze_inputs(input_path, output_root)
    snapshot = load_snapshot(snapshot_path)
    return {"batch_id": snapshot["batch_id"], "snapshot_path": snapshot_path}


def ingest_inputs(configuration, repository, scheduled_date=None, request_key=None, connection_id=DEFAULT_CONNECTION_ID):
    """Persist file or generated submissions and return only database references and artifact settings."""
    repository, input_path, output_root = input_locations(configuration, repository, scheduled_date)
    if configuration.get("ingestion_id"):
        handoff = {"ingestion_id": configuration["ingestion_id"], "connection_id": connection_id}
    else:
        if configuration.get("generate_demo"):
            options = dict(configuration.get("generation", {}))
            options["valuation_date"] = scheduled_date or configuration.get("valuation_date")
            # Scheduled generation uses the static sample as a template, not the daily feed.
            template = input_path if "input_path" in configuration else repository / "examples/xva/inputs.json"
            payloads = generate_submissions(read_json(template), **options)
            raw_texts = [json.dumps(payload, allow_nan=False) for payload in payloads]
            source = f"generated:{template}:seed={options.get('seed', 42)}"
        else:
            raw_text = input_path.read_text()
            try:
                payload = parse_payload(raw_text)
            except ValueError:
                payload = None
            if isinstance(payload, dict) and isinstance(payload.get("submissions"), list) and payload["submissions"]:
                raw_texts = [json.dumps(item, allow_nan=False) for item in payload["submissions"]]
            else:
                raw_texts = [raw_text]
            source = str(input_path.resolve())
        handoff = ingest_submissions(raw_texts, source, connection_id, request_key)
    return {**handoff, "output_root": str(output_root), "scheduled_date": scheduled_date}


def require_snapshot(configuration):
    """Require a complete calculation handoff and return the path to its verified frozen snapshot."""
    if not configuration.get("batch_id") or not configuration.get("snapshot_path"):
        raise ValueError(
            "Calculation run requires batch_id and snapshot_path in dag_run.conf. "
            "For a new batch, trigger xva_inputs; it freezes the inputs and passes "
            "both values automatically. To rerun an existing batch, trigger "
            "xva_calculations with the original batch_id and snapshot_path."
        )
    path = str(Path(configuration["snapshot_path"]).resolve())
    snapshot = load_snapshot(path)
    if configuration["batch_id"] != snapshot["batch_id"]:
        raise ValueError("Requested batch does not match the frozen snapshot")
    return path


def calculation_handoff(snapshot_path, publication_path):
    """Build and validate the batch ID and publication path passed to the reporting DAG."""
    snapshot = load_snapshot(snapshot_path)
    configuration = {"batch_id": snapshot["batch_id"], "publication_path": str(Path(publication_path).resolve())}
    require_publication(configuration)
    return configuration


def require_publication(configuration):
    """Require a complete reporting handoff and verify the manifest identifies a published batch."""
    if not configuration.get("batch_id") or not configuration.get("publication_path"):
        raise ValueError("Report run requires batch_id and publication_path")
    path = str(Path(configuration["publication_path"]).resolve())
    manifest = read_json(path)
    if manifest["batch_id"] != configuration["batch_id"] or manifest["status"] != "published":
        raise ValueError("Requested batch does not match a published manifest")
    return path
