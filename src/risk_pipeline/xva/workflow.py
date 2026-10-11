"""Validate identities at cross-DAG boundaries."""

import os
from pathlib import Path

from risk_pipeline.xva.batch import freeze_inputs, load_snapshot, read_json


def prepare_inputs(configuration, repository, scheduled_date=None):
    """Resolve portable input/output locations and freeze the requested batch.

    Scheduled runs require inputs matching the interval start date. Return the
    batch ID and snapshot path used to trigger the calculation DAG.
    """
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
    # Check the date before writing artifacts or triggering another DAG.
    if scheduled_date and read_json(input_path)["valuation_date"] != scheduled_date:
        raise ValueError("Scheduled input valuation date must match the data interval start date")
    airflow_home = Path(os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))).expanduser().resolve()
    default_output = os.environ.get("XVA_OUTPUT_ROOT", str(airflow_home / "xva"))
    output_root = Path(configuration.get("output_root", default_output)).expanduser()
    if not output_root.is_absolute():
        output_root = airflow_home / output_root
    snapshot_path = freeze_inputs(input_path, output_root)
    snapshot = load_snapshot(snapshot_path)
    return {"batch_id": snapshot["batch_id"], "snapshot_path": snapshot_path}


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
