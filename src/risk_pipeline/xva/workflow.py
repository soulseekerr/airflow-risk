"""Validate identities at cross-DAG boundaries."""

import os
from pathlib import Path

from risk_pipeline.xva.batch import freeze_inputs, load_snapshot, read_json


def prepare_inputs(configuration, repository, scheduled_date=None):
    repository = Path(repository)
    if scheduled_date:
        input_root = Path(os.environ.get("XVA_INPUT_ROOT", str(repository / "data/xva")))
        default = input_root / f"{scheduled_date}.json"
    else:
        default = repository / "examples/xva/inputs.json"
    input_path = configuration.get("input_path", str(default))
    # Check the date before writing artifacts or triggering another DAG.
    if scheduled_date and read_json(input_path)["valuation_date"] != scheduled_date:
        raise ValueError("Scheduled input valuation date must match the data interval start date")
    snapshot_path = freeze_inputs(input_path, configuration.get("output_root", "/workspace/.xva-risk"))
    snapshot = load_snapshot(snapshot_path)
    return {"batch_id": snapshot["batch_id"], "snapshot_path": snapshot_path}


def require_snapshot(configuration):
    if not configuration.get("batch_id") or not configuration.get("snapshot_path"):
        raise ValueError("Calculation run requires batch_id and snapshot_path")
    path = str(Path(configuration["snapshot_path"]).resolve())
    snapshot = load_snapshot(path)
    if configuration["batch_id"] != snapshot["batch_id"]:
        raise ValueError("Requested batch does not match the frozen snapshot")
    return path


def calculation_handoff(snapshot_path, publication_path):
    snapshot = load_snapshot(snapshot_path)
    configuration = {"batch_id": snapshot["batch_id"], "publication_path": str(Path(publication_path).resolve())}
    require_publication(configuration)
    return configuration


def require_publication(configuration):
    if not configuration.get("batch_id") or not configuration.get("publication_path"):
        raise ValueError("Report run requires batch_id and publication_path")
    path = str(Path(configuration["publication_path"]).resolve())
    manifest = read_json(path)
    if manifest["batch_id"] != configuration["batch_id"] or manifest["status"] != "published":
        raise ValueError("Requested batch does not match a published manifest")
    return path
