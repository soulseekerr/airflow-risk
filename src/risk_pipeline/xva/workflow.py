"""Validate identities at cross-DAG boundaries."""

import os
import json
import uuid
from pathlib import Path

from risk_pipeline.xva.batch import freeze_inputs, load_snapshot, read_json, write_json
from risk_pipeline.xva.generation import current_valuation_date, generate_submissions, write_feed_sets
from risk_pipeline.xva.ingestion import ingest_submissions, parse_payload
from risk_pipeline.xva.feeds import FIELDS, create_input_set, ingest_feed
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID
from risk_pipeline.xva.inputs import integer


def input_locations(configuration, repository, scheduled_date=None):
    """Resolve portable project, input-file, and artifact locations without reading or validating inputs."""
    repository = Path(os.environ.get("XVA_PROJECT_ROOT", repository)).expanduser().resolve()
    if scheduled_date:
        input_root = Path(os.environ.get("XVA_INPUT_ROOT", str(repository / "data/xva"))).expanduser()
        if not input_root.is_absolute():
            input_root = repository / input_root
        default = input_root / f"{scheduled_date}.json"
    else:
        default = repository / "data/xva/inputs.json"
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
    if "input_path" not in configuration and not scheduled_date:
        raise ValueError("Standalone freezing requires input_path; generate random inputs first")
    _, input_path, output_root = input_locations(configuration, repository, scheduled_date)
    if scheduled_date and read_json(input_path)["valuation_date"] != scheduled_date:
        raise ValueError("Scheduled input valuation date must match the data interval start date")
    snapshot_path = freeze_inputs(input_path, output_root)
    snapshot = load_snapshot(snapshot_path)
    return {"batch_id": snapshot["batch_id"], "snapshot_path": snapshot_path}


def ingest_inputs(configuration, repository, scheduled_date=None, request_key=None, connection_id=DEFAULT_CONNECTION_ID):
    """Stage explicit feeds or portfolios, defaulting to generated synthetic source files."""
    repository, input_path, output_root = input_locations(configuration, repository, scheduled_date)
    if configuration.get("ingestion_id"):
        handoff = {"ingestion_id": configuration["ingestion_id"], "connection_id": connection_id}
    elif configuration.get("feeds") is not None:
        specs = configuration["feeds"]
        valuation_date = scheduled_date or configuration.get("valuation_date")
        if valuation_date is None:
            raise ValueError("An explicit feed set requires valuation_date")
        feed_ids = {}
        for kind, spec in specs.items():
            if kind not in FIELDS:
                raise ValueError(f"Unknown feed type: {kind}")
            if isinstance(spec, dict) and spec.get("feed_id"):
                feed_ids[kind] = spec["feed_id"]
                continue
            path = Path(spec["path"] if isinstance(spec, dict) else spec).expanduser()
            if not path.is_absolute():
                path = repository / path
            source = spec.get("source", str(path.resolve())) if isinstance(spec, dict) else str(path.resolve())
            feed_ids[kind] = ingest_feed(kind, path.read_text(), source, valuation_date, connection_id,
                                         request_key=f"{request_key}:{kind}" if request_key else None)
        handoff = create_input_set(feed_ids, valuation_date, connection_id, request_key=request_key)
    else:
        if configuration.get("generate_demo"):
            options = {"count": 1, "rejection_rate": 0, **configuration.get("generation", {})}
            options["valuation_date"] = scheduled_date or configuration.get("valuation_date") or current_valuation_date()
            payloads = generate_submissions(**options)
            raw_texts = [json.dumps(payload, allow_nan=False) for payload in payloads]
            source = f"generated:synthetic:seed={options.get('seed', 42)}"
        elif "input_path" not in configuration:
            sources = generate_input_sources(configuration, repository, request_key or str(uuid.uuid4()),
                                             run_date=scheduled_date)
            return ingest_inputs(sources, repository, scheduled_date, request_key, connection_id)
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


def generate_input_sources(configuration, repository, generation_key, run_date=None):
    """Generate one scalable synthetic portfolio as reusable feed artifacts before database ingestion.

    Existing artifacts are reused on retry. A changed request must use a new
    generation run so previously ingested source files are not overwritten.
    """
    _, _, output_root = input_locations(configuration, repository)
    generation_id = str(uuid.uuid5(uuid.NAMESPACE_URL, generation_key))
    root = output_root / "generated" / generation_id
    request_path, config_path = root / "generation_request.json", root / "submission-000/run_config.json"
    previous_request = read_json(request_path) if request_path.exists() else None
    options = {"count": 1, "seed": 42, "noise": 0.1, "rejection_rate": 0,
               "counterparty_count": 2000, "trade_count": 10000, "horizon_days": 365,
               "simulation_paths": 20, "collateral_fraction": 0.7,
               **configuration.get("generation", {})}
    if options["count"] != 1:
        raise ValueError("xva_generate_inputs produces one portfolio; use generate_demo for alternative submissions")
    for field in ("counterparty_count", "trade_count"):
        integer(options[field], field, 1)
    options["valuation_date"] = (configuration.get("valuation_date") or run_date
                                  or (previous_request["options"]["valuation_date"] if previous_request else current_valuation_date()))
    request = {"options": options, "output_root": str(output_root)}
    if request_path.exists():
        if previous_request != request:
            raise ValueError("Generation retry changed its request; start a new generation run")
        if config_path.exists():
            saved = read_json(config_path)
            if all(Path(path).is_file() for path in saved["feeds"].values()):
                return saved
    payload = generate_submissions(**options)[0]
    handoff = write_feed_sets([payload], root)[0]
    handoff.update({"output_root": str(output_root), "generation_id": generation_id,
                    "generation_summary": {"counterparties": len(payload["counterparties"]),
                                           "trades": len(payload["trades"]), "csas": len(payload["csas"]),
                                           "seed": options["seed"], "simulation_paths": payload["simulation"]["paths"]}})
    write_json(config_path, handoff)
    write_json(request_path, request)
    return handoff


def ingest_one_feed(configuration, repository, request_key=None, connection_id=DEFAULT_CONNECTION_ID):
    """Stage one independently arriving source without assembling or triggering a portfolio calculation."""
    for field in ("feed_type", "input_path", "valuation_date"):
        if not configuration.get(field):
            raise ValueError(f"Feed ingestion requires {field}")
    _, path, _ = input_locations(configuration, repository)
    source = configuration.get("source", str(path.resolve()))
    feed_id = ingest_feed(configuration["feed_type"], path.read_text(), source,
                          configuration["valuation_date"], connection_id, request_key)
    return {"feed_id": feed_id, "feed_type": configuration["feed_type"], "source": source}


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
