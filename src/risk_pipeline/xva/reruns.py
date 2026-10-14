"""Audited counterparty recalculation against immutable corrected feed versions."""

import copy
from datetime import date
import json
import math
from pathlib import Path
import uuid

from risk_pipeline.xva.batch import build_snapshot, insert_results, load_snapshot, read_json, validate_snapshot, write_json
from risk_pipeline.xva.calculations import calculate, generate_scenarios, reconcile, reporting_days
from risk_pipeline.xva.feeds import FIELDS, assemble_set, validate_ingestion
from risk_pipeline.xva.generation import write_feed_sets
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID, OUTPUT_COLUMNS, ensure_schema, transaction
from risk_pipeline.xva.reports import read_results
from risk_pipeline.xva.workflow import ingest_inputs, input_locations


def outside_counterparty(inputs, code):
    """Select everything affecting counterparties outside the requested recalculation scope."""
    other = copy.deepcopy(inputs)
    other["counterparties"] = [c for c in inputs["counterparties"] if c["code"] != code]
    other["master_agreements"] = [ma for ma in inputs["master_agreements"] if ma["counterparty_code"] != code]
    ma_ids = {ma["ma_id"] for ma in other["master_agreements"]}
    curve_ids = {c["credit_curve_id"] for c in other["counterparties"]}
    other["credit_curves"] = [curve for curve in inputs["credit_curves"] if curve["curve_id"] in curve_ids]
    other["csas"] = [csa for csa in inputs["csas"] if csa["ma_id"] in ma_ids]
    other["trades"] = [trade for trade in inputs["trades"] if trade["counterparty_code"] != code]
    keys = {"counterparties": "code", "credit_curves": "curve_id", "master_agreements": "ma_id",
            "csas": "csa_id", "trades": "trade_id"}
    for kind, key in keys.items():
        other[kind] = sorted(other[kind], key=lambda row: row[key])
    return other


def check_scope(base, updated, code):
    """Reject outside-scope changes or any change to the shared exposure reporting grid."""
    for inputs in (base, updated):
        if code not in {c["code"] for c in inputs["counterparties"]}:
            raise ValueError("Counterparty reruns require an existing counterparty in both input versions")
    if outside_counterparty(base, code) != outside_counterparty(updated, code):
        raise ValueError("Inputs outside this counterparty changed; use a full batch rerun")
    valuation = date.fromisoformat(base["valuation_date"])
    grids = []
    for inputs in (base, updated):
        horizon = max((date.fromisoformat(t["maturity_date"]) - valuation).days for t in inputs["trades"])
        grids.append((horizon, reporting_days(inputs, horizon)))
    if grids[0] != grids[1]:
        raise ValueError("Shared scenario or reporting grid changed; use a full batch rerun")


def prepare_counterparty_rerun(configuration, repository, request_key, connection_id=DEFAULT_CONNECTION_ID):
    """Bind corrected feed versions to the original portfolio and freeze a scoped, retryable rerun."""
    valuation_date, code = configuration.get("valuation_date"), configuration.get("counterparty_code")
    if not isinstance(valuation_date, str) or not valuation_date or not isinstance(code, str) or len(code) != 7:
        raise ValueError("Counterparty rerun requires valuation_date and a seven-character counterparty_code")
    date.fromisoformat(valuation_date)
    specs = configuration.get("feeds")
    if not isinstance(specs, dict) or not specs or set(specs) - set(FIELDS):
        raise ValueError("Counterparty rerun requires updated feeds: file paths or feed IDs")
    _, _, output_root = input_locations(configuration, repository)
    rerun_id = str(uuid.uuid5(uuid.NAMESPACE_URL, request_key))
    request = json.loads(json.dumps({"configuration": configuration, "repository": str(Path(repository).resolve())}))
    with transaction(connection_id) as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("airflow_xva_publication",))
        ensure_schema(cursor)
        cursor.execute("SELECT base_batch_id,request FROM airflow_xva.counterparty_reruns WHERE rerun_id=%s", (rerun_id,))
        previous = cursor.fetchone()
        if previous:
            if previous[1] != request:
                raise ValueError("Rerun retry changed its request; start a new rerun")
            base_batch_id = previous[0]
        else:
            cursor.execute("SELECT batch_id FROM airflow_xva.batches WHERE valuation_date=%s AND is_current", (valuation_date,))
            current = cursor.fetchone()
            if not current:
                raise ValueError("No published batch for this valuation date; run the full batch first")
            base_batch_id = current[0]
            cursor.execute("""INSERT INTO airflow_xva.counterparty_reruns
                (rerun_id,valuation_date,counterparty_code,base_batch_id,request) VALUES (%s,%s,%s,%s,%s::jsonb)""",
                (rerun_id, valuation_date, code, base_batch_id, json.dumps(request)))
        cursor.execute("SELECT input_snapshot FROM airflow_xva.batches WHERE batch_id=%s", (base_batch_id,))
        base = validate_snapshot(cursor.fetchone()[0])
        original_feeds = {}
        cursor.execute("SELECT to_regclass('airflow_xva.input_sets')")
        if cursor.fetchone()[0] is not None:
            cursor.execute("""SELECT submission_id FROM airflow_xva.input_sets
                WHERE batch_id=%s AND status='accepted' AND selected_for_calculation
                ORDER BY validated_at DESC LIMIT 1""", (base_batch_id,))
            original_set = cursor.fetchone()
            if original_set:
                baseline_payload, _, errors = assemble_set(cursor, original_set[0], valuation_date)
                if errors or build_snapshot(baseline_payload)["batch_id"] != base_batch_id:
                    raise ValueError("Published input rows changed; submit new feed versions instead of editing accepted rows")
                cursor.execute("SELECT feed_type,feed_id FROM airflow_xva.input_set_feeds WHERE submission_id=%s", original_set)
                original_feeds = {kind: {"feed_id": feed_id} for kind, feed_id in cursor.fetchall()}
    # Reuse exact baseline feed IDs, never regenerate random inputs or guess the latest feed.
    directory = output_root / "reruns" / rerun_id
    if set(original_feeds) != set(FIELDS):
        # Imported publications may predate feed ingestion; reconstruct their immutable snapshot.
        original_feeds = write_feed_sets([base["inputs"]], directory / "baseline")[0]["feeds"]
    configured = {"valuation_date": valuation_date, "output_root": str(output_root),
                  "feeds": {**original_feeds, **specs}}
    ingestion = ingest_inputs(configured, repository, request_key=f"counterparty-rerun:{rerun_id}",
                              connection_id=connection_id)
    frozen = validate_ingestion(ingestion)
    updated = load_snapshot(frozen["snapshot_path"])
    check_scope(base["inputs"], updated["inputs"], code)
    with transaction(connection_id) as cursor:
        cursor.execute("""UPDATE airflow_xva.counterparty_reruns SET snapshot_batch_id=%s,ingestion_id=%s,submission_id=%s
            WHERE rerun_id=%s AND (snapshot_batch_id IS NULL OR snapshot_batch_id=%s)""",
            (frozen["batch_id"], frozen["ingestion_id"], frozen["submission_id"], rerun_id, frozen["batch_id"]))
        if cursor.rowcount != 1:
            raise ValueError("Validated rerun snapshot changed; start a new rerun")
    handoff = {**frozen, "rerun_id": rerun_id, "base_batch_id": base_batch_id,
               "counterparty_code": code, "connection_id": connection_id, "directory": str(directory)}
    write_json(directory / "handoff.json", handoff)
    return handoff


def calculate_counterparty(handoff):
    """Price only the requested counterparty while retaining the original portfolio-wide scenario grid."""
    snapshot = load_snapshot(handoff["snapshot_path"])
    scenarios = generate_scenarios(snapshot["inputs"])
    write_json(Path(handoff["directory"]) / "scenarios.json", {"batch_id": snapshot["batch_id"], "scenarios": scenarios})
    results = calculate(snapshot["inputs"], scenarios, counterparty_code=handoff["counterparty_code"])
    reconcile(results)
    return write_json(Path(handoff["directory"]) / "results.json", {"batch_id": snapshot["batch_id"], "results": results})


def publish_counterparty(handoff, result_path):
    """Atomically replace one counterparty's outputs, record its CVA correction, and publish a complete revision.

    Reject stale base revisions. Retries after commit reconstruct the manifest
    without updating outputs or timestamps again. Other XVA types are preserved.
    """
    snapshot = load_snapshot(handoff["snapshot_path"])
    payload = read_json(result_path)
    code = handoff["counterparty_code"]
    if payload["batch_id"] != snapshot["batch_id"]:
        raise ValueError("Counterparty results belong to another snapshot")
    results = payload["results"]
    if any(row["counterparty_code"] != code for rows in results.values() for row in rows):
        raise ValueError("Counterparty results contain another counterparty")
    inputs, batch_id = snapshot["inputs"], snapshot["batch_id"]
    valuation_date, connection_id = inputs["valuation_date"], handoff["connection_id"]
    with transaction(connection_id) as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("airflow_xva_publication",))
        ensure_schema(cursor)
        cursor.execute("""SELECT base_batch_id,status,published_batch_id,valuation_date,counterparty_code,snapshot_batch_id
            FROM airflow_xva.counterparty_reruns WHERE rerun_id=%s FOR UPDATE""", (handoff["rerun_id"],))
        audit = cursor.fetchone()
        if audit is None or (audit[0], audit[3].isoformat(), audit[4], audit[5]) != (handoff["base_batch_id"], valuation_date, code, batch_id):
            raise ValueError("Counterparty rerun identity does not match its audit record")
        cursor.execute("SELECT batch_id,input_snapshot FROM airflow_xva.batches WHERE valuation_date=%s AND is_current", (valuation_date,))
        current = cursor.fetchone()
        required = audit[2] if audit[1] == "published" else audit[0]
        if current is None or current[0] != required:
            raise ValueError("Base batch was superseded; start a new counterparty rerun")
        if audit[1] == "published" and audit[2] != batch_id:
            raise ValueError("Published rerun snapshot changed; start a new rerun")
        complete = read_results(cursor, valuation_date)
        if audit[1] != "published":
            base = validate_snapshot(current[1])
            check_scope(base["inputs"], inputs, code)
            old_cva = next(row["cva"] for row in complete["counterparty_cva"] if row["counterparty_code"] == code)
            complete = {kind: [row for row in rows if row["counterparty_code"] != code] + results[kind]
                        for kind, rows in complete.items()}
            reconcile(complete, inputs)
            for table in OUTPUT_COLUMNS:
                type_filter = " AND xva_type='CVA'" if table == "counterparty_xva" else ""
                cursor.execute(f"DELETE FROM airflow_xva.{table} WHERE valuation_date=%s AND counterparty_code=%s{type_filter}",
                               (valuation_date, code))
            insert_results(cursor, valuation_date, results)
            cursor.execute("UPDATE airflow_xva.batches SET is_current=FALSE WHERE valuation_date=%s", (valuation_date,))
            cursor.execute("""INSERT INTO airflow_xva.batches
                (batch_id,valuation_date,model_version,currency,input_snapshot,is_current)
                VALUES (%s,%s,%s,%s,%s::jsonb,TRUE) ON CONFLICT (batch_id) DO UPDATE SET is_current=TRUE""",
                (batch_id, valuation_date, snapshot["model_version"], inputs["reporting_currency"], json.dumps(snapshot)))
            new_cva = next(row["cva"] for row in results["counterparty_cva"] if row["counterparty_code"] == code)
            cursor.execute("""UPDATE airflow_xva.counterparty_reruns SET status='published',published_batch_id=%s,
                old_cva=%s,new_cva=%s,published_at=clock_timestamp() WHERE rerun_id=%s""",
                (batch_id, old_cva, new_cva, handoff["rerun_id"]))
        else:
            reconcile(complete, inputs)
    manifest = {"batch_id": batch_id, "valuation_date": valuation_date, "status": "published", "storage": "postgres",
                "connection_id": connection_id, "trade_count": len(complete["trade_pv"]),
                "netting_set_count": len(complete["netting_set_cva"]),
                "total_cva": math.fsum(row["cva"] for row in complete["counterparty_cva"]),
                "rerun_id": handoff["rerun_id"], "counterparty_code": code, "base_batch_id": handoff["base_batch_id"]}
    return write_json(Path(handoff["snapshot_path"]).parent / "published.json", manifest)
