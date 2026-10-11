"""Independent input feeds, row-level staging, and explicit portfolio assembly."""

import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import uuid

from risk_pipeline.xva.batch import build_snapshot, write_json
from risk_pipeline.xva.postgres import DEFAULT_CONNECTION_ID, transaction


# Nullable typed columns allow invalid feed rows to land before validation.
# raw_row retains each original entity, rather than a whole portfolio document.
FIELDS = {
    "counterparties": {"code": "TEXT", "name": "TEXT", "recovery_rate": "DOUBLE PRECISION", "credit_curve_id": "TEXT"},
    "credit_curves": {"curve_id": "TEXT", "valuation_date": "DATE"},
    "master_agreements": {"ma_id": "TEXT", "counterparty_code": "TEXT"},
    "csas": {"csa_id": "TEXT", "ma_id": "TEXT", "currency": "TEXT", "margin_frequency_days": "INTEGER",
             "settlement_lag_days": "INTEGER", "post_collateral": "BOOLEAN", "receive_collateral": "BOOLEAN",
             "posting_threshold": "DOUBLE PRECISION", "receiving_threshold": "DOUBLE PRECISION",
             "minimum_transfer_amount": "DOUBLE PRECISION", "initial_balance": "DOUBLE PRECISION"},
    "trades": {"trade_id": "TEXT", "trade_type": "TEXT", "counterparty_code": "TEXT", "ma_id": "TEXT",
               "csa_id": "TEXT", "currency": "TEXT", "notional": "DOUBLE PRECISION", "direction": "TEXT",
               "fixed_rate": "DOUBLE PRECISION", "start_date": "DATE", "maturity_date": "DATE"},
    "settings": {"valuation_date": "DATE", "reporting_currency": "TEXT", "flat_rate": "DOUBLE PRECISION",
                 "rate_volatility": "DOUBLE PRECISION", "paths": "INTEGER", "seed": "INTEGER",
                 "exposure_frequency_days": "INTEGER"},
}
POINT_FIELDS = {"date": "DATE", "survival_probability": "DOUBLE PRECISION"}
ENTITY_KEYS = {"counterparties": "code", "credit_curves": "curve_id", "master_agreements": "ma_id", "csas": "csa_id", "trades": "trade_id"}

METADATA_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS airflow_xva;
CREATE TABLE IF NOT EXISTS airflow_xva.input_ingestions (
 ingestion_id TEXT PRIMARY KEY, expected_submission_count INTEGER NOT NULL,
 source TEXT, request_key TEXT, received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS airflow_xva.input_feed_submissions (
 feed_id TEXT PRIMARY KEY, feed_type TEXT NOT NULL,
 source TEXT NOT NULL, request_key TEXT, content_hash TEXT NOT NULL,
 declared_valuation_date TEXT, valuation_date DATE, received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 status TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received','validated','rejected')),
 parse_error TEXT, raw_document TEXT
);
CREATE INDEX IF NOT EXISTS input_feed_catalog ON airflow_xva.input_feed_submissions (feed_type,valuation_date,received_at);
CREATE TABLE IF NOT EXISTS airflow_xva.input_sets (
 submission_id TEXT PRIMARY KEY, ingestion_id TEXT NOT NULL, submission_index INTEGER NOT NULL,
 declared_valuation_date TEXT, received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','accepted','rejected')),
 validated_at TIMESTAMPTZ, valuation_date DATE, batch_id TEXT, snapshot_path TEXT,
 selected_for_calculation BOOLEAN NOT NULL DEFAULT FALSE,
 UNIQUE (ingestion_id, submission_index),
 CHECK (NOT selected_for_calculation OR status='accepted')
);
CREATE UNIQUE INDEX IF NOT EXISTS selected_set_per_ingestion
 ON airflow_xva.input_sets (ingestion_id) WHERE selected_for_calculation;
CREATE TABLE IF NOT EXISTS airflow_xva.input_set_feeds (
 submission_id TEXT REFERENCES airflow_xva.input_sets ON DELETE CASCADE,
 feed_type TEXT NOT NULL, feed_id TEXT NOT NULL,
 PRIMARY KEY (submission_id,feed_type)
);
CREATE TABLE IF NOT EXISTS airflow_xva.input_validation_errors (
 error_id BIGSERIAL PRIMARY KEY,
 submission_id TEXT REFERENCES airflow_xva.input_sets ON DELETE CASCADE,
 feed_id TEXT, feed_type TEXT, row_index INTEGER, point_index INTEGER,
 field TEXT NOT NULL, message TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS input_error_submission ON airflow_xva.input_validation_errors (submission_id);
"""


def ensure_feed_schema(cursor):
    """Create feed metadata and entity tables without changing legacy input history or result tables."""
    cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("airflow_xva_feed_schema",))
    cursor.execute(METADATA_SCHEMA)
    for kind, fields in FIELDS.items():
        columns = ", ".join(f"{name} {sql_type}" for name, sql_type in fields.items())
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS airflow_xva.input_{kind} (
            feed_id TEXT REFERENCES airflow_xva.input_feed_submissions ON DELETE CASCADE,
            row_index INTEGER NOT NULL, raw_row JSONB NOT NULL, {columns},
            PRIMARY KEY (feed_id,row_index))""")
        if kind in ENTITY_KEYS:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS input_{kind}_identifier ON airflow_xva.input_{kind} (feed_id,{ENTITY_KEYS[kind]})")
    cursor.execute("""CREATE TABLE IF NOT EXISTS airflow_xva.input_credit_curve_points (
        feed_id TEXT NOT NULL, curve_row_index INTEGER NOT NULL, row_index INTEGER NOT NULL,
        raw_row JSONB NOT NULL, date DATE, survival_probability DOUBLE PRECISION,
        PRIMARY KEY (feed_id,curve_row_index,row_index),
        FOREIGN KEY (feed_id,curve_row_index) REFERENCES airflow_xva.input_credit_curves ON DELETE CASCADE)""")


def reject_constant(value):
    """Reject NaN and Infinity tokens while preserving malformed source text for audit."""
    raise ValueError(f"Nonstandard JSON constant: {value}")


def parse_payload(raw_text):
    """Decode strict JSON before business validation."""
    return json.loads(raw_text, parse_constant=reject_constant)


def typed_value(value, sql_type):
    """Convert well-typed raw scalars for staging; return null for invalid scalars so ingestion still succeeds."""
    try:
        if sql_type == "TEXT":
            return value if isinstance(value, str) else None
        if sql_type == "DATE":
            return date.fromisoformat(value) if isinstance(value, str) else None
        if sql_type == "BOOLEAN":
            return value if isinstance(value, bool) else None
        if sql_type == "INTEGER":
            return value if type(value) is int and -(2**31) <= value < 2**31 else None
        if sql_type == "DOUBLE PRECISION":
            return float(value) if type(value) in (int, float) and math.isfinite(value) else None
    except (ValueError, OverflowError):
        return None
    return None


def settings_row(payload):
    """Flatten the run/market settings feed into explicit relational columns."""
    if not isinstance(payload, dict) or ("market" not in payload and "simulation" not in payload):
        return payload
    market = payload.get("market") if isinstance(payload.get("market"), dict) else {}
    simulation = payload.get("simulation") if isinstance(payload.get("simulation"), dict) else {}
    return {"valuation_date": payload.get("valuation_date"), "reporting_currency": payload.get("reporting_currency"),
            **{field: market[field] for field in ("flat_rate", "rate_volatility") if field in market},
            **{field: simulation[field] for field in ("paths", "seed", "exposure_frequency_days") if field in simulation}}


def stage_rows(cursor, feed_id, kind, rows):
    """Store each entity and curve point separately, retaining invalid values in row-level audit JSON."""
    fields = FIELDS[kind]
    values, points = [], []
    for index, original in enumerate(rows):
        raw = dict(original) if isinstance(original, dict) else original
        if kind == "credit_curves" and isinstance(raw, dict) and isinstance(raw.get("points"), list):
            for point_index, point in enumerate(raw["points"]):
                point_values = point if isinstance(point, dict) else {}
                points.append((feed_id, index, point_index, json.dumps(point, allow_nan=False),
                               *[typed_value(point_values.get(name), sql_type) for name, sql_type in POINT_FIELDS.items()]))
            raw["points"] = []
        row = raw if isinstance(raw, dict) else {}
        values.append((feed_id, index, json.dumps(raw, allow_nan=False),
                       *[typed_value(row.get(name), sql_type) for name, sql_type in fields.items()]))
    placeholders = ", ".join(["%s", "%s", "%s::jsonb", *["%s" for _ in fields]])
    cursor.executemany(f"INSERT INTO airflow_xva.input_{kind} (feed_id,row_index,raw_row,{', '.join(fields)}) VALUES ({placeholders})", values)
    if points:
        cursor.executemany("""INSERT INTO airflow_xva.input_credit_curve_points
            (feed_id,curve_row_index,row_index,raw_row,date,survival_probability) VALUES (%s,%s,%s,%s::jsonb,%s,%s)""", points)


def ingest_feed(feed_type, raw_text, source, valuation_date=None, connection_id=DEFAULT_CONNECTION_ID, request_key=None):
    """Commit one independently sourced feed before validation; retries reuse unchanged versions."""
    if feed_type not in FIELDS:
        raise ValueError(f"Unknown feed type: {feed_type}")
    feed_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"feed:{request_key}")) if request_key else str(uuid.uuid4())
    digest = hashlib.sha256(raw_text.encode()).hexdigest()
    error, rows = None, []
    try:
        payload = parse_payload(raw_text)
        if isinstance(payload, dict) and "rows" in payload:
            valuation_date = payload.get("valuation_date", valuation_date)
            payload = payload["rows"]
        if feed_type == "settings" and isinstance(payload, dict):
            payload = [settings_row(payload)]
        if not isinstance(payload, list):
            raise ValueError("Feed must contain an array of rows (settings may be a single object)")
        rows = [settings_row(row) for row in payload] if feed_type == "settings" else payload
        # Detect nonfinite numbers produced by oversized JSON exponents before any row INSERT.
        json.dumps(rows, allow_nan=False)
    except (ValueError, TypeError) as failure:
        error = f"{type(failure).__name__}: {failure}"
    declared = str(valuation_date) if valuation_date is not None else None
    with transaction(connection_id) as cursor:
        ensure_feed_schema(cursor)
        cursor.execute("SELECT feed_type,source,content_hash,declared_valuation_date FROM airflow_xva.input_feed_submissions WHERE feed_id=%s", (feed_id,))
        previous = cursor.fetchone()
        if previous is not None:
            if previous != (feed_type, source, digest, declared):
                raise ValueError("Feed retry changed source, date, or contents; submit a new feed version")
        else:
            cursor.execute("""INSERT INTO airflow_xva.input_feed_submissions
                (feed_id,feed_type,source,request_key,content_hash,declared_valuation_date,valuation_date,parse_error,raw_document)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (feed_id, feed_type, source, request_key, digest, declared, typed_value(declared, "DATE"), error, raw_text if error else None))
            if error is None:
                stage_rows(cursor, feed_id, feed_type, rows)
    return feed_id


def create_input_set(feed_ids, valuation_date, connection_id=DEFAULT_CONNECTION_ID, request_key=None,
                     ingestion_id=None, submission_index=0):
    """Bind explicit feed versions to one valuation date without validating their business contents."""
    unknown = set(feed_ids) - set(FIELDS)
    if unknown:
        raise ValueError(f"Unknown feed types: {sorted(unknown)}")
    ingestion_id = ingestion_id or (str(uuid.uuid5(uuid.NAMESPACE_URL, request_key)) if request_key else str(uuid.uuid4()))
    submission_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"input-set:{ingestion_id}:{submission_index}"))
    declared = str(valuation_date) if valuation_date is not None else None
    with transaction(connection_id) as cursor:
        ensure_feed_schema(cursor)
        cursor.execute("""INSERT INTO airflow_xva.input_ingestions (ingestion_id,expected_submission_count)
            VALUES (%s,1) ON CONFLICT DO NOTHING""", (ingestion_id,))
        cursor.execute("SELECT declared_valuation_date FROM airflow_xva.input_sets WHERE submission_id=%s", (submission_id,))
        previous = cursor.fetchone()
        if previous is not None:
            cursor.execute("SELECT feed_type,feed_id FROM airflow_xva.input_set_feeds WHERE submission_id=%s", (submission_id,))
            if previous[0] != declared or dict(cursor.fetchall()) != feed_ids:
                raise ValueError("Input-set retry changed its date or feed versions; start a new run")
        else:
            cursor.execute("""INSERT INTO airflow_xva.input_sets (submission_id,ingestion_id,submission_index,declared_valuation_date)
                VALUES (%s,%s,%s,%s)""", (submission_id, ingestion_id, submission_index, declared))
            cursor.executemany("INSERT INTO airflow_xva.input_set_feeds VALUES (%s,%s,%s)",
                               [(submission_id, kind, feed_id) for kind, feed_id in feed_ids.items()])
    return {"ingestion_id": ingestion_id, "connection_id": connection_id}


def ingest_submissions(raw_texts, source, connection_id=DEFAULT_CONNECTION_ID, request_key=None, ingestion_id=None):
    """Adapt legacy portfolio documents or generated alternatives into separate feed versions and input sets."""
    if not raw_texts:
        raise ValueError("An ingestion requires at least one submission")
    ingestion_id = ingestion_id or (str(uuid.uuid5(uuid.NAMESPACE_URL, request_key)) if request_key else str(uuid.uuid4()))
    with transaction(connection_id) as cursor:
        ensure_feed_schema(cursor)
        cursor.execute("SELECT expected_submission_count,source FROM airflow_xva.input_ingestions WHERE ingestion_id=%s", (ingestion_id,))
        previous = cursor.fetchone()
        if previous and previous != (len(raw_texts), source):
            raise ValueError("Ingestion retry changed its source or submission count; start a new run")
        if not previous:
            cursor.execute("""INSERT INTO airflow_xva.input_ingestions
                (ingestion_id,expected_submission_count,source,request_key) VALUES (%s,%s,%s,%s)""",
                (ingestion_id, len(raw_texts), source, request_key))
    for index, raw_text in enumerate(raw_texts):
        try:
            payload = parse_payload(raw_text)
            json.dumps(payload, allow_nan=False)
        except ValueError:
            payload = None
        feeds = {}
        if isinstance(payload, dict):
            for kind in FIELDS:
                if kind == "settings":
                    data = settings_row({key: payload[key] for key in ("valuation_date", "reporting_currency", "market", "simulation") if key in payload})
                else:
                    data = payload.get(kind, [])
                feeds[kind] = ingest_feed(kind, json.dumps(data, allow_nan=False), f"{source}#{kind}", payload.get("valuation_date"),
                                          connection_id, request_key=f"{ingestion_id}:{index}:{kind}")
        else:
            feeds["settings"] = ingest_feed("settings", raw_text, source, connection_id=connection_id,
                                               request_key=f"{ingestion_id}:{index}:settings")
        create_input_set(feeds, payload.get("valuation_date") if isinstance(payload, dict) else None,
                         connection_id, ingestion_id=ingestion_id, submission_index=index)
    return {"ingestion_id": ingestion_id, "connection_id": connection_id}


def read_feed(cursor, feed_id, kind, raw=False):
    """Read one feed from relational columns, or its row-level originals for validation diagnostics."""
    fields = FIELDS[kind]
    cursor.execute(f"SELECT row_index, raw_row, {', '.join(fields)} FROM airflow_xva.input_{kind} WHERE feed_id=%s ORDER BY row_index", (feed_id,))
    entities = cursor.fetchall()
    points_by_curve = {}
    if kind == "credit_curves":
        cursor.execute("""SELECT curve_row_index,raw_row,date,survival_probability
            FROM airflow_xva.input_credit_curve_points WHERE feed_id=%s ORDER BY curve_row_index,row_index""", (feed_id,))
        for curve_index, original, when, survival in cursor.fetchall():
            point = original if raw else {"date": when.isoformat() if when else None, "survival_probability": survival}
            points_by_curve.setdefault(curve_index, []).append(point)
    rows = []
    for index, original, *values in entities:
        row = original if raw else {name: value.isoformat() if isinstance(value, date) else value for name, value in zip(fields, values)}
        if kind == "credit_curves" and isinstance(row, dict) and (not raw or isinstance(row.get("points"), list)):
            row = dict(row)
            row["points"] = points_by_curve.get(index, [])
        rows.append(row)
    return rows


def row_errors(kind, row, feed_id, index, point_index=None):
    """Report missing, mistyped, or out-of-range fields against the exact source row."""
    errors = []
    fields = POINT_FIELDS if point_index is not None else FIELDS[kind]
    if not isinstance(row, dict):
        return [{"feed_id": feed_id, "feed_type": kind, "row_index": index, "point_index": point_index,
                 "field": "row", "message": "Row must be a JSON object"}]
    for name, sql_type in fields.items():
        value = row.get(name)
        message = None
        if name not in row:
            message = "Required field is missing"
        elif kind == "trades" and name == "csa_id" and value is None:
            continue
        elif typed_value(value, sql_type) is None or (sql_type == "TEXT" and not value.strip()):
            message = f"Invalid {sql_type.lower()} value"
        elif name in ("code", "counterparty_code") and len(value) != 7:
            message = "Counterparty code must contain exactly seven characters"
        elif name in ("currency", "reporting_currency") and value != "EUR":
            message = "Only EUR is supported"
        elif name == "trade_type" and value != "interest_rate_swap":
            message = "Only interest_rate_swap is supported"
        elif name == "direction" and value not in ("pay_fixed", "receive_fixed"):
            message = "Invalid swap direction"
        elif name == "recovery_rate" and not 0 <= value <= 1:
            message = "Recovery rate must be between 0 and 1"
        elif name == "survival_probability" and not 0 < value <= 1:
            message = "Survival probability must be greater than 0 and at most 1"
        elif name in ("flat_rate", "fixed_rate") and not -0.1 <= value <= 1:
            message = "Rate must be between -0.1 and 1"
        elif name == "rate_volatility" and not 0 <= value <= 0.1:
            message = "Rate volatility must be between 0 and 0.1"
        elif name in ("notional", "paths", "margin_frequency_days", "exposure_frequency_days") and value <= 0:
            message = "Value must be positive"
        elif name in ("seed", "settlement_lag_days", "posting_threshold", "receiving_threshold", "minimum_transfer_amount") and value < 0:
            message = "Value must be nonnegative"
        if message:
            errors.append({"feed_id": feed_id, "feed_type": kind, "row_index": index, "point_index": point_index,
                           "field": name, "message": message})
    return errors


def assemble_set(cursor, submission_id, declared, raw=False):
    """Assemble an explicit feed set and collect feed/row defects without dropping invalid entities."""
    cursor.execute("SELECT feed_type,feed_id FROM airflow_xva.input_set_feeds WHERE submission_id=%s", (submission_id,))
    references = dict(cursor.fetchall())
    payload, errors = {"valuation_date": declared}, []
    for kind in FIELDS:
        feed_id = references.get(kind)
        if not feed_id:
            errors.append({"feed_type": kind, "field": "feed", "message": "Required feed is missing"})
            continue
        cursor.execute("SELECT feed_type,declared_valuation_date,parse_error FROM airflow_xva.input_feed_submissions WHERE feed_id=%s", (feed_id,))
        metadata = cursor.fetchone()
        if metadata is None or metadata[0] != kind:
            errors.append({"feed_id": feed_id, "feed_type": kind, "field": "feed", "message": "Feed ID is missing or has the wrong type"})
            continue
        if metadata[1] != declared:
            errors.append({"feed_id": feed_id, "feed_type": kind, "field": "valuation_date", "message": "Feed valuation date differs from the input set"})
        if metadata[2]:
            errors.append({"feed_id": feed_id, "feed_type": kind, "field": "document", "message": metadata[2]})
        rows = read_feed(cursor, feed_id, kind, raw=raw)
        if not rows and kind != "csas":
            errors.append({"feed_id": feed_id, "feed_type": kind, "field": "rows", "message": "Required feed is empty"})
        seen = set()
        for index, row in enumerate(rows):
            errors.extend(row_errors(kind, row, feed_id, index))
            if kind in ENTITY_KEYS and isinstance(row, dict):
                key = row.get(ENTITY_KEYS[kind])
                if isinstance(key, str):
                    if key in seen:
                        errors.append({"feed_id": feed_id, "feed_type": kind, "row_index": index,
                                       "field": ENTITY_KEYS[kind], "message": "Duplicate entity identifier"})
                    seen.add(key)
            if kind == "credit_curves" and isinstance(row, dict):
                if not isinstance(row.get("points"), list) or not row["points"]:
                    errors.append({"feed_id": feed_id, "feed_type": kind, "row_index": index,
                                   "field": "points", "message": "Credit curve requires a nonempty points array"})
                else:
                    for point_index, point in enumerate(row["points"]):
                        errors.extend(row_errors(kind, point, feed_id, index, point_index))
        if kind == "settings":
            if len(rows) != 1:
                errors.append({"feed_id": feed_id, "feed_type": kind, "field": "rows", "message": "Settings feed must contain exactly one row"})
            elif isinstance(rows[0], dict):
                settings = rows[0]
                payload.update({"reporting_currency": settings.get("reporting_currency"),
                                "market": {field: settings.get(field) for field in ("flat_rate", "rate_volatility")},
                                "simulation": {field: settings.get(field) for field in ("paths", "seed", "exposure_frequency_days")}})
                if settings.get("valuation_date") != declared:
                    errors.append({"feed_id": feed_id, "feed_type": kind, "row_index": 0,
                                   "field": "valuation_date", "message": "Settings date differs from the input set"})
        else:
            payload[kind] = rows
    return payload, references, errors


def cross_feed_errors(payload, references):
    """Locate cross-source allocation and date defects at the originating feed row."""
    errors = []
    def add(kind, index, field, message, point_index=None):
        """Append a structured cross-feed error with source identifiers."""
        errors.append({"feed_id": references[kind], "feed_type": kind, "row_index": index,
                       "point_index": point_index, "field": field, "message": message})
    counterparties = {row["code"]: row for row in payload["counterparties"]}
    curves = {row["curve_id"]: row for row in payload["credit_curves"]}
    agreements = {row["ma_id"]: row for row in payload["master_agreements"]}
    csas = {row["ma_id"]: row for row in payload["csas"]}
    valuation = date.fromisoformat(payload["valuation_date"])
    horizon = max(date.fromisoformat(row["maturity_date"]) for row in payload["trades"])
    for index, row in enumerate(payload["counterparties"]):
        if row["credit_curve_id"] not in curves:
            add("counterparties", index, "credit_curve_id", "Unknown credit curve")
    for index, row in enumerate(payload["master_agreements"]):
        if row["counterparty_code"] not in counterparties:
            add("master_agreements", index, "counterparty_code", "Unknown counterparty")
    allocated = set()
    for index, row in enumerate(payload["csas"]):
        if row["ma_id"] not in agreements or row["ma_id"] in allocated:
            add("csas", index, "ma_id", "Unknown MA or more than one CSA for this MA")
        allocated.add(row["ma_id"])
        if (row["initial_balance"] > 0 and not row["receive_collateral"]) or (row["initial_balance"] < 0 and not row["post_collateral"]):
            add("csas", index, "initial_balance", "Balance conflicts with collateral permissions")
    for index, row in enumerate(payload["trades"]):
        ma = agreements.get(row["ma_id"])
        if ma is None:
            add("trades", index, "ma_id", "Unknown master agreement")
        elif ma["counterparty_code"] != row["counterparty_code"]:
            add("trades", index, "counterparty_code", "Trade counterparty differs from its MA")
        expected = csas.get(row["ma_id"], {}).get("csa_id")
        if row["csa_id"] != expected:
            add("trades", index, "csa_id", "Trade CSA differs from its MA allocation")
        if date.fromisoformat(row["start_date"]) > valuation:
            add("trades", index, "start_date", "Trade has not started at valuation date")
        if date.fromisoformat(row["maturity_date"]) <= valuation:
            add("trades", index, "maturity_date", "Trade has matured at valuation date")
    for index, row in enumerate(payload["credit_curves"]):
        if row["valuation_date"] != payload["valuation_date"]:
            add("credit_curves", index, "valuation_date", "Curve date differs from the input set")
        previous_date, previous_survival = valuation, 1.0
        for point_index, point in enumerate(row["points"]):
            when = date.fromisoformat(point["date"])
            if when <= previous_date:
                add("credit_curves", index, "date", "Curve point dates must increase after valuation", point_index)
            if point["survival_probability"] > previous_survival:
                add("credit_curves", index, "survival_probability", "Survival probability must not increase", point_index)
            previous_date, previous_survival = when, point["survival_probability"]
        if previous_date < horizon:
            add("credit_curves", index, "points", "Curve does not cover all trade maturities")
    return errors


def migrate_legacy_ingestion(ingestion_id, connection_id):
    """Convert a referenced legacy ingestion into feeds without altering or deleting its original audit records."""
    with transaction(connection_id, readonly=True) as cursor:
        cursor.execute("SELECT to_regclass('airflow_xva.input_submissions')")
        if cursor.fetchone()[0] is None:
            raise ValueError("Input ingestion is missing from the database")
        cursor.execute("SELECT raw_text,source FROM airflow_xva.input_submissions WHERE ingestion_id=%s ORDER BY submission_index", (ingestion_id,))
        legacy = cursor.fetchall()
    if not legacy:
        raise ValueError("Input ingestion is missing from the database")
    # Legacy ingestions used one common source for all their portfolio alternatives.
    return ingest_submissions([row[0] for row in legacy], legacy[0][1], connection_id, ingestion_id=ingestion_id)


def validate_ingestion(handoff):
    """Validate explicit feed sets, commit structured errors, and freeze the final accepted normalized portfolio."""
    ingestion_id, connection_id = handoff["ingestion_id"], handoff["connection_id"]
    with transaction(connection_id) as cursor:
        ensure_feed_schema(cursor)
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.input_sets WHERE ingestion_id=%s", (ingestion_id,))
        exists = cursor.fetchone()[0]
        cursor.execute("SELECT expected_submission_count FROM airflow_xva.input_ingestions WHERE ingestion_id=%s", (ingestion_id,))
        expected = cursor.fetchone()
        if expected and exists != expected[0]:
            raise ValueError(f"Input ingestion is incomplete: received {exists} of {expected[0]} input sets; resume ingestion")
    if not exists:
        migrate_legacy_ingestion(ingestion_id, connection_id)
    accepted, rejected = [], 0
    with transaction(connection_id) as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"xva_validate:{ingestion_id}",))
        cursor.execute("""SELECT submission_id,declared_valuation_date,status,batch_id FROM airflow_xva.input_sets
            WHERE ingestion_id=%s ORDER BY submission_index FOR UPDATE""", (ingestion_id,))
        sets = cursor.fetchall()
        for submission_id, declared, status, stored_batch_id in sets:
            if status == "rejected":
                rejected += 1
                continue
            payload, references, errors = assemble_set(cursor, submission_id, declared, raw=status == "pending")
            source_errors = list(errors)
            if handoff.get("scheduled_date") and declared != handoff["scheduled_date"]:
                if status == "accepted":
                    raise ValueError("Accepted ingestion valuation date does not match the requested scheduled date")
                errors.append({"field": "valuation_date", "message": "Scheduled input valuation date must match the data interval start date"})
            if not errors:
                try:
                    errors.extend(cross_feed_errors(payload, references))
                except (ValueError, TypeError, KeyError) as error:
                    errors.append({"field": "valuation_date", "message": str(error)})
            snapshot = None
            if not errors:
                try:
                    normalized, _, _ = assemble_set(cursor, submission_id, declared)
                    snapshot = build_snapshot(normalized)
                except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as error:
                    errors.append({"field": "portfolio", "message": f"{type(error).__name__}: {error}"})
            if status == "accepted":
                if errors or snapshot["batch_id"] != stored_batch_id:
                    raise ValueError("Accepted normalized input rows changed; ingest new feed versions")
                accepted.append((submission_id, snapshot))
                continue
            for kind, feed_id in references.items():
                # Feed status reflects its own shape/types, not contextual defects
                # caused by another source or by binding the wrong valuation date.
                own_errors = [error for error in source_errors if error.get("feed_id") == feed_id
                              and "differs from the input set" not in error["message"]]
                cursor.execute("UPDATE airflow_xva.input_feed_submissions SET status=%s WHERE feed_id=%s AND feed_type=%s",
                               ("rejected" if own_errors else "validated", feed_id, kind))
            if errors:
                cursor.executemany("""INSERT INTO airflow_xva.input_validation_errors
                    (submission_id,feed_id,feed_type,row_index,point_index,field,message) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    [(submission_id, error.get("feed_id"), error.get("feed_type"), error.get("row_index"),
                      error.get("point_index"), error["field"], error["message"]) for error in errors])
                cursor.execute("UPDATE airflow_xva.input_sets SET status='rejected',validated_at=clock_timestamp() WHERE submission_id=%s", (submission_id,))
                rejected += 1
            else:
                cursor.execute("""UPDATE airflow_xva.input_sets SET status='accepted',valuation_date=%s,batch_id=%s,
                    validated_at=clock_timestamp() WHERE submission_id=%s""", (declared, snapshot["batch_id"], submission_id))
                accepted.append((submission_id, snapshot))
        if accepted:
            submission_id, snapshot = accepted[-1]
            cursor.execute("UPDATE airflow_xva.input_sets SET selected_for_calculation=FALSE WHERE ingestion_id=%s AND selected_for_calculation", (ingestion_id,))
            cursor.execute("UPDATE airflow_xva.input_sets SET selected_for_calculation=TRUE WHERE submission_id=%s", (submission_id,))
    if not accepted:
        raise ValueError(f"All {rejected} input submissions were rejected; inspect airflow_xva.input_validation_errors")
    snapshot_path = write_json(Path(handoff["output_root"]) / snapshot["batch_id"] / "inputs.json", snapshot)
    with transaction(connection_id) as cursor:
        cursor.execute("UPDATE airflow_xva.input_sets SET snapshot_path=%s WHERE submission_id=%s", (snapshot_path, submission_id))
    return {"batch_id": snapshot["batch_id"], "snapshot_path": snapshot_path, "ingestion_id": ingestion_id,
            "submission_id": submission_id, "accepted_count": len(accepted), "rejected_count": rejected}


def main():
    """Ingest an individual source/feed from the CLI and print its version ID for later explicit assembly."""
    parser = argparse.ArgumentParser(description="Ingest an independent XVA feed before validation")
    parser.add_argument("--feed-type", required=True, choices=FIELDS)
    parser.add_argument("--input", required=True)
    parser.add_argument("--valuation-date", required=True)
    parser.add_argument("--source")
    parser.add_argument("--request-key")
    parser.add_argument("--connection-id", default=DEFAULT_CONNECTION_ID)
    args = parser.parse_args()
    path = Path(args.input).expanduser().resolve()
    feed_id = ingest_feed(args.feed_type, path.read_text(), args.source or str(path), args.valuation_date,
                          args.connection_id, args.request_key)
    print(json.dumps({"feed_type": args.feed_type, "feed_id": feed_id}))


if __name__ == "__main__":
    main()
