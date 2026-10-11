from concurrent.futures import ThreadPoolExecutor
import csv
from pathlib import Path

import pytest
from airflow.providers.postgres.hooks.postgres import USE_PSYCOPG3

from risk_pipeline.xva.batch import publish, read_json, run_batch, write_json
from risk_pipeline.xva.postgres import OUTPUT_COLUMNS, ensure_schema, transaction
from risk_pipeline.xva.reports import generate_reports, load_published_results

if USE_PSYCOPG3:
    from psycopg import Error as DatabaseError
else:
    from psycopg2 import Error as DatabaseError


@pytest.fixture
def publication(tmp_path, pg_connection):
    inputs = read_json(Path(__file__).resolve().parents[1] / "examples/xva/inputs.json")
    inputs["simulation"]["paths"] = 2
    source = write_json(tmp_path / "inputs.json", inputs)
    return run_batch(source, tmp_path / "output", pg_connection)


@pytest.mark.parametrize("new_batch", [False, True])
def test_failed_republication_rolls_back_previous_batch(publication, pg_connection, tmp_path, new_batch):
    before = load_published_results(publication)
    manifest_before = Path(publication).read_bytes()
    with transaction(pg_connection) as cursor:
        cursor.execute("""
            CREATE FUNCTION airflow_xva.fail_test_insert() RETURNS trigger AS $$
            BEGIN RAISE EXCEPTION 'injected integration-test failure'; END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER fail_test_insert BEFORE INSERT ON airflow_xva.exposures
            FOR EACH ROW EXECUTE FUNCTION airflow_xva.fail_test_insert();
        """)
    with pytest.raises(DatabaseError):
        if new_batch:
            inputs = read_json(Path(publication).parent / "inputs.json")["inputs"]
            inputs["simulation"]["seed"] += 1
            source = write_json(tmp_path / "failed.json", inputs)
            run_batch(source, tmp_path / "failed", pg_connection)
        else:
            publish(Path(publication).parent / "inputs.json", Path(publication).parent / "results.json", pg_connection)
    assert load_published_results(publication) == before
    assert Path(publication).read_bytes() == manifest_before
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.batches WHERE is_current")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.batches")
        assert cursor.fetchone()[0] == 1


def test_readonly_transaction_rejects_writes(publication, pg_connection):
    with pytest.raises(DatabaseError):
        with transaction(pg_connection, readonly=True) as cursor:
            cursor.execute("DELETE FROM airflow_xva.batches")
    assert len(load_published_results(publication)[1]["trade_pv"]) == 3


def test_concurrent_republication_preserves_one_batch(publication, pg_connection):
    directory = Path(publication).parent
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(publish, directory / "inputs.json", directory / "results.json", pg_connection) for _ in range(2)]
        assert all(future.result() == publication for future in futures)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.batches")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.exposures")
        assert cursor.fetchone()[0] == 52


def test_manifest_contains_connection_id_without_credentials(publication, pg_connection):
    manifest = read_json(publication)
    assert manifest["storage"] == "postgres" and manifest["connection_id"] == pg_connection
    assert "database" not in manifest and "password" not in manifest


def test_output_tables_use_valuation_date_only(publication, pg_connection):
    manifest = read_json(publication)
    with transaction(pg_connection, readonly=True) as cursor:
        for table in OUTPUT_COLUMNS:
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='airflow_xva' AND table_name=%s", (table,))
            columns = {row[0] for row in cursor.fetchall()}
            assert "valuation_date" in columns and "batch_id" not in columns
            if table == "trade_pv":
                assert "currency" not in columns
            cursor.execute(f"SELECT DISTINCT valuation_date FROM airflow_xva.{table}")
            assert [row[0].isoformat() for row in cursor.fetchall()] == [manifest["valuation_date"]]


def test_new_batch_replaces_date_outputs_but_keeps_metadata(publication, tmp_path, pg_connection):
    snapshot = read_json(Path(publication).parent / "inputs.json")
    inputs = snapshot["inputs"]
    inputs["simulation"]["seed"] += 1
    inputs["trades"] = inputs["trades"][:2]
    source = write_json(tmp_path / "replacement.json", inputs)
    replacement = run_batch(source, tmp_path / "replacement", pg_connection)
    assert len(load_published_results(replacement)[1]["trade_pv"]) == 2
    with pytest.raises(ValueError, match="no longer current"):
        load_published_results(publication)
    with transaction(pg_connection, readonly=True) as cursor:
        for table, count in (("batches", 2), ("trade_pv", 2), ("exposures", 26), ("netting_set_cva", 1)):
            cursor.execute(f"SELECT COUNT(*) FROM airflow_xva.{table}")
            assert cursor.fetchone()[0] == count
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.batches WHERE is_current")
        assert cursor.fetchone()[0] == 1


def test_other_valuation_dates_remain_available(publication, tmp_path, pg_connection):
    previous = load_published_results(publication)
    inputs = read_json(Path(publication).parent / "inputs.json")["inputs"]
    inputs["valuation_date"] = "2026-10-11"
    for curve in inputs["credit_curves"]:
        curve["valuation_date"] = "2026-10-11"
    source = write_json(tmp_path / "another_date.json", inputs)
    other = run_batch(source, tmp_path / "other_date", pg_connection)
    assert load_published_results(publication) == previous
    assert load_published_results(other)[0]["inputs"]["valuation_date"] == "2026-10-11"
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT valuation_date, COUNT(*) FROM airflow_xva.trade_pv GROUP BY valuation_date ORDER BY valuation_date")
        assert [(date.isoformat(), count) for date, count in cursor.fetchall()] == [("2026-10-10", 3), ("2026-10-11", 3)]


def make_legacy_schema(connection_id):
    """Recreate the previous batch-keyed layout, with its foreign keys."""
    primary_keys = {"trade_pv": "trade_id", "exposures": "ma_id, exposure_date", "netting_set_cva": "ma_id", "counterparty_xva": "counterparty_code"}
    with transaction(connection_id) as cursor:
        for table, columns in OUTPUT_COLUMNS.items():
            fields = ", ".join(f"t.{column.strip()}" for column in columns.split(","))
            if table == "counterparty_xva":
                fields = "t.counterparty_code, t.currency, t.value_calc AS cva"
            cursor.execute(f"""CREATE TABLE airflow_xva.legacy_{table} AS
                SELECT b.batch_id, {fields} FROM airflow_xva.{table} t
                JOIN airflow_xva.batches b ON b.valuation_date = t.valuation_date""")
            cursor.execute(f"DROP TABLE airflow_xva.{table}")
            cursor.execute(f"ALTER TABLE airflow_xva.legacy_{table} RENAME TO {table}")
            cursor.execute(f"ALTER TABLE airflow_xva.{table} ADD PRIMARY KEY (batch_id, {primary_keys[table]})")
            cursor.execute(f"ALTER TABLE airflow_xva.{table} ADD FOREIGN KEY (batch_id) REFERENCES airflow_xva.batches ON DELETE CASCADE")
        cursor.execute("ALTER TABLE airflow_xva.trade_pv ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR'")
        cursor.execute("ALTER TABLE airflow_xva.batches DROP COLUMN is_current CASCADE")
        cursor.execute("ALTER TABLE airflow_xva.counterparty_xva RENAME TO counterparty_cva")


def test_trade_pv_csv_is_implicitly_eur(publication):
    outputs = read_json(generate_reports(publication))["outputs"]
    header = Path(outputs["trade_pv"]).read_text().splitlines()[0].split(",")
    assert "currency" not in header and "pv" in header
    assert {row["currency"] for row in load_published_results(publication)[1]["trade_pv"]} == {"EUR"}


def test_date_keyed_eur_migration_preserves_pvs(publication, pg_connection):
    previous = load_published_results(publication)
    with transaction(pg_connection) as cursor:
        cursor.execute("ALTER TABLE airflow_xva.trade_pv ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR'")
        ensure_schema(cursor)
        ensure_schema(cursor)
    assert load_published_results(publication) == previous
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='airflow_xva' AND table_name='trade_pv'")
        assert "currency" not in {row[0] for row in cursor.fetchall()}


@pytest.mark.parametrize("legacy", [False, True])
def test_non_eur_migration_is_rejected_without_changes(publication, pg_connection, legacy):
    if legacy:
        make_legacy_schema(pg_connection)
    else:
        with transaction(pg_connection) as cursor:
            cursor.execute("ALTER TABLE airflow_xva.trade_pv ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'EUR'")
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.trade_pv SET currency='USD' WHERE trade_id=(SELECT MIN(trade_id) FROM airflow_xva.trade_pv)")
    with pytest.raises(ValueError, match="not EUR"):
        with transaction(pg_connection) as cursor:
            ensure_schema(cursor)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE currency='USD') FROM airflow_xva.trade_pv")
        assert cursor.fetchone() == (3, 1)


def test_legacy_migration_preserves_archive_and_current_results(publication, pg_connection):
    before = load_published_results(publication)
    make_legacy_schema(pg_connection)
    with transaction(pg_connection) as cursor:
        ensure_schema(cursor)
        ensure_schema(cursor)  # Repeatable after migration.
    assert load_published_results(publication) == before
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva_batch_archive.exposures")
        assert cursor.fetchone()[0] == 52
        cursor.execute("SELECT batch_id FROM airflow_xva_batch_archive.trade_pv LIMIT 1")
        assert cursor.fetchone()[0] == read_json(publication)["batch_id"]


def test_ambiguous_legacy_dates_require_republication(publication, tmp_path, pg_connection):
    inputs = read_json(Path(publication).parent / "inputs.json")["inputs"]
    inputs["simulation"]["seed"] += 1
    source = write_json(tmp_path / "replacement.json", inputs)
    replacement = run_batch(source, tmp_path / "replacement", pg_connection)
    make_legacy_schema(pg_connection)
    with transaction(pg_connection) as cursor:
        ensure_schema(cursor)
    with pytest.raises(ValueError, match="no longer current"):
        load_published_results(replacement)
    directory = Path(replacement).parent
    publish(directory / "inputs.json", directory / "results.json", pg_connection)
    assert len(load_published_results(replacement)[1]["trade_pv"]) == 3
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva_batch_archive.trade_pv")
        assert cursor.fetchone()[0] == 6
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.trade_pv")
        assert cursor.fetchone()[0] == 3


def test_migration_refuses_to_overwrite_archive(publication, pg_connection):
    make_legacy_schema(pg_connection)
    with transaction(pg_connection) as cursor:
        cursor.execute("CREATE SCHEMA airflow_xva_batch_archive; CREATE TABLE airflow_xva_batch_archive.trade_pv (id INTEGER)")
    with pytest.raises(ValueError, match="will not overwrite"):
        with transaction(pg_connection) as cursor:
            ensure_schema(cursor)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.trade_pv")
        assert cursor.fetchone()[0] == 3


def test_partial_legacy_schema_is_rejected(publication, pg_connection):
    make_legacy_schema(pg_connection)
    with transaction(pg_connection) as cursor:
        cursor.execute("ALTER TABLE airflow_xva.counterparty_cva RENAME COLUMN batch_id TO custom_id")
    with pytest.raises(ValueError, match="Mixed legacy"):
        with transaction(pg_connection) as cursor:
            ensure_schema(cursor)


def test_manifest_date_must_match_batch(publication):
    manifest = read_json(publication)
    manifest["valuation_date"] = "2026-10-11"
    write_json(publication, manifest)
    with pytest.raises(ValueError, match="valuation date"):
        load_published_results(publication)


def test_generic_xva_defaults_and_timestamp(publication, pg_connection):
    with transaction(pg_connection) as cursor:
        cursor.execute("SELECT xva_type, value_calc, value_adj, updated_at FROM airflow_xva.counterparty_xva")
        rows = cursor.fetchall()
        assert all(kind == "CVA" and calc == adj and stamp.tzinfo is not None for kind, calc, adj, stamp in rows)
        cursor.execute("""INSERT INTO airflow_xva.counterparty_xva
            (valuation_date, counterparty_code, currency, xva_type, value_calc)
            VALUES ('2026-10-10', '0000001', 'EUR', 'DVA', -123)
            RETURNING value_adj, updated_at""")
        adj, inserted_at = cursor.fetchone()
        assert adj == -123
    with transaction(pg_connection) as cursor:
        cursor.execute("""UPDATE airflow_xva.counterparty_xva SET value_adj=-110
            WHERE xva_type='DVA' RETURNING updated_at""")
        assert cursor.fetchone()[0] > inserted_at
    assert len(load_published_results(publication)[1]["counterparty_cva"]) == 2


def test_manual_adjustments_in_reports_and_reset_on_rerun(publication, pg_connection):
    calculated = load_published_results(publication)[1]["counterparty_cva"]
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.counterparty_xva SET value_adj=value_calc+10 WHERE xva_type='CVA'")
    report = read_json(generate_reports(publication))
    assert report["total_cva"] == pytest.approx(sum(r["cva"] for r in calculated))
    assert report["total_cva_adj"] == pytest.approx(report["total_cva"] + 20)
    with open(report["outputs"]["counterparty_summary"], newline="") as stream:
        rows = list(csv.DictReader(stream))
    for row in rows:
        assert float(row["cva_adj"]) == pytest.approx(float(row["cva"]) + 10)
        assert float(row["cva_adjusted_pv"]) == pytest.approx(float(row["pv_t0"]) - float(row["cva_adj"]))
    directory = Path(publication).parent
    publish(directory / "inputs.json", directory / "results.json", pg_connection)
    assert load_published_results(publication)[1]["counterparty_cva"] == calculated


def test_cva_republication_preserves_other_xva_types(publication, pg_connection):
    with transaction(pg_connection) as cursor:
        cursor.execute("""INSERT INTO airflow_xva.counterparty_xva
            (valuation_date, counterparty_code, currency, xva_type, value_calc, value_adj)
            VALUES ('2026-10-10', '0000001', 'EUR', 'DVA', -123, -110) RETURNING updated_at""")
        inserted_at = cursor.fetchone()[0]
    directory = Path(publication).parent
    publish(directory / "inputs.json", directory / "results.json", pg_connection)
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT value_calc, value_adj, updated_at FROM airflow_xva.counterparty_xva WHERE xva_type='DVA'")
        assert cursor.fetchone() == (-123, -110, inserted_at)


def test_date_keyed_cva_migration_preserves_values(publication, pg_connection):
    before = load_published_results(publication)
    with transaction(pg_connection) as cursor:
        cursor.execute("""CREATE TABLE airflow_xva.old_counterparty_cva AS
            SELECT valuation_date, counterparty_code, currency, value_calc AS cva
            FROM airflow_xva.counterparty_xva;
            DROP TABLE airflow_xva.counterparty_xva;
            ALTER TABLE airflow_xva.old_counterparty_cva RENAME TO counterparty_cva;
            ALTER TABLE airflow_xva.counterparty_cva ADD PRIMARY KEY (valuation_date, counterparty_code);
            ALTER TABLE airflow_xva.counterparty_cva ADD CONSTRAINT counterparty_cva_cva_check CHECK (cva>=0);
        """)
        ensure_schema(cursor)
        ensure_schema(cursor)
    assert load_published_results(publication) == before


def test_rename_generic_table_preserves_adjustments_and_timestamps(publication, pg_connection):
    with transaction(pg_connection) as cursor:
        cursor.execute("UPDATE airflow_xva.counterparty_xva SET value_adj=value_calc+7")
        cursor.execute("SELECT counterparty_code, value_calc, value_adj, updated_at FROM airflow_xva.counterparty_xva ORDER BY counterparty_code")
        previous = cursor.fetchall()
        cursor.execute("ALTER TABLE airflow_xva.counterparty_xva RENAME TO counterparty_cva")
        ensure_schema(cursor)
        ensure_schema(cursor)
        cursor.execute("SELECT counterparty_code, value_calc, value_adj, updated_at FROM airflow_xva.counterparty_xva ORDER BY counterparty_code")
        assert cursor.fetchall() == previous
        cursor.execute("SELECT to_regclass('airflow_xva.counterparty_cva')")
        assert cursor.fetchone()[0] is None
    assert all(row["cva_adj"] == row["cva"] + 7 for row in load_published_results(publication)[1]["counterparty_cva"])


def test_rename_refuses_conflicting_tables(publication, pg_connection):
    with transaction(pg_connection) as cursor:
        cursor.execute("CREATE TABLE airflow_xva.counterparty_cva (id INTEGER)")
    with pytest.raises(ValueError, match="Both counterparty_cva and counterparty_xva exist"):
        with transaction(pg_connection) as cursor:
            ensure_schema(cursor)
    assert len(load_published_results(publication)[1]["counterparty_cva"]) == 2


@pytest.mark.parametrize("column", ["value_calc", "value_adj"])
@pytest.mark.parametrize("value", [float("inf"), float("nan")])
def test_nonfinite_generic_xva_rejected(publication, pg_connection, column, value):
    with pytest.raises(DatabaseError):
        with transaction(pg_connection) as cursor:
            cursor.execute(f"UPDATE airflow_xva.counterparty_xva SET {column}=%s", (value,))
