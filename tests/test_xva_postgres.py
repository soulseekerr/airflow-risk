from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from airflow.providers.postgres.hooks.postgres import USE_PSYCOPG3

from risk_pipeline.xva.batch import publish, read_json, run_batch, write_json
from risk_pipeline.xva.postgres import transaction
from risk_pipeline.xva.reports import load_published_results

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


def test_failed_republication_rolls_back_previous_batch(publication, pg_connection):
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
        publish(Path(publication).parent / "inputs.json", Path(publication).parent / "results.json", pg_connection)
    assert load_published_results(publication) == before
    assert Path(publication).read_bytes() == manifest_before


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
