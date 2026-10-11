"""Database tests use a disposable PostgreSQL database, never risk_postgres."""

import os

import pytest

from risk_pipeline.xva.postgres import transaction


@pytest.fixture
def pg_connection():
    """Provide an isolated test database connection and clean only the disposable XVA schemas."""
    connection_id = os.environ.get("XVA_TEST_POSTGRES_CONN_ID")
    if not connection_id:
        pytest.skip("Set XVA_TEST_POSTGRES_CONN_ID for PostgreSQL integration tests")
    if not connection_id.startswith("xva_test_"):
        pytest.fail("Integration connection ID must start with xva_test_")
    with transaction(connection_id) as cursor:
        cursor.execute("SELECT current_database()")
        if not cursor.fetchone()[0].endswith("_test"):
            pytest.fail("Integration database name must end in _test")
        cursor.execute("DROP SCHEMA IF EXISTS airflow_xva CASCADE")
        cursor.execute("DROP SCHEMA IF EXISTS airflow_xva_batch_archive CASCADE")
    try:
        yield connection_id
    finally:
        with transaction(connection_id) as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS airflow_xva CASCADE")
            cursor.execute("DROP SCHEMA IF EXISTS airflow_xva_batch_archive CASCADE")
