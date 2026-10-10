"""PostgreSQL transactions using the same Airflow connection as xcom_demo."""

from contextlib import closing, contextmanager

from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_CONNECTION_ID = "risk_postgres"


@contextmanager
def transaction(connection_id, readonly=False):
    with closing(PostgresHook(postgres_conn_id=connection_id).get_conn()) as connection:
        connection.autocommit = False
        with closing(connection.cursor()) as cursor:
            try:
                if readonly:
                    cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                yield cursor
                connection.commit()
            except BaseException:
                connection.rollback()
                raise


SCHEMA = """
CREATE SCHEMA IF NOT EXISTS airflow_xva;
CREATE TABLE IF NOT EXISTS airflow_xva.batches (
 batch_id TEXT PRIMARY KEY, valuation_date DATE NOT NULL,
 model_version TEXT NOT NULL, currency VARCHAR(3) NOT NULL, input_snapshot JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS airflow_xva.trade_pv (
 batch_id TEXT REFERENCES airflow_xva.batches ON DELETE CASCADE,
 trade_id TEXT NOT NULL, ma_id TEXT NOT NULL, counterparty_code VARCHAR(7) NOT NULL,
 currency VARCHAR(3) NOT NULL, pv DOUBLE PRECISION NOT NULL,
 PRIMARY KEY (batch_id, trade_id)
);
CREATE TABLE IF NOT EXISTS airflow_xva.exposures (
 batch_id TEXT REFERENCES airflow_xva.batches ON DELETE CASCADE,
 ma_id TEXT NOT NULL, exposure_date DATE NOT NULL, counterparty_code VARCHAR(7) NOT NULL,
 currency VARCHAR(3) NOT NULL,
 expected_positive_exposure DOUBLE PRECISION NOT NULL CHECK (expected_positive_exposure >= 0),
 discount_factor DOUBLE PRECISION NOT NULL CHECK (discount_factor > 0),
 survival_probability DOUBLE PRECISION NOT NULL CHECK (survival_probability > 0 AND survival_probability <= 1),
 cva_increment DOUBLE PRECISION NOT NULL CHECK (cva_increment >= 0),
 PRIMARY KEY (batch_id, ma_id, exposure_date)
);
CREATE TABLE IF NOT EXISTS airflow_xva.netting_set_cva (
 batch_id TEXT REFERENCES airflow_xva.batches ON DELETE CASCADE,
 ma_id TEXT NOT NULL, counterparty_code VARCHAR(7) NOT NULL,
 currency VARCHAR(3) NOT NULL, cva DOUBLE PRECISION NOT NULL CHECK (cva >= 0),
 PRIMARY KEY (batch_id, ma_id)
);
CREATE TABLE IF NOT EXISTS airflow_xva.counterparty_cva (
 batch_id TEXT REFERENCES airflow_xva.batches ON DELETE CASCADE,
 counterparty_code VARCHAR(7) NOT NULL, currency VARCHAR(3) NOT NULL,
 cva DOUBLE PRECISION NOT NULL CHECK (cva >= 0), PRIMARY KEY (batch_id, counterparty_code)
);
"""
