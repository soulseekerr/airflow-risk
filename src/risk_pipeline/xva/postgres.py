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
 model_version TEXT NOT NULL, currency VARCHAR(3) NOT NULL, input_snapshot JSONB NOT NULL,
 is_current BOOLEAN NOT NULL DEFAULT FALSE
);
ALTER TABLE airflow_xva.batches ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT FALSE;
CREATE UNIQUE INDEX IF NOT EXISTS batches_current_date ON airflow_xva.batches (valuation_date) WHERE is_current;
CREATE TABLE IF NOT EXISTS airflow_xva.trade_pv (
 valuation_date DATE NOT NULL,
 trade_id TEXT NOT NULL, ma_id TEXT NOT NULL, counterparty_code VARCHAR(7) NOT NULL,
 pv DOUBLE PRECISION NOT NULL,
 PRIMARY KEY (valuation_date, trade_id)
);
CREATE TABLE IF NOT EXISTS airflow_xva.exposures (
 valuation_date DATE NOT NULL,
 ma_id TEXT NOT NULL, exposure_date DATE NOT NULL, counterparty_code VARCHAR(7) NOT NULL,
 currency VARCHAR(3) NOT NULL,
 expected_positive_exposure DOUBLE PRECISION NOT NULL CHECK (expected_positive_exposure >= 0),
 discount_factor DOUBLE PRECISION NOT NULL CHECK (discount_factor > 0),
 survival_probability DOUBLE PRECISION NOT NULL CHECK (survival_probability > 0 AND survival_probability <= 1),
 cva_increment DOUBLE PRECISION NOT NULL CHECK (cva_increment >= 0),
 PRIMARY KEY (valuation_date, ma_id, exposure_date)
);
CREATE TABLE IF NOT EXISTS airflow_xva.netting_set_cva (
 valuation_date DATE NOT NULL,
 ma_id TEXT NOT NULL, counterparty_code VARCHAR(7) NOT NULL,
 currency VARCHAR(3) NOT NULL, cva DOUBLE PRECISION NOT NULL CHECK (cva >= 0),
 PRIMARY KEY (valuation_date, ma_id)
);
CREATE TABLE IF NOT EXISTS airflow_xva.counterparty_cva (
 valuation_date DATE NOT NULL,
 counterparty_code VARCHAR(7) NOT NULL, currency VARCHAR(3) NOT NULL,
 cva DOUBLE PRECISION NOT NULL CHECK (cva >= 0), PRIMARY KEY (valuation_date, counterparty_code)
);
"""


OUTPUT_COLUMNS = {
    "trade_pv": "trade_id, ma_id, counterparty_code, pv",
    "exposures": "ma_id, exposure_date, counterparty_code, currency, expected_positive_exposure, discount_factor, survival_probability, cva_increment",
    "netting_set_cva": "ma_id, counterparty_code, currency, cva",
    "counterparty_cva": "counterparty_code, currency, cva",
}


def ensure_schema(cursor):
    """Archive the old tables and migrate dates with an unambiguous batch."""
    cursor.execute("""SELECT 1 FROM information_schema.columns
                      WHERE table_schema = 'airflow_xva' AND table_name = 'trade_pv'
                        AND column_name = 'currency'""")
    if cursor.fetchone() is not None:
        cursor.execute("SELECT 1 FROM airflow_xva.trade_pv WHERE currency IS DISTINCT FROM 'EUR' LIMIT 1")
        if cursor.fetchone() is not None:
            raise ValueError("Existing trade PV is not EUR; convert or archive it before migration")
    cursor.execute("""
        SELECT table_name FROM information_schema.columns
        WHERE table_schema = 'airflow_xva' AND column_name = 'batch_id'
          AND table_name IN ('trade_pv', 'exposures', 'netting_set_cva', 'counterparty_cva')
    """)
    legacy_tables = {row[0] for row in cursor.fetchall()}
    if legacy_tables:
        if legacy_tables != set(OUTPUT_COLUMNS):
            raise ValueError("Mixed legacy XVA output schema; restore a consistent schema before publication")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS airflow_xva_batch_archive")
        for table in OUTPUT_COLUMNS:
            cursor.execute("SELECT to_regclass(%s)", (f"airflow_xva_batch_archive.{table}",))
            if cursor.fetchone()[0] is not None:
                raise ValueError("Legacy XVA archive already exists; migration will not overwrite it")
        for table in OUTPUT_COLUMNS:
            cursor.execute(f"ALTER TABLE airflow_xva.{table} SET SCHEMA airflow_xva_batch_archive")
    cursor.execute(SCHEMA)
    cursor.execute("ALTER TABLE airflow_xva.trade_pv DROP COLUMN IF EXISTS currency")
    if legacy_tables:
        # Multiple old batches for one date have no recorded chronology. Keep
        # them archived; a deliberate republication selects the current batch.
        cursor.execute("""
            UPDATE airflow_xva.batches b SET is_current = TRUE
            WHERE NOT EXISTS (SELECT 1 FROM airflow_xva.batches other
                              WHERE other.valuation_date = b.valuation_date AND other.batch_id <> b.batch_id)
        """)
        for table, columns in OUTPUT_COLUMNS.items():
            qualified = ", ".join(f"t.{column.strip()}" for column in columns.split(","))
            cursor.execute(f"""
                INSERT INTO airflow_xva.{table} (valuation_date, {columns})
                SELECT b.valuation_date, {qualified}
                FROM airflow_xva_batch_archive.{table} t
                JOIN airflow_xva.batches b ON b.batch_id = t.batch_id
                WHERE b.is_current
            """)
