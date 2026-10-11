import pendulum
from airflow.sdk import dag, task, Variable
from datetime import timedelta
from airflow.sdk import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.timetables.interval import CronDataIntervalTimetable
import risk_pipeline.calculations as risk_calc

from airflow.sdk import get_current_context


@task
def show_run_context():
    """Log the Airflow logical date and data interval for the demo run."""
    context = get_current_context()

    print(
        "Logical date:",
        context["logical_date"],
    )

    print(
        "Data interval start:",
        context["data_interval_start"],
    )

    print(
        "Data interval end:",
        context["data_interval_end"],
    )

@dag(
    dag_id="xcom_demo",
    schedule=CronDataIntervalTimetable(
        "@daily",
        timezone="Europe/London",
    ),
    start_date=pendulum.datetime(
        2026, 10, 1,
        tz="Europe/London",
    ),
    catchup=False,
)
def xcom_demo():

    """Build the daily XCom demonstration DAG with summaries, retry behavior, and database output."""
    @task
    def get_run_date():
        """Return the data interval start date as the demo valuation date in ISO format."""
        context = get_current_context()

        run_date = context["data_interval_start"].date()

        print(f"Run date: {run_date}")

        return run_date.isoformat()

    @task
    def extract():
        """Return three sample trades to demonstrate passing structured values through XCom."""
        trades = [
            {"trade_id": "T001", "mtm": 100_000},
            {"trade_id": "T002", "mtm": -25_000},
            {"trade_id": "T003", "mtm": 70_000},
        ]

        return trades

    @task(multiple_outputs=True)
    def summarize(trades):
        # AIRFLOW VARIABLE
        """Read the MTM threshold from an Airflow Variable and return the shared trade summary."""
        THRESHOLD= int(
            Variable.get(
                "mtm_threshold",
                default="50000"
            )
        )

        return risk_calc.summarize_trades(
            trades,
            THRESHOLD
        )

        large_trades = [
            trade
            for trade in trades
            if abs(trade["mtm"]) >= threshold
        ]

        total_mtm = sum(
            trade["mtm"]
            for trade in trades
        )

        return {
            "trade_count": len(trades),
            "total_mtm": total_mtm,
            "large_trade_count": len(large_trades),
        }

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def flaky_calculation(total_mtm):
        """Simulate a first-attempt failure, then return a one-percent MTM adjustment on retry."""
        context = get_current_context()
        ti = context["ti"]

        print(f"Attempt number: {ti.try_number}")

        if ti.try_number == 1:
            print("Simulating temporary failure...")
            raise RuntimeError("Temporary market data problem")

        print("Calculation successful")

        return total_mtm * 1.01

    @task
    def save_to_database(run_date, trade_count, total_mtm, large_trade_count):
        """Create the demo storage objects if needed and insert the daily summary via risk_postgres."""
        hook = PostgresHook(
            postgres_conn_id="risk_postgres"
        )

        hook.run("""
            CREATE SCHEMA IF NOT EXISTS airflow_demo
        """)

        hook.run("""
            CREATE TABLE IF NOT EXISTS airflow_risk.risk_summary (
                id SERIAL PRIMARY KEY,
                trade_count INTEGER,
                total_mtm NUMERIC,
                large_trade_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_date DATE
            )
        """)

        hook.run(
            """
            INSERT INTO airflow_risk.risk_summary (
                run_date,
                trade_count,
                total_mtm,
                large_trade_count
            )
            VALUES (%s, %s, %s, %s)
            """,
            parameters=(
                run_date,
                trade_count,
                total_mtm,
                large_trade_count,
            ),
        )

    @task
    def report(
        run_date,
        trade_count, 
        total_mtm,
        large_trade_count,
    ):
        """Print the demo run date, trade counts, and adjusted total MTM."""
        print(f"Run date: {run_date}")
        print(f"Number of trades: {trade_count}")
        print(f"Total MTM: {total_mtm:,}")
        print(f"Large trades: {large_trade_count}")

    context_task = show_run_context()

    run_date = get_run_date()

    trades = extract()

    summary = summarize(trades)

    adjusted_mtm = flaky_calculation(
        summary["total_mtm"]
    )

    save_to_database(
        run_date,
        summary["trade_count"],
        summary["total_mtm"],
        summary["large_trade_count"],
    )

    report(
        run_date,
        summary["trade_count"],
        adjusted_mtm,
        summary["large_trade_count"],
    )


xcom_demo()