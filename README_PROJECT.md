# Airflow Risk: project overview

This project demonstrates a small trade risk pipeline using Apache Airflow's
TaskFlow API. It extracts sample trades, calculates a summary, passes results
between tasks through XCom, retries a simulated failure, and writes the summary
to PostgreSQL.

For installation notes, see [README.md](README.md). This document explains the
project's behavior and how its parts fit together.

## Repository layout

| Path | Purpose |
| --- | --- |
| `dags/xcom_demo.py` | Defines the `xcom_demo` DAG and its tasks. |
| `src/risk_pipeline/calculations.py` | Contains the reusable `summarize_trades` function. |
| `tests/test_risk_calculations.py` | Tests totals, empty input, and threshold boundaries. |
| `tests/test_dags.py` | Checks DAG imports and registration. |
| `requirements.txt` | Pins Airflow 3.3.2 with PostgreSQL, Slack, and Celery extras; includes test tools. |
| `pyproject.toml` | Defines the Python package and its `src` layout. |
| `.github/workflows/tests.yml` | Runs tests with a minimum coverage requirement of 90%. |

The package declares Python 3.10 or newer. Airflow must also support the chosen
Python version; use the matching Airflow constraints file when installing it.

## What the DAG does

The DAG runs daily in the `Europe/London` timezone, with a start date of
1 October 2026. `catchup=False` prevents automatic creation of all historical
scheduled runs.

1. `show_run_context` logs the logical date and data interval. It is an
   independent task with no explicit dependencies on the other tasks.
2. `get_run_date` takes the date from the start of the data interval.
3. `extract` returns three hardcoded sample trades.
4. `summarize` calls `summarize_trades`, using the Airflow variable
   `mtm_threshold` or a default of `50000`.
5. `flaky_calculation` deliberately fails its first attempt. It allows two
   retries with a ten-second delay and, on a later attempt, multiplies total MTM
   by `1.01`.
6. `save_to_database` writes the run date and the original summary to PostgreSQL.
7. `report` logs the run date, trade count, adjusted total MTM, and large trade
   count.

TaskFlow creates dependencies from the task outputs passed as arguments.
`summarize` uses `multiple_outputs=True`, so downstream tasks can consume each
summary field separately. The database task and retry task both consume the
summary; the database write does not wait for the adjusted calculation. The
report waits for that calculation but does not depend on the database write.

## Risk calculation example

MTM means mark-to-market value. The calculation adds signed MTM values and counts
trades whose absolute MTM is greater than or equal to the threshold. A negative
trade can therefore count as a large trade.

```python
from risk_pipeline.calculations import summarize_trades

trades = [
    {"trade_id": "T001", "mtm": 100_000},
    {"trade_id": "T002", "mtm": -25_000},
    {"trade_id": "T003", "mtm": 70_000},
]

summary = summarize_trades(trades, threshold=50_000)
# {"trade_count": 3, "total_mtm": 145000, "large_trade_count": 2}
```

For these trades, the report's adjusted total is `146450`, while the database
stores the original total of `145000`. The adjustment demonstrates retry
behavior rather than a financial risk model.

## PostgreSQL requirements

To execute the complete DAG, configure an Airflow connection named
`risk_postgres` pointing to the intended demo database. Keep credentials in
Airflow's connection configuration or a supported secrets backend.

The database task currently creates the schema `airflow_demo`, but creates and
inserts into `airflow_risk.risk_summary`. Before running it, create the target
schema in your demo database:

```sql
CREATE SCHEMA IF NOT EXISTS airflow_risk;
```

The connection's database role needs permission to create the schemas and
table, and to insert rows. Each successful database task execution inserts a
new row; there is no uniqueness rule for `run_date` or upsert logic, so reruns
can produce duplicate summaries.

PostgreSQL is the destination for the summary, separate from Airflow's metadata
database. Local Airflow development can use SQLite for metadata. The current
DAG does not use Slack or a Celery broker despite those extras being listed in
the dependencies.

## Tests and local validation

From the repository root, with the development environment activated, run:

```bash
python -m pytest -v
python -m pytest --cov=risk_pipeline --cov-report=term-missing --cov-fail-under=90 -v
```

The original demo has nine test cases, including parametrized threshold cases.
They validate the calculation function and DAG loading without checking the
demo's PostgreSQL writes or retries. The suite also includes tests for the
three-DAG XVA workflow; see [README_XVA.md](README_XVA.md).

To load this repository's DAG in a local Airflow instance:

```bash
export AIRFLOW_HOME="/workspace/.airflow-risk"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
airflow standalone
```

Run these commands from the repository root with the virtual environment
activated so Airflow can also launch its subprocesses. `standalone` is intended
for development. In another activated terminal with the same environment
variables, check `airflow dags list` and `airflow dags list-import-errors`.
Prepare the PostgreSQL connection and target schema before triggering
`xcom_demo`.

## Scope

This is a learning demo with static input data. It illustrates scheduling,
data intervals, XCom, configurable thresholds, retries, and database hooks.
Production use would need real extraction, input validation, duplicate-safe
database writes, and operational monitoring.
