# XVA batch: PV, positive exposure, and CVA

Three DAGs form a batch workflow alongside `xcom_demo`:
`xva_inputs` → `xva_calculations` → `xva_reports`. They read counterparty,
credit curve, MA, CSA, trade, and market data; calculate trade PV at t0 and
netting-set expected positive exposure; store unilateral CVA; and generate
reports for the exact published batch.

This first version is an educational model, with synthetic interest rate swaps
in EUR. Results are stored in PostgreSQL through the same
`risk_postgres` Airflow connection used by `xcom_demo`.

## PostgreSQL connection

Configure the Airflow connection `risk_postgres` with your PostgreSQL host, port,
database, user, and password through Airflow's connection configuration or a
supported secrets backend. This is the same connection ID used by the original
demo. The database role needs permission to create the `airflow_xva` schema and
its tables, and to select, insert, and delete XVA results. TLS options can be
configured through the PostgreSQL connection extras for your server.

XVA creates its own `airflow_xva` schema; it does not write to or alter the
original demo's `airflow_risk.risk_summary` table. Existing SQLite files are
not migrated automatically. Republish a frozen batch to PostgreSQL before
requesting reports for it; legacy SQLite publication manifests are rejected.

## Run without Airflow

From the repository root, using the installed development environment:

```bash
source .venv/bin/activate
XVA_OUTPUT_ROOT="${XVA_OUTPUT_ROOT:-${AIRFLOW_HOME:-$HOME/airflow}/xva}"
python -m risk_pipeline.xva.batch \
  --inputs examples/xva/inputs.json \
  --output "$XVA_OUTPUT_ROOT"
```

The command validates and freezes inputs, generates daily scenarios, calculates
the results, reconciles CVA totals, and publishes to PostgreSQL. It prints a manifest
with the batch ID, connection ID, trade count, netting-set count, and total
CVA. All results are in EUR. Generate reports separately
using the `published.json` path for that batch:

```bash
python -m risk_pipeline.xva.reports \
  --publication "$XVA_OUTPUT_ROOT/<batch_id>/published.json"
```

Replace `<batch_id>` with the ID printed by the batch command. Reporting reads
only published database results and can be repeated without simulation or
pricing. The CLI also resolves the Airflow connection; it does not take a raw
database password. To use another configured connection, add
`--connection-id <connection_id>` to the batch command. The Airflow DAG defaults
to `risk_postgres`, and reports use the connection ID recorded at publication.
Publication manifests never contain connection URLs or credentials.
For CLI execution outside an Airflow 3 task, make the connection available through
`AIRFLOW_CONN_RISK_POSTGRES` (or the corresponding name for another connection ID)
or a secrets backend accessible to that process. During DAG execution, Airflow
resolves metadata-stored connections through its execution API.

## Run the three-DAG chain

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `xva_inputs` | Daily, initially paused | Validate/freeze inputs, trigger calculations |
| `xva_calculations` | Triggered only | Validate batch identity, simulate, calculate, publish, trigger reports |
| `xva_reports` | Triggered only | Validate published batch, generate reports |

```text
xva_inputs: validate_and_freeze → trigger_calculations
                                         ↓
xva_calculations: validate_batch → generate_market_scenarios
                                         ↓
                    calculate_pv_exposure_cva → reconcile_and_publish
                                                      ↓
                                                trigger_reports
                                                      ↓
xva_reports: validate_published_batch → create_reports
```

The input snapshot is also passed directly to the calculation and publication
tasks. Cross-DAG triggers pass the batch ID and snapshot/publication path in
`dag_run.conf`. Each receiving DAG validates that identity. XCom carries paths,
rather than scenario arrays or exposures.
The files must be visible at the same paths to every task. This works with the
local standalone environment; distributed workers would need shared storage.

With Airflow standalone running and the three DAGs loaded, make sure the two
downstream DAGs are unpaused:

```bash
airflow dags unpause xva_calculations
airflow dags unpause xva_reports
```

Run the supplied example without enabling the daily input schedule:

```bash
airflow dags test xva_inputs
```

This executes the input DAG and triggers a real calculation run. The scheduler
executes calculations and then reports. The input DAG's successful completion
means the calculation run was requested; it does not wait for calculations or
reports to finish. The calculation DAG likewise does not wait for reports.

Artifacts default to `$AIRFLOW_HOME/xva`, or `~/airflow/xva` when `AIRFLOW_HOME`
is unset. Set `XVA_OUTPUT_ROOT` in the Airflow task processes to use another
writable directory. A run's `output_root` configuration overrides that setting.
Relative output directories are resolved against Airflow home; `~` is expanded.
Use the same shared artifact directory across all workers.

The checkout is inferred from the DAG file location. If you copy the DAG files
into a separate Airflow DAG directory, set `XVA_PROJECT_ROOT` to the checkout
containing `examples/xva/inputs.json` and `data/xva`. Relative `input_path` and
`XVA_INPUT_ROOT` paths are resolved against that checkout. For example, use
`{"input_path":"examples/xva/inputs.json"}` as a manual run's configuration.

For daily operation, supply files at `data/xva/YYYY-MM-DD.json` in the repository,
or set `XVA_INPUT_ROOT` in the Airflow processes to another input directory.
An explicit daily interval timetable in `Europe/London` is used. A run after
midnight processes the preceding calendar day's valuation date (its interval
start), and requires the file's valuation date to match. Missing or stale data fails
the input DAG and prevents the calculation trigger. Once the feed is ready:

```bash
airflow dags unpause xva_inputs
```

Manual input runs default to the sample file unless `input_path` is supplied.
When the input schedule is enabled, `airflow dags trigger xva_inputs --conf ...`
can also be used. Only `xva_inputs` has a schedule; calculations and reports
use `schedule=None` and have `max_active_runs=1`.

Triggers require successful upstream tasks and fail if the target DAG is paused.
Their run IDs include a PostgreSQL prefix and the batch ID, so old SQLite runs
do not suppress the first PostgreSQL run. If that run ID already exists, the trigger
is skipped to avoid duplicate downstream runs. This does **not** retry a failed
existing run: clear its failed tasks or create a new manual run with the same
batch configuration when deliberately recovering a batch.

To rerun calculations directly, pass `batch_id` and `snapshot_path` to
`xva_calculations`. To rerun reports without recalculation, pass `batch_id` and
`publication_path` to `xva_reports`, for example:

```bash
airflow dags trigger xva_reports --conf \
  '{"batch_id":"<batch_id>","publication_path":"/absolute/path/to/xva/<batch_id>/published.json"}'
```

Input and calculation errors fail immediately; publication allows two retries
for storage failures. The original `xva_daily_batch` DAG has been replaced by
these three DAGs.

## Input contract

[examples/xva/inputs.json](examples/xva/inputs.json) is a complete example with
three trades, two counterparties, two MAs, one collateralized MA, and one
uncollateralized MA. Counterparty codes are strings so leading zeros survive.

| Entity | Required fields |
| --- | --- |
| Batch | `valuation_date`, `reporting_currency` |
| Counterparty | `name`, seven-character `code`, `recovery_rate`, `credit_curve_id` |
| Credit curve | `curve_id`, `valuation_date`, dated `survival_probability` points |
| MA | `ma_id`, `counterparty_code` |
| CSA | `csa_id`, `ma_id`, `currency`, `margin_frequency_days`, `settlement_lag_days`, `post_collateral`, `receive_collateral`, `posting_threshold`, `receiving_threshold`, `minimum_transfer_amount`, `initial_balance` |
| Trade | `trade_id`, `trade_type`, `counterparty_code`, `ma_id`, `csa_id`, `currency`, `notional`, `direction`, `fixed_rate`, `start_date`, `maturity_date` |
| Market | `flat_rate`, `rate_volatility` |
| Simulation | `paths`, `seed`, `exposure_frequency_days` |

Rates, volatility, recovery, and survival probabilities are decimals, e.g.
`0.04` means 4%. Monetary amounts use currency units. `rate_volatility` is the
annual absolute rate volatility, e.g. `0.01` means one percentage point.
All dates use ISO `YYYY-MM-DD` format.

`reporting_currency` must be `EUR`. Trade and CSA currencies must also be
EUR; this model does not yet support FX conversion. Trade PV is always in
EUR, so `trade_pv` and `trade_pv.csv` omit the currency column.

The current trade type is `interest_rate_swap`, with `pay_fixed` or
`receive_fixed` direction. Only swaps that have already started and mature
after the valuation date are accepted. A trade's counterparty and CSA must
match its MA. Each MA defines a netting set and has at most one CSA; an
uncollateralized MA uses `csa_id: null` on its trades.

Credit curves supply survival probabilities, not CDS spreads. They must have
the batch valuation date, increasing dates, positive nonincreasing survival,
and coverage through the latest trade maturity. Survival at t0 is 1, and
interpolation is log-linear. Recovery is a counterparty-level constant between
0 and 1. FX conversion, credit curve calibration, and hazard-rate inputs are
outside this version.

## Calculation conventions

### PV and scenarios

PV is positive for an asset from our perspective. Swaps use a continuous-annuity
approximation:

```text
pay-fixed PV = notional × (flat rate − fixed rate) × annuity(remaining years)
annuity(T) = (1 − exp(−rate × T)) / rate
```

The zero-rate limit is `T`; receive-fixed PV reverses the sign. ACT/365 is used.
There are no explicit coupon schedules, accrued interest, floating resets,
multi-curve discounting, or principal exchanges in this approximation.

All trades share the same daily rate paths, generated using additive Gaussian
increments with zero drift and the recorded random seed. Negative rates are
allowed. Each future valuation treats the simulated rate as a flat curve for
the remaining maturity. This is not a calibrated arbitrage-free interest rate
model; CVA is illustrative, not a production valuation.

### Netting and collateral

Signed trade values are summed within each MA **before** taking the positive
part. Different MAs are never netted, even for the same counterparty.

```text
positive exposure = max(net trade value − signed collateral balance, 0)
```

A positive collateral balance means collateral held; a negative balance means
collateral posted. Posted collateral therefore remains part of our exposure.
At t0 the supplied `initial_balance` is used without a new margin call.

Subsequent calls happen on calendar-day multiples of `margin_frequency_days`
from the valuation date. Calls target the net value less the receiving threshold
or plus the posting threshold, subject to the permitted directions. Pending
transfers are counted when determining a new call so they are not duplicated.
Transfers below the minimum transfer amount are ignored; transfers at the
minimum are permitted. Settlement happens after the configured calendar-day lag.

Collateral can be returned when exposure falls or reverses. For simplicity,
all collateral is assumed returned immediately at the netting set's final trade
maturity, overriding frequency and lag at that point. There is no collateral
interest, haircut, dispute, holiday calendar, independent amount, or default
closeout/MPOR model.

### Expected exposure and CVA

Expected positive exposure is the mean of the positive exposures across all
paths at each reporting date. Reporting dates include t0, regular calendar-day
intervals, each trade maturity, and the final horizon. Margining still happens
on the full daily grid between reporting dates.

```text
CVA = (1 − recovery) × sum(mean discounted EE over interval × interval default probability)
```

The integration uses the trapezoidal average of discounted expected exposures
at interval endpoints and the decrease in survival probability. Discounting
uses the initial deterministic flat rate. Exposure and default are assumed
independent: no wrong-way risk. A finer exposure reporting grid improves the
integration resolution. CVA is stored as a nonnegative loss amount; a
CVA-adjusted asset valuation would subtract it from the clean PV. DVA, FVA,
MVA, PFE, and sensitivities are outside the current scope.

## Results and reruns

Artifacts are stored beneath `<output>/<batch_id>/`:

| Artifact | Contents |
| --- | --- |
| `inputs.json` | Validated input snapshot, model version, and batch ID |
| `scenarios.json` | Shared daily rate paths |
| `results.json` | Trade PV, MA exposure profiles, MA CVA, and counterparty CVA |
| `published.json` | Written after successful reconciliation and database publication |

Reports are stored at `<output>/<batch_id>/reports/v1/`:

| Report | Contents |
| --- | --- |
| `counterparty_summary.csv` | Counterparty name/code, trade count, signed PV t0, CVA, and PV minus CVA |
| `trade_pv.csv` | Trade-level PV and MA allocation |
| `exposures.csv` | Dated expected positive exposure profiles and CVA increments by MA |
| `netting_set_cva.csv` | CVA by MA/netting set |
| `summary.md` | Readable totals and counterparty summary |
| `report_manifest.json` | Batch ID, totals, report version, and output paths; written last |

Every CSV row includes the batch ID and valuation date. Import counterparty codes
as text in spreadsheets to retain leading zeros. Reporting uses a PostgreSQL
repeatable-read, read-only transaction,
checks the requested batch and reconciliation, and does not change results or
recalculate. Repeated reporting replaces the same deterministic report files.

The `airflow_xva` PostgreSQL schema contains `batches`, `trade_pv`, `exposures`,
`netting_set_cva`, and `counterparty_cva` tables. **Output tables contain
`valuation_date`, not `batch_id`.** Their primary keys are:

| Table | Primary key |
| --- | --- |
| `trade_pv` | `valuation_date`, `trade_id` |
| `exposures` | `valuation_date`, `ma_id`, `exposure_date` |
| `netting_set_cva` | `valuation_date`, `ma_id` |
| `counterparty_cva` | `valuation_date`, `counterparty_code` |

`valuation_date` is the batch's as-of date. `exposure_date` is a future date on
the exposure profile; both are retained on exposure rows. Query results directly
by valuation date, for example:

```sql
SELECT * FROM airflow_xva.trade_pv WHERE valuation_date = DATE '2026-10-10';
SELECT * FROM airflow_xva.counterparty_cva WHERE valuation_date = DATE '2026-10-10';
```

`batches` retains `batch_id`, `valuation_date`, frozen inputs, and model metadata
for every successful batch. Its `is_current` flag identifies which batch currently
owns the published outputs for a date; a unique partial index permits at most
one current batch per valuation date.

Dates are stored as PostgreSQL `DATE`, the frozen inputs as `JSONB`, and
counterparty codes as strings.
Exposure rows retain the
discount factor, survival probability, and CVA increment for reconciliation.
The result database is separate from Airflow's metadata database, which can
still use SQLite for local development.

Batch identity is the valuation date plus a SHA-256 hash of the input data and
model version. Publishing a batch replaces **all output rows for its valuation
date** inside a single transaction, without adding duplicates. Results for other
dates are preserved. A transaction-scoped advisory
lock serializes publications, including concurrent reruns and table bootstrap.
A failed write rolls back the entire batch replacement, preserving previously
published results and the previous date's current-batch marker. Different inputs,
seeds, or model versions produce separate metadata records, but only the most
recently published batch's outputs remain active for a given valuation date.
Reports reject a superseded batch manifest rather than reading another batch's
date-level results. Existing report files and per-batch calculation artifacts
remain available. Bump `MODEL_VERSION` whenever the
calculation methodology changes. Published batches are checked for finite PV,
nonnegative exposure and CVA, required result coverage, and reconciliation of
exposure increments to MA CVA and of MA CVA to counterparty CVA.

### Migrating existing PostgreSQL tables

The first publication after this update migrates the old batch-keyed output
tables transactionally. It moves them intact to `airflow_xva_batch_archive`
and creates the new valuation-date tables in `airflow_xva`. Batch metadata
stays in `airflow_xva.batches`.

Migration also removes `trade_pv.currency` from existing valuation-date tables
after verifying that all stored PV rows are EUR. Non-EUR rows block migration
and remain unchanged; they need explicit FX conversion or archival first.
The legacy archive retains its original currency column.

Dates with exactly one historical batch are copied to the new tables automatically.
For dates with several historical batches, the old schema has no publication
timestamp from which to infer a winner. All old rows remain in the archive;
republish the selected frozen batch to establish that date's current outputs.
The batch being published during migration becomes current for its own date.
The migration refuses to overwrite an existing archive or guess at a partially
modified schema. No archive tables are deleted by normal publication.

## Validation

```bash
python -m pytest --cov=risk_pipeline --cov-report=term-missing --cov-fail-under=90 -v
```

Tests cover pricing signs, par and zero-rate cases, expiry, credit interpolation,
netting before the positive part, collateral thresholds and timing, deterministic
scenarios, controlled CVA examples, invalid allocations, artifact mismatches,
reconciliation failures, and repeatable PostgreSQL publication. Database tests
also verify real transaction rollback, read-only enforcement, concurrent reruns,
valuation-date replacement, preservation of other dates, and archival migration.
Workflow tests check
scheduled feed dates, cross-DAG batch identities, isolated report reruns,
read-only report generation, and rejection of superseded batches. DAG tests
check trigger targets and success dependencies as well
as the existing demo.

CI starts an isolated PostgreSQL 17 service and runs the database integration
tests. For local integration tests, configure an Airflow connection whose ID
starts with `xva_test_` to a **disposable database whose name ends in `_test`**,
then set `XVA_TEST_POSTGRES_CONN_ID` to that connection ID before running pytest.
These tests drop and recreate the `airflow_xva` and `airflow_xva_batch_archive`
schemas in the test database.
Without the test connection, database integration tests are explicitly skipped;
calculation and DAG-structure tests still run. Never point this fixture at the
normal `risk_postgres` database.
