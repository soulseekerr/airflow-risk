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
python -m risk_pipeline.xva.generation --output /tmp/xva-generated.json --count 1
python -m risk_pipeline.xva.batch \
  --inputs /tmp/xva-generated.json \
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

## Run the calculation chain and independent feed ingestion

| DAG | Schedule | Tasks |
| --- | --- | --- |
| `xva_counterparty_rerun` | Triggered only | Validate corrected feeds, recompute one counterparty, publish and refresh reports |
| `xva_generate_inputs` | Triggered only | Generate a scalable synthetic portfolio as source feeds, trigger inputs |
| `xva_feed_ingestion` | Triggered only | Stage one independently arriving source/feed |
| `xva_inputs` | Daily, initially paused | Bind explicit feed versions, validate/freeze, trigger calculations |
| `xva_calculations` | Triggered only | Validate batch identity, simulate, calculate, publish, trigger reports |
| `xva_reports` | Triggered only | Validate published batch, generate reports |

```text
xva_inputs: ingest_raw_inputs → validate_and_freeze → trigger_calculations
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

The checkout is inferred from the DAG file location. If you copy DAGs into a
separate Airflow directory, set `XVA_PROJECT_ROOT` to the checkout. Relative
explicit input paths are resolved against that checkout.

Trigger **`xva_generate_inputs`** to generate independent random source files and
run the complete pipeline. You can also trigger **`xva_inputs`** with an empty
configuration: it generates 2,000 counterparties and 10,000 trades before staging
the six source feeds in PostgreSQL. Neither path reads example files.
Manual generation defaults to the current London calendar date; supply
`valuation_date` to override it. Retries reuse the saved generation date and files.

For daily operation, unpause `xva_inputs`. Its `Europe/London` timetable generates
random inputs for the preceding calendar day's valuation date (the interval start):

```bash
airflow dags unpause xva_inputs
```

Explicit `feeds` references or `input_path` remain supported for replaying generated
files and integrating feeds. Files under `data/xva` are not automatically loaded.
Do not trigger `xva_calculations` with an empty configuration: its `validate_batch`
task requires the frozen identity and path supplied by `xva_inputs`.

When the input schedule is enabled, `airflow dags trigger xva_inputs --conf ...`
can also be used. Only `xva_inputs` has a schedule. Synthetic generation, individual feed ingestion,
calculations, and reports use `schedule=None`; calculations and reports each
have `max_active_runs=1`.

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
airflow dags trigger xva_calculations --conf \
  '{"batch_id":"<batch_id>","snapshot_path":"/absolute/path/to/xva/<batch_id>/inputs.json"}'

airflow dags trigger xva_reports --conf \
  '{"batch_id":"<batch_id>","publication_path":"/absolute/path/to/xva/<batch_id>/published.json"}'
```

If `validate_batch` reports missing `batch_id` and `snapshot_path`, inspect the
failed run's configuration. Clearing tasks in that run preserves its configuration,
so an empty configuration will fail again. Start `xva_inputs` for a fresh batch,
or create a new calculation run with both values from `validate_and_freeze`'s
`return_value` XCom. If the run was automatically triggered, compare its
configuration with that XCom and check that all DAG files are up to date.

An entirely rejected ingestion fails validation after recording its rejection
reasons. An ingestion with some rejected submissions can still proceed with its
selected accepted portfolio. Calculation errors fail immediately; publication allows two retries
for storage failures. The original `xva_daily_batch` DAG has been replaced by
these three DAGs.

## Independent input feeds and normalized storage

Each input type has its own feed, source, version ID, valuation date, and ingestion
timestamp. Feed arrivals are independent of portfolio assembly. A calculation uses
an **explicit set of feed IDs**; it never silently picks the latest source record.
All chosen feeds must declare the input set's valuation date. Cross-feed allocations,
curve coverage, and business rules are checked together before calculation.

| Feed type | Relational tables | Example source |
| --- | --- | --- |
| `counterparties` | `input_counterparties` | Counterparty/reference-data system |
| `credit_curves` | `input_credit_curves`, `input_credit_curve_points` | Credit market-data system |
| `master_agreements` | `input_master_agreements` | Legal agreement system |
| `csas` | `input_csas` | Collateral agreement system |
| `trades` | `input_trades` | Front-office trade system |
| `settings` | `input_settings` | Market assumptions and run configuration for this demo |

The tables contain explicit typed fields and one row per entity or curve point,
keyed by feed version and source row index. Identifiers are indexed within a feed.
Invalid business values can still land: staging columns are nullable and each
`raw_row` retains that entity's original values. Curve points are separate rows,
rather than an array inside a curve record. Calculation inputs are assembled from
these **relational columns**, not a portfolio JSONB field. Filter through an accepted
input set when querying validated business data, since the landing tables also
retain rejected source rows.

Metadata and audit tables are:

- `input_feed_submissions`: feed type/version, source, date, checksum, request key,
  receipt timestamp, and row-validation status. `raw_document` is retained only
  when the document cannot be parsed into rows.
- `input_ingestions`: request provenance and expected number of input sets, allowing
  interrupted ingestion to resume without calculating an incomplete group.
- `input_sets`: explicit portfolio assemblies, validation status, batch identity,
  and selected artifact path; no full portfolio payload is stored here.
- `input_set_feeds`: the exact feed versions bound to each input set.
- `input_validation_errors`: input set, feed, source row, optional curve-point index,
  field, reason, and timestamp. A bad trade rejects its complete input set; it is
  never silently excluded from risk calculations.

Raw rows commit before business validation. A valid set is frozen into the usual
`<output>/<batch_id>/inputs.json` for calculation tasks and VS Code replay. Existing
result-side `batches.input_snapshot` remains a publication audit snapshot; it is
not the primary input store. If all sets are rejected, rejection records commit
before the validation task fails. With several demo alternatives, the last accepted
set in source order is selected; the others remain audited and are not calculated.

### Supply independently arriving feeds

The six example sources under [examples/xva/feeds](examples/xva/feeds) remain
as test fixtures and input-format references; runtime defaults do not read them.
Entity feeds accept an array of rows, or `{"valuation_date":"2026-10-11","rows":[...]}`.
The settings feed accepts a single object containing `valuation_date`,
`reporting_currency`, `market`, and `simulation`, or its flat relational fields.
A CSA feed must be present but can explicitly contain `[]` for an entirely
uncollateralized portfolio.

To ingest one source independently, trigger **`xva_feed_ingestion`** with:

```json
{
  "feed_type": "trades",
  "input_path": "/path/to/front-office/trades.json",
  "valuation_date": "2026-10-11",
  "source": "front-office"
}
```

The `ingest_source` return-value XCom contains its `feed_id`. This DAG stages the
source only; it does not trigger calculations. The same operation is available
from the CLI:

```bash
python -m risk_pipeline.xva.feeds --feed-type trades \
  --input /path/to/front-office/trades.json --valuation-date 2026-10-11 \
  --source front-office
```

Once all sources have arrived, trigger **`xva_inputs`** with their explicit IDs:

```json
{
  "valuation_date": "2026-10-11",
  "feeds": {
    "counterparties": {"feed_id": "<counterparty-feed-id>"},
    "credit_curves": {"feed_id": "<credit-feed-id>"},
    "master_agreements": {"feed_id": "<agreement-feed-id>"},
    "csas": {"feed_id": "<csa-feed-id>"},
    "trades": {"feed_id": "<trade-feed-id>"},
    "settings": {"feed_id": "<settings-feed-id>"}
  }
}
```

A feed entry may instead be a file path, or an object with `path` and `source`;
then `xva_inputs` stages that particular source before binding it. References and
file entries can be mixed. Relative paths resolve against the project checkout.
To correct a trade feed, ingest a **new trade version** and create a new input set
with that ID while reusing the other source IDs. Existing accepted sets remain
immutable. Clearing a task reuses unchanged versions; changed contents under the
same retry key are rejected. Retrying validation can reconstruct a missing snapshot
artifact from its normalized database rows. `{"ingestion_id":"<ingestion_id>"}`
reuses an existing group without requiring its source files.

Legacy combined files and generated portfolios are compatibility adapters: they
are split into individual entity feeds before validation. Existing old
`input_submissions` records are retained unchanged. Referencing an old ingestion
ID creates normalized feed versions for it and revalidates them; no historical
input or output table is deleted.

### Generate 2,000 counterparties and 10,000 trades

`xva_generate_inputs` is an intermediate source-generation layer:

```text
xva_generate_inputs → xva_inputs → xva_calculations → xva_reports
```

It builds a synthetic portfolio from code instead of repeating the two-counterparty,
three-trade example. By default it creates exactly **2,000 counterparties, 10,000
trades, 2,000 credit curves, 2,000 MAs, and 1,400 CSAs**. Every counterparty has
a unique seven-character code and at least one trade. Trade directions, notionals,
fixed rates, allocations, recovery rates, credit hazards, collateral frequencies,
lags, and thresholds vary reproducibly. One MA and one curve are created per
counterparty; 70% of MAs are collateralized by default.

Unpause `xva_inputs`, `xva_calculations`, and `xva_reports`, then trigger
**`xva_generate_inputs`**. An empty configuration uses those sizes and the run's
London calendar date. To make the parameters explicit, use:

```json
{
  "valuation_date": "2026-10-12",
  "generation": {
    "counterparty_count": 2000,
    "trade_count": 10000,
    "seed": 42,
    "noise": 0.1,
    "horizon_days": 365,
    "simulation_paths": 20,
    "collateral_fraction": 0.7
  }
}
```

The generator writes six independent source files under
`<output_root>/generated/<generation_id>/submission-000/`, then passes their
paths to `xva_inputs`. Portfolio rows do not travel through XCom. Files and
generation parameters are preserved for retries; changing parameters requires
a new generation run. The same seed and valuation date produce the same data;
change the seed for another sample. A retry can reproduce missing files.

The synthetic default is a one-year horizon, 20 simulation paths, and monthly
exposure reporting. Standard quarterly maturity buckets prevent thousands of
different expiry dates from expanding the reporting grid to every day. Increasing
path count or horizon increases the calculation workload. Sizes must be positive,
and `trade_count` must be at least `counterparty_count`; codes support up to
9,999,999 counterparties. Counts apply to this portfolio's feed versions; the
database also retains earlier versions, so total table counts can be larger.

The generation DAG produces one portfolio with no deliberately invalid submission
by default. For several alternatives with rejected submissions, use the existing
`generate_demo` mode below, optionally with the same size controls.

To generate the source files directly without triggering Airflow:

```bash
python -m risk_pipeline.xva.generation --feed-directory /tmp/xva-large \
  --counterparties 2000 --trades 10000 --seed 42 --valuation-date 2026-10-12 \
  --simulation-paths 20 --horizon-days 365
```

The CLI defaults to **one valid random portfolio**, with 2,000 counterparties
and 10,000 trades even without size flags. It reads no template file. Its
`submission-000/run_config.json` can also be used to trigger `xva_inputs` manually.

### Generate noisy demo inputs with deliberate rejections

Trigger `xva_inputs` with this configuration in the Airflow UI:

```json
{
  "generate_demo": true,
  "valuation_date": "2026-10-11",
  "generation": {
    "count": 8,
    "seed": 42,
    "noise": 0.1,
    "rejection_rate": 0.25
  }
}
```

The generator creates independent feed versions for each alternative input set.
This produces **six accepted and two rejected random portfolios**,
then calculates the last accepted one. Ordinary noise varies rates, notionals,
recovery rates, credit survival probabilities, and CSA thresholds within valid
bounds. Deliberate faults create predictable rejects: invalid counterparty codes,
out-of-range recovery rates, negative notionals, unknown MAs, invalid survival
probabilities, or unsupported trade currencies. Validation checks actual payloads;
it does not accept/reject them based on a generator label.

Add `counterparty_count` and `trade_count` to `generation` to change the default
2,000 counterparties and 10,000 trades. `generate_demo` defaults to one valid
random portfolio unless `count` and `rejection_rate` are explicitly supplied. For example, `count: 8` and
`rejection_rate: 0.25` produce eight full-size alternatives with two rejects.

`noise` must be between 0 and 0.5. `rejection_rate` is between 0 and 1; the rejected
count is `count * rejection_rate`, rounded to the nearest integer with halves up.
The seed makes the generated data repeatable; change it for another sample.
Market parameters and simulation seeds also vary across portfolios. An all-rejected
generation is permitted and intentionally fails validation while preserving its audit trail.
Manual generation uses the current London date unless `valuation_date` is supplied.
Scheduled generation builds random data for the daily interval start date, including
consistent credit-curve points and trade dates. Explicit file ingestion remains available.

You can also generate a file before ingestion:

```bash
python -m risk_pipeline.xva.generation --output /tmp/xva-submissions.json \
  --count 8 --seed 42 --noise 0.1 --rejection-rate 0.25 --valuation-date 2026-10-11
airflow dags trigger xva_inputs --conf '{"input_path":"/tmp/xva-submissions.json"}'
```

Generate separate source files instead of a combined envelope with:

```bash
python -m risk_pipeline.xva.generation --feed-directory /tmp/xva-feeds \
  --count 8 --seed 42 --noise 0.1 --rejection-rate 0.25 --valuation-date 2026-10-11
```

Each `submission-NNN` directory contains six feed files and `run_config.json`.
Use that configuration to submit one alternative through `xva_inputs`.

Inspect source versions, structured rejections, and specific trade rows with:

```sql
SELECT feed_id, feed_type, source, valuation_date, received_at, status
FROM airflow_xva.input_feed_submissions ORDER BY received_at DESC;

SELECT e.submission_id, f.source, e.feed_type, e.row_index, e.point_index,
       e.field, e.message
FROM airflow_xva.input_validation_errors e
LEFT JOIN airflow_xva.input_feed_submissions f USING (feed_id)
ORDER BY e.error_id DESC;

SELECT t.trade_id, t.counterparty_code, t.currency, t.notional
FROM airflow_xva.input_trades t
JOIN airflow_xva.input_set_feeds b ON b.feed_id = t.feed_id AND b.feed_type = 'trades'
JOIN airflow_xva.input_sets s ON s.submission_id = b.submission_id
WHERE s.status = 'accepted' AND s.submission_id = '<submission_id>';
```

## Rerun one counterparty after correcting inputs

Trigger **`xva_counterparty_rerun`** with the valuation date, seven-character
counterparty code, and the feed versions you corrected:

```json
{
  "valuation_date": "2026-10-14",
  "counterparty_code": "0000001",
  "feeds": {
    "counterparties": "/path/to/corrected-counterparties.json",
    "trades": {"feed_id": "<new-trades-feed-id>"}
  }
}
```

Each supplied feed is a **complete version of that source**, with the other
counterparties' rows unchanged. For example, a revised trades file contains all
trades, including the corrected counterparty's changes. Supply only the feeds
that changed; omitted feeds come from the current published input snapshot for
that date. File paths, `{ "path": "...", "source": "provider" }` objects, and
`{ "feed_id": "..." }` references use the existing feed-ingestion format.
The rerun does not generate new random data or select feeds by arrival time.

The DAG stages raw feed versions before validation, freezes the corrected input
set, and calculates **only the selected counterparty** on the existing shared
scenario and exposure-reporting grid. It atomically replaces that counterparty's
trade PVs, exposures, netting-set CVA, and counterparty CVA. Its `value_adj` resets
to the new `value_calc`. Other counterparties' rows, adjustment timestamps, and
non-CVA XVA types are preserved. Removed trades or netting sets leave no old output
rows. An invalid correction or a publication failure leaves current outputs intact.

A new current batch snapshot represents the complete portfolio with the correction,
so the automatically triggered `xva_reports` run includes all counterparties and
the corrected date-level CVA total. Previous batch snapshots remain available for
audit; their old publication manifests are superseded.

Clear a failed task in the **same DAG run** to retry the frozen correction. A
publication retry after commit does not apply the correction twice. To change the
feed contents or select another feed version, start a new DAG run. If another full
batch or counterparty correction publishes while this rerun is in flight, it fails
as stale; start a new run against the current revision.

This version supports counterparty data, credit curves, trades, MAs, and CSA
corrections for existing counterparties. Changes to shared market/simulation
settings, a curve used by another counterparty, or the portfolio-wide maturity or
reporting grid require a full batch rerun. A new counterparty also requires a full
batch. A counterparty can have all its trades removed provided other portfolio
trades remain; its recalculated CVA becomes zero.

Inspect the correction history in PostgreSQL:

```sql
SELECT valuation_date, counterparty_code, status,
       old_cva, new_cva, new_cva - old_cva AS cva_change,
       base_batch_id, published_batch_id, published_at
FROM airflow_xva.counterparty_reruns
ORDER BY prepared_at DESC;
```

## Input contract

[examples/xva/inputs.json](examples/xva/inputs.json) is a test fixture with
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

**Wrong-way risk** occurs when exposure to a counterparty tends to increase as
that counterparty becomes more likely to default. For example, an energy producer
may owe more on a commodity derivative as energy prices fall, while those same
falling prices weaken its ability to pay. This dependence can increase CVA
relative to an assumption of independent exposure and default; the current demo
does not model it.

## Results and reruns

Artifacts are stored beneath `<output>/<batch_id>/`:

| Artifact | Contents |
| --- | --- |
| `inputs.json` | Validated input snapshot, model version, and batch ID |
| `scenarios.json` | Shared daily rate paths |
| `results.json` | Trade PV, MA exposure profiles, MA CVA, and counterparty CVA |
| `published.json` | Written after successful reconciliation and database publication |

Reports are stored at `<output>/<batch_id>/reports/v2/`; earlier `v1` reports remain available:

| Report | Contents |
| --- | --- |
| `counterparty_summary.csv` | Counterparty name/code, trade count, signed PV t0, calculated CVA (`cva`), adjusted CVA (`cva_adj`), and PV minus adjusted CVA |
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

The `airflow_xva` PostgreSQL schema contains the normalized input/feed tables
described above, plus `batches`, `trade_pv`, `exposures`,
`netting_set_cva`, and `counterparty_xva` tables. **Output tables contain
`valuation_date`, not `batch_id`.** Their primary keys are:

| Table | Primary key |
| --- | --- |
| `trade_pv` | `valuation_date`, `trade_id` |
| `exposures` | `valuation_date`, `ma_id`, `exposure_date` |
| `netting_set_cva` | `valuation_date`, `ma_id` |
| `counterparty_xva` | `valuation_date`, `counterparty_code`, `xva_type` |

`valuation_date` is the batch's as-of date. `exposure_date` is a future date on
the exposure profile; both are retained on exposure rows. Query results directly
by valuation date, for example:

```sql
SELECT * FROM airflow_xva.trade_pv WHERE valuation_date = DATE '2026-10-10';
SELECT * FROM airflow_xva.counterparty_xva WHERE valuation_date = DATE '2026-10-10';
```

`counterparty_xva` stores generic counterparty XVA measures:

| Column | Meaning |
| --- | --- |
| `valuation_date` | As-of date |
| `counterparty_code` | Seven-character counterparty code |
| `currency` | Result currency (currently EUR) |
| `xva_type` | Measure name; this batch publishes `CVA`, with room for measures such as `DVA` and `FVA` |
| `value_calc` | Calculated amount |
| `value_adj` | Final amount including any manual adjustment; initially equals `value_calc` |
| `updated_at` | PostgreSQL `TIMESTAMPTZ`, automatically stamped on insertion and every update |

`value_adj` is the complete adjusted amount, rather than an adjustment delta.
Rerunning the batch **resets CVA `value_adj` to the newly calculated `value_calc`**.
Other XVA types are preserved when this CVA batch is republished. Values must be
finite; generic measures can be signed. The current calculation still produces
nonnegative CVA losses.

For example, a manual adjustment needs no explicit timestamp update:

```sql
UPDATE airflow_xva.counterparty_xva
SET value_adj = 1250.00
WHERE valuation_date = DATE '2026-10-10'
  AND counterparty_code = '0000001' AND xva_type = 'CVA';
```

Reports reconcile calculated CVA against the exposure results and publication
manifest. They show both calculated and adjusted CVA, and use `value_adj` for
PV minus CVA. Report regeneration picks up manual adjustments without
recalculating the batch.

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
model version. Publishing a batch replaces its PV, exposure, netting-set CVA,
and counterparty `CVA` rows **for its valuation date** inside a single transaction,
without adding duplicates. Results for other
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

The first publication renames the existing `counterparty_cva` table to
`counterparty_xva` transactionally, preserving rows, adjustments, and timestamps.
If both names exist, migration stops rather than overwriting either table.
Existing counterparty `cva` columns migrate to `value_calc`, with `xva_type='CVA'`
and `value_adj=value_calc`. Previously batch-keyed counterparty rows are copied
from the archive with the same defaults. Existing amounts are preserved;
`updated_at` starts at migration time because the old schema has no write timestamp.
This migration runs during publication and is repeatable.

Dates with exactly one historical batch are copied to the new tables automatically.
For dates with several historical batches, the old schema has no publication
timestamp from which to infer a winner. All old rows remain in the archive;
republish the selected frozen batch to establish that date's current outputs.
The batch being published during migration becomes current for its own date.
The migration refuses to overwrite an existing archive or guess at a partially
modified schema. No archive tables are deleted by normal publication.

## Debugging a saved calculation in VS Code

Select the Python interpreter used for this project, with the project installed
(`python -m pip install -e .`) and its Airflow dependencies available. Set a
breakpoint at the start of `calculate()` in
`src/risk_pipeline/xva/calculations.py`. Choose **Replay saved XVA calculation**
in Run and Debug, press **F5**, and enter the batch directory, for example
`~/airflow/xva/<batch_id>`. The checked-in `.vscode/launch.json` contains settings
and arguments only; the replay command lives in the calculation module.

The debugger loads that batch's `inputs.json` and `scenarios.json` and calls
`calculate()` with the original data. While paused, use the Debug Console to
change an input before it is used, for example:

```python
inputs["trades"][0]["notional"] = 2_000_000
```

Continue execution. The integrated terminal prints counterparty CVA and prompts
for the next action: **Enter** recalculates with the current in-memory inputs and
scenarios, **r** regenerates scenarios and recalculates, and **q** exits. Each
calculation stops at the breakpoint again. Use **r** after changing the market
rate, volatility, seed, path count, valuation date, or maturity horizon. Keep
related input fields consistent, including credit-curve dates and coverage.
Restarting the debugger reloads the original files and discards in-memory edits.

The command reads saved artifacts and prints results; it does not overwrite
files, publish to PostgreSQL, or alter the completed Airflow run. To replay once
from the terminal and print all calculation results:

```bash
python -m risk_pipeline.xva.calculations --batch-dir ~/airflow/xva/<batch_id>
```

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
They also cover generic XVA keys, adjustment defaults and resets, manual-edit
timestamps, signed measures, and calculated versus adjusted reporting.
Input tests verify commit-before-validation, durable row-level rejection reasons,
independent feed reuse, explicit date binding, thousands of trade rows, preserved
legacy history, seeded rejection counts, immutable retries, interrupted-group
recovery, and reconstruction from normalized database rows.
Synthetic generation tests check exact 2,000/10,000 dimensions, consistent links,
seeded variation, collateral coverage, and a complete full-size database workflow
using a short horizon and one simulation path for test speed.
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
