import copy
import math
from datetime import date, timedelta
from pathlib import Path

import pytest

from risk_pipeline.xva.batch import freeze_inputs, load_snapshot, publish, read_json, run_batch, simulate, value_batch, write_json
from risk_pipeline.xva.calculations import calculate, collateral_target, exposure_path, generate_scenarios, reconcile, survival, swap_pv
from risk_pipeline.xva.inputs import validate
from risk_pipeline.xva.postgres import transaction


@pytest.fixture
def inputs():
    """Load a fresh sample portfolio with fewer simulation paths for fast calculation tests."""
    value = read_json(Path(__file__).resolve().parents[1] / "examples/xva/inputs.json")
    value["simulation"]["paths"] = 4
    return value


def test_reporting_currency_must_be_eur_even_for_single_currency_inputs(inputs):
    """Reject a consistent non-EUR portfolio because the demo has no FX conversion."""
    inputs["reporting_currency"] = "USD"
    for row in inputs["trades"] + inputs["csas"]:
        row["currency"] = "USD"
    with pytest.raises(ValueError, match="must be EUR"):
        validate(inputs)


def test_swap_pv_direction_par_zero_rate_and_expiry(inputs):
    """Verify swap PV signs, par pricing, the zero-rate limit, and zero value after maturity."""
    trade = inputs["trades"][0]
    as_of = date(2026, 10, 10)
    years = (date(2028, 10, 10) - as_of).days / 365
    expected = 1_000_000 * 0.01 * (1 - math.exp(-0.04 * years)) / 0.04
    assert swap_pv(trade, as_of, 0.04) == pytest.approx(expected)
    assert swap_pv(trade, as_of, trade["fixed_rate"]) == 0
    assert swap_pv(trade, date(2028, 10, 10), 0.04) == 0
    assert swap_pv(trade, as_of, 0) == pytest.approx(-1_000_000 * 0.03 * years)
    assert swap_pv({**trade, "direction": "receive_fixed"}, as_of, 0.04) == pytest.approx(-expected)


def test_survival_interpolation_and_no_extrapolation(inputs):
    """Verify log-linear survival interpolation and reject dates beyond the credit curve."""
    curve = inputs["credit_curves"][0]
    valuation = date(2026, 10, 10)
    assert survival(curve, valuation, valuation) == 1
    assert survival(curve, valuation, date(2027, 10, 10)) == pytest.approx(0.98)
    target = date(2028, 4, 10)
    weight = (target - date(2027, 10, 10)).days / 366
    assert survival(curve, valuation, target) == pytest.approx(0.98 ** (1 - weight) * 0.95 ** weight)
    with pytest.raises(ValueError, match="beyond"):
        survival(curve, valuation, date(2030, 1, 1))


def test_scenarios_repeatable_and_shared(inputs):
    """Check seeded scenarios repeat exactly and cover the portfolio horizon."""
    first = generate_scenarios(inputs)
    assert first == generate_scenarios(inputs)
    inputs["simulation"]["seed"] += 1
    assert first != generate_scenarios(inputs)
    assert all(p[0] == 0.04 for p in first["rates"])
    assert first["dates"][-1] == "2028-10-10"


def test_netting_before_positive_part(inputs):
    """Verify offsetting trades net to zero before positive exposure is calculated."""
    trade = inputs["trades"][0]
    opposite = {**trade, "direction": "receive_fixed"}
    dates = [date(2026, 10, 10), date(2026, 10, 11)]
    assert exposure_path([trade, opposite], dates, [0.05, 0.06], None) == [0, 0]
    assert exposure_path([trade], dates, [0.05, 0.06], None)[0] > 0


def test_collateral_frequency_lag_mta_and_threshold(inputs):
    """Check margin dates, settlement lag, minimum transfers, and receiving thresholds."""
    trade = inputs["trades"][0]
    dates = [date(2026, 10, 10) + timedelta(days=i) for i in range(5)]
    csa = {**inputs["csas"][0], "margin_frequency_days": 2, "settlement_lag_days": 1,
           "minimum_transfer_amount": 0, "receiving_threshold": 1000}
    rates = [0.04] * 5
    exposed = exposure_path([trade], dates, rates, csa)
    assert exposed[0] == swap_pv(trade, dates[0], 0.04)
    assert exposed[2] > 1000  # Call on day 2 has not yet settled.
    assert 0 < exposed[3] < 1000  # Previous day's collateral, slightly declining PV.
    csa["settlement_lag_days"] = 0
    assert exposure_path([trade], dates, rates, csa)[2] == pytest.approx(1000)
    csa["minimum_transfer_amount"] = 1e9
    assert exposure_path([trade], dates, rates, csa) == exposure_path([trade], dates, rates, None)


def test_collateral_directions_initial_balance_and_expiry(inputs):
    """Check posting/receiving permissions, initial balances, and collateral closeout at expiry."""
    csa = inputs["csas"][0]
    assert collateral_target(30000, csa) == 20000
    assert collateral_target(-30000, csa) == -20000
    assert collateral_target(5000, csa) == 0
    assert collateral_target(30000, {**csa, "receive_collateral": False}) == 0
    assert collateral_target(-30000, {**csa, "post_collateral": False}) == 0
    trade = inputs["trades"][0]
    as_of = date(2026, 10, 10)
    initial = {**csa, "initial_balance": -1000}
    assert exposure_path([trade], [as_of], [0.04], initial)[0] == pytest.approx(swap_pv(trade, as_of, 0.04) + 1000)
    assert exposure_path([trade], [date(2028, 10, 10)], [0.04], initial) == [0]


def test_cva_controlled_example(inputs):
    # Zero rate/volatility; positive swap PV decreases linearly from 100 to zero.
    """Verify CVA against a simple known exposure/default example and zero-loss limiting cases."""
    inputs["trades"] = [{**inputs["trades"][2], "notional": 10000, "fixed_rate": -0.01, "maturity_date": "2027-10-10"}]
    inputs["market"] = {"flat_rate": 0, "rate_volatility": 0}
    inputs["simulation"]["exposure_frequency_days"] = 365
    inputs["credit_curves"][1]["points"][0]["survival_probability"] = 0.9
    inputs["credit_curves"][1]["points"][1]["survival_probability"] = 0.8
    result = calculate(validate(inputs), generate_scenarios(inputs))
    assert result["trade_pv"][0]["pv"] == 100
    assert [p["expected_positive_exposure"] for p in result["exposures"]] == [100, 0]
    assert result["netting_set_cva"][0]["cva"] == pytest.approx(3)  # LGD .6 * PD .1 * mean EE 50
    reconcile(result, inputs)
    inputs["counterparties"][1]["recovery_rate"] = 1
    assert calculate(inputs, generate_scenarios(inputs))["netting_set_cva"][0]["cva"] == 0
    inputs["counterparties"][1]["recovery_rate"] = 0.4
    for point in inputs["credit_curves"][1]["points"]:
        point["survival_probability"] = 1
    assert calculate(inputs, generate_scenarios(inputs))["netting_set_cva"][0]["cva"] == 0


@pytest.mark.parametrize("path,value", [
    (("reporting_currency",), "gbp"), (("reporting_currency",), ""),
    (("market", "flat_rate"), float("nan")), (("market", "flat_rate"), "0.04"),
    (("market", "flat_rate"), True), (("market", "flat_rate"), -0.2),
    (("market", "flat_rate"), 2), (("market", "rate_volatility"), -1),
    (("simulation", "paths"), 0), (("simulation", "seed"), 1.5),
    (("simulation", "paths"), True), (("simulation", "exposure_frequency_days"), 0),
    (("counterparties", 0, "code"), "123"), (("counterparties", 0, "code"), 1234567),
    (("counterparties", 0, "name"), ""), (("counterparties", 0, "credit_curve_id"), "UNKNOWN"),
    (("counterparties", 0, "recovery_rate"), 1.1),
    (("credit_curves", 0, "valuation_date"), "2026-10-09"),
    (("credit_curves", 0, "points"), []),
    (("credit_curves", 0, "points", 0, "date"), "2026-10-10"),
    (("credit_curves", 0, "points", 1, "survival_probability"), 0.99),
    (("credit_curves", 0, "points", 0, "survival_probability"), 0),
    (("credit_curves", 0, "points", 1, "date"), "2028-01-01"),
    (("master_agreements", 0, "counterparty_code"), "UNKNOWN"),
    (("csas", 0, "ma_id"), "UNKNOWN"), (("csas", 0, "currency"), "USD"),
    (("csas", 0, "post_collateral"), "yes"),
    (("csas", 0, "minimum_transfer_amount"), -1),
    (("trades", 0, "counterparty_code"), "0000002"),
    (("trades", 0, "ma_id"), "UNKNOWN"), (("trades", 0, "csa_id"), None),
    (("trades", 0, "trade_type"), "fx_forward"), (("trades", 0, "currency"), "USD"),
    (("trades", 0, "direction"), "invalid"), (("trades", 0, "notional"), 0),
    (("trades", 0, "start_date"), "2027-01-01"),
    (("trades", 0, "maturity_date"), "2026-10-10"),
    (("trades",), []), (("trades",), {}),
])
def test_invalid_inputs_rejected(inputs, path, value):
    """Reject unsupported or invalid values across the input contract."""
    parent = inputs
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    with pytest.raises(ValueError):
        validate(inputs)


def test_duplicate_allocations_and_collateral_rules(inputs):
    """Reject duplicate trade identifiers, multiple CSAs per MA, and incompatible collateral balances."""
    duplicate = copy.deepcopy(inputs)
    duplicate["trades"].append(duplicate["trades"][0])
    with pytest.raises(ValueError, match="Duplicate"):
        validate(duplicate)
    duplicate = copy.deepcopy(inputs)
    duplicate["csas"].append({**duplicate["csas"][0], "csa_id": "ANOTHER"})
    with pytest.raises(ValueError, match="at most one"):
        validate(duplicate)
    for field, balance in (("post_collateral", -1), ("receive_collateral", 1)):
        changed = copy.deepcopy(inputs)
        changed["csas"][0][field] = False
        changed["csas"][0]["initial_balance"] = balance
        with pytest.raises(ValueError, match="conflicts"):
            validate(changed)


def test_batch_publication_repeatability_and_identity(inputs, tmp_path, pg_connection):
    """Check deterministic batch identity and repeatable publication without duplicate database rows."""
    source = tmp_path / "source.json"
    write_json(source, inputs)
    manifest_path = run_batch(source, tmp_path / "output", pg_connection)
    before = Path(manifest_path).read_bytes()
    assert run_batch(source, tmp_path / "output", pg_connection) == manifest_path
    assert Path(manifest_path).read_bytes() == before
    with transaction(pg_connection, readonly=True) as cursor:
        for table, expected in (("batches", 1), ("trade_pv", 3), ("netting_set_cva", 2)):
            cursor.execute(f"SELECT COUNT(*) FROM airflow_xva.{table}")
            assert cursor.fetchone()[0] == expected
        cursor.execute("SELECT counterparty_code FROM airflow_xva.trade_pv ORDER BY trade_id LIMIT 1")
        assert cursor.fetchone()[0] == "0000001"
    inputs["simulation"]["seed"] += 1
    write_json(source, inputs)
    assert run_batch(source, tmp_path / "output", pg_connection) != manifest_path
    with transaction(pg_connection, readonly=True) as cursor:
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.batches")
        assert cursor.fetchone()[0] == 2


def test_corrupt_artifacts_not_published(inputs, tmp_path):
    """Reject mismatched results and altered frozen snapshots before publication."""
    source = write_json(tmp_path / "source.json", inputs)
    snapshot = freeze_inputs(source, tmp_path / "output")
    scenario_path = simulate(snapshot)
    result_path = value_batch(snapshot, scenario_path)
    changed = read_json(scenario_path)
    changed["batch_id"] = "other"
    write_json(scenario_path, changed)
    with pytest.raises(ValueError, match="another batch"):
        value_batch(snapshot, scenario_path)
    changed = read_json(result_path)
    changed["batch_id"] = "other"
    write_json(result_path, changed)
    with pytest.raises(ValueError, match="another batch"):
        publish(snapshot, result_path)
    changed = read_json(snapshot)
    changed["inputs"]["simulation"]["seed"] += 1
    write_json(snapshot, changed)
    with pytest.raises(ValueError, match="identity"):
        load_snapshot(snapshot)


@pytest.mark.parametrize("change", ["pv", "exposure", "netting_cva", "profile_total", "counterparty_total", "missing",
                                   "missing_ma", "missing_date", "currency", "allocation"])
def test_reconciliation_rejects_broken_results(inputs, change):
    """Reject missing, misallocated, nonfinite, or financially inconsistent result collections."""
    results = calculate(inputs, generate_scenarios(inputs))
    if change == "pv":
        results["trade_pv"][0]["pv"] = math.inf
    elif change == "exposure":
        results["exposures"][0]["expected_positive_exposure"] = -1
    elif change == "netting_cva":
        results["netting_set_cva"][0]["cva"] = -1
    elif change == "profile_total":
        results["exposures"][1]["cva_increment"] += 100
    elif change == "counterparty_total":
        results["counterparty_cva"][0]["cva"] += 100
    elif change == "missing_ma":
        results["exposures"] = [r for r in results["exposures"] if r["ma_id"] != "MA_BANK"]
    elif change == "missing_date":
        results["exposures"].pop()
    elif change == "currency":
        results["trade_pv"][0]["currency"] = "USD"
    elif change == "allocation":
        results["trade_pv"][0]["counterparty_code"] = "0000002"
    else:
        results["trade_pv"].pop()
    with pytest.raises(ValueError):
        reconcile(results, inputs)


@pytest.mark.parametrize("change", ["dates", "count", "length", "rate"])
def test_invalid_scenarios_rejected(inputs, change):
    """Reject scenario date grids, counts, lengths, or rates that conflict with the input contract."""
    scenarios = generate_scenarios(inputs)
    if change == "dates":
        scenarios["dates"].pop()
    elif change == "count":
        scenarios["rates"].pop()
    elif change == "length":
        scenarios["rates"][0].pop()
    else:
        scenarios["rates"][0][1] = math.nan
    with pytest.raises(ValueError):
        calculate(inputs, scenarios)
