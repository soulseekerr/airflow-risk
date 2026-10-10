"""Transparent demo models, not calibrated production swap pricing."""

import math
import random
from datetime import date, timedelta


def swap_pv(trade, as_of, rate):
    """Continuous-annuity approximation; positive PV is an asset to us."""
    years = max((date.fromisoformat(trade["maturity_date"]) - as_of).days / 365, 0)
    annuity = years if abs(rate) < 1e-10 else -math.expm1(-rate * years) / rate
    direction = 1 if trade["direction"] == "pay_fixed" else -1
    return direction * trade["notional"] * (rate - trade["fixed_rate"]) * annuity


def survival(curve, valuation, target):
    """Log-linear survival interpolation, equivalent to piecewise constant hazard."""
    left_date, left_probability = valuation, 1.0
    for point in curve["points"]:
        right_date = date.fromisoformat(point["date"])
        right_probability = point["survival_probability"]
        if target <= right_date:
            weight = (target - left_date).days / (right_date - left_date).days
            return math.exp(math.log(left_probability) * (1 - weight) + math.log(right_probability) * weight)
        left_date, left_probability = right_date, right_probability
    raise ValueError("Exposure date falls beyond the credit curve")


def generate_scenarios(inputs):
    valuation = date.fromisoformat(inputs["valuation_date"])
    horizon = max((date.fromisoformat(t["maturity_date"]) - valuation).days for t in inputs["trades"])
    random_source = random.Random(inputs["simulation"]["seed"])
    initial = inputs["market"]["flat_rate"]
    daily_vol = inputs["market"]["rate_volatility"] / math.sqrt(365)
    paths = []
    for _ in range(inputs["simulation"]["paths"]):
        path = [initial]
        for _ in range(horizon):
            path.append(path[-1] + random_source.gauss(0, daily_vol))
        paths.append(path)
    return {"dates": [(valuation + timedelta(days=d)).isoformat() for d in range(horizon + 1)], "rates": paths}


def collateral_target(net_value, csa):
    """Signed balance: positive collateral held, negative collateral posted."""
    if net_value > 0 and csa["receive_collateral"]:
        return max(net_value - csa["receiving_threshold"], 0)
    if net_value < 0 and csa["post_collateral"]:
        return min(net_value + csa["posting_threshold"], 0)
    return 0.0


def exposure_path(trades, dates, rates, csa):
    balance = csa["initial_balance"] if csa else 0.0
    pending = {}
    exposures = []
    for day, (as_of, rate) in enumerate(zip(dates, rates)):
        # Demo closeout convention: return all collateral at netting-set expiry.
        if all(as_of >= date.fromisoformat(t["maturity_date"]) for t in trades):
            exposures.append(0.0)
            continue
        balance += pending.pop(day, 0.0)
        net_value = sum(swap_pv(t, as_of, rate) for t in trades)
        # Day zero is the supplied collateral snapshot, before new margin calls.
        if csa and day > 0 and day % csa["margin_frequency_days"] == 0:
            committed = balance + sum(pending.values())
            transfer = collateral_target(net_value, csa) - committed
            if transfer and abs(transfer) >= csa["minimum_transfer_amount"]:
                due = day + csa["settlement_lag_days"]
                if due == day:
                    balance += transfer
                else:
                    pending[due] = pending.get(due, 0.0) + transfer
        exposures.append(max(net_value - balance, 0.0))
    return exposures


def reporting_days(inputs, horizon_days):
    valuation = date.fromisoformat(inputs["valuation_date"])
    frequency = inputs["simulation"]["exposure_frequency_days"]
    days = set(range(0, horizon_days + 1, frequency)) | {horizon_days}
    days.update((date.fromisoformat(t["maturity_date"]) - valuation).days for t in inputs["trades"])
    return sorted(days)


def calculate(inputs, scenarios):
    valuation = date.fromisoformat(inputs["valuation_date"])
    dates = [date.fromisoformat(d) for d in scenarios["dates"]]
    flat_rate = inputs["market"]["flat_rate"]
    horizon = max(date.fromisoformat(t["maturity_date"]) for t in inputs["trades"])
    if dates != [valuation + timedelta(days=i) for i in range((horizon - valuation).days + 1)]:
        raise ValueError("Scenario dates must cover the daily valuation-to-maturity grid")
    if len(scenarios["rates"]) != inputs["simulation"]["paths"]:
        raise ValueError("Scenario count does not match configuration")
    for rates in scenarios["rates"]:
        if len(rates) != len(dates) or rates[0] != flat_rate or not all(math.isfinite(r) for r in rates):
            raise ValueError("Invalid scenario rate path")
    pv = [{"trade_id": t["trade_id"], "ma_id": t["ma_id"], "counterparty_code": t["counterparty_code"],
           "currency": t["currency"], "pv": swap_pv(t, valuation, flat_rate)} for t in inputs["trades"]]
    counterparties = {c["code"]: c for c in inputs["counterparties"]}
    curves = {c["curve_id"]: c for c in inputs["credit_curves"]}
    csas = {c["ma_id"]: c for c in inputs["csas"]}
    profiles, cva_rows = [], []
    sample_days = reporting_days(inputs, len(dates) - 1)
    for ma in inputs["master_agreements"]:
        trades = [t for t in inputs["trades"] if t["ma_id"] == ma["ma_id"]]
        if not trades:
            continue
        sums = [0.0] * len(dates)
        for rates in scenarios["rates"]:
            for day, exposure in enumerate(exposure_path(trades, dates, rates, csas.get(ma["ma_id"]))):
                sums[day] += exposure
        expected = [value / len(scenarios["rates"]) for value in sums]
        cpty = counterparties[ma["counterparty_code"]]
        curve = curves[cpty["credit_curve_id"]]
        cva, previous_survival, previous_discounted = 0.0, 1.0, expected[0]
        for day in sample_days:
            discount = math.exp(-flat_rate * day / 365)
            probability = survival(curve, valuation, dates[day])
            discounted = expected[day] * discount
            contribution = (1 - cpty["recovery_rate"]) * (previous_survival - probability) * (previous_discounted + discounted) / 2
            cva += contribution
            profiles.append({"ma_id": ma["ma_id"], "counterparty_code": cpty["code"], "date": dates[day].isoformat(),
                             "currency": inputs["reporting_currency"], "expected_positive_exposure": expected[day],
                             "discount_factor": discount, "survival_probability": probability, "cva_increment": contribution})
            previous_survival, previous_discounted = probability, discounted
        cva_rows.append({"ma_id": ma["ma_id"], "counterparty_code": cpty["code"], "cva": cva, "currency": inputs["reporting_currency"]})
    aggregates = [{"counterparty_code": c["code"], "cva": sum(row["cva"] for row in cva_rows if row["counterparty_code"] == c["code"]),
                   "currency": inputs["reporting_currency"]} for c in inputs["counterparties"]]
    return {"trade_pv": pv, "exposures": profiles, "netting_set_cva": cva_rows, "counterparty_cva": aggregates}


def reconcile(results, inputs=None):
    if inputs is not None:
        expected_trades = {t["trade_id"] for t in inputs["trades"]}
        expected_mas = {t["ma_id"] for t in inputs["trades"]}
        expected_counterparties = {c["code"] for c in inputs["counterparties"]}
        for collection, key, expected in (("trade_pv", "trade_id", expected_trades),
                                           ("netting_set_cva", "ma_id", expected_mas),
                                           ("counterparty_cva", "counterparty_code", expected_counterparties)):
            values = [r[key] for r in results[collection]]
            if len(values) != len(set(values)) or set(values) != expected:
                raise ValueError(f"Incomplete or duplicate {collection} results")
        if {p["ma_id"] for p in results["exposures"]} != expected_mas:
            raise ValueError("Incomplete exposure results")
        valuation = date.fromisoformat(inputs["valuation_date"])
        horizon_days = max((date.fromisoformat(t["maturity_date"]) - valuation).days for t in inputs["trades"])
        expected_dates = {(valuation + timedelta(days=d)).isoformat() for d in reporting_days(inputs, horizon_days)}
        allocations = {ma["ma_id"]: ma["counterparty_code"] for ma in inputs["master_agreements"]}
        for ma_id in expected_mas:
            dates = [p["date"] for p in results["exposures"] if p["ma_id"] == ma_id]
            if len(dates) != len(set(dates)) or set(dates) != expected_dates:
                raise ValueError("Incomplete or duplicate exposure dates")
        for collection in results.values():
            for row in collection:
                if row["currency"] != inputs["reporting_currency"]:
                    raise ValueError("Result currency mismatch")
                if "ma_id" in row and row["counterparty_code"] != allocations[row["ma_id"]]:
                    raise ValueError("Result counterparty allocation mismatch")
    for row in results["trade_pv"]:
        if not math.isfinite(row["pv"]):
            raise ValueError("Nonfinite trade PV")
    for row in results["exposures"]:
        for key in ("expected_positive_exposure", "discount_factor", "survival_probability", "cva_increment"):
            if not math.isfinite(row[key]) or row[key] < 0:
                raise ValueError(f"Invalid exposure result: {key}")
    for row in results["netting_set_cva"] + results["counterparty_cva"]:
        if not math.isfinite(row["cva"]) or row["cva"] < 0:
            raise ValueError("Invalid CVA")
    for row in results["netting_set_cva"]:
        total = sum(p["cva_increment"] for p in results["exposures"] if p["ma_id"] == row["ma_id"])
        if not math.isclose(total, row["cva"], abs_tol=1e-8, rel_tol=1e-10):
            raise ValueError("CVA profile does not reconcile")
    for row in results["counterparty_cva"]:
        total = sum(p["cva"] for p in results["netting_set_cva"] if p["counterparty_code"] == row["counterparty_code"])
        if not math.isclose(total, row["cva"], abs_tol=1e-8, rel_tol=1e-10):
            raise ValueError("Counterparty CVA does not reconcile")
