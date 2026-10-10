"""Input contract and validation, deliberately limited to one currency."""

import math
from datetime import date


MODEL_VERSION = "flat-rate-swap-demo-v1"


def number(value, label, lower=None, upper=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    if not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")
    if lower is not None and value < lower:
        raise ValueError(f"{label} must be >= {lower}")
    if upper is not None and value > upper:
        raise ValueError(f"{label} must be <= {upper}")
    return value


def integer(value, label, minimum):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{label} must be an integer >= {minimum}")
    return value


def identifier(value, label):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a nonempty string")
    return value


def indexed(rows, key):
    result = {}
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{key} collection must be a nonempty list")
    for row in rows:
        value = identifier(row[key], key)
        if value in result:
            raise ValueError(f"Duplicate {key}: {value}")
        result[value] = row
    return result


def validate(inputs):
    """Reject unsupported allocations and invalid data before simulation."""
    valuation = date.fromisoformat(inputs["valuation_date"])
    currency = identifier(inputs["reporting_currency"], "reporting_currency")
    if len(currency) != 3 or not currency.isalpha() or not currency.isupper():
        raise ValueError("reporting_currency must be a three-letter uppercase code")
    market = inputs["market"]
    number(market["flat_rate"], "flat_rate", -0.1, 1)
    number(market["rate_volatility"], "rate_volatility", 0, 0.1)
    integer(inputs["simulation"]["paths"], "paths", 1)
    integer(inputs["simulation"]["seed"], "seed", 0)
    integer(inputs["simulation"]["exposure_frequency_days"], "exposure_frequency_days", 1)

    counterparties = indexed(inputs["counterparties"], "code")
    curves = indexed(inputs["credit_curves"], "curve_id")
    agreements = indexed(inputs["master_agreements"], "ma_id")
    csas = indexed(inputs["csas"], "csa_id") if inputs["csas"] else {}
    trades = indexed(inputs["trades"], "trade_id")
    for cpty in counterparties.values():
        if len(cpty["code"]) != 7:
            raise ValueError("Counterparty code must contain exactly seven characters")
        identifier(cpty["name"], "counterparty name")
        number(cpty["recovery_rate"], "recovery_rate", 0, 1)
        if cpty["credit_curve_id"] not in curves:
            raise ValueError("Counterparty references an unknown credit curve")

    horizon = max(date.fromisoformat(t["maturity_date"]) for t in trades.values())
    for curve in curves.values():
        if curve["valuation_date"] != inputs["valuation_date"]:
            raise ValueError("Credit curve valuation date must match batch valuation date")
        previous_date, previous_survival = valuation, 1.0
        if not curve["points"]:
            raise ValueError("Credit curve must have points")
        for point in curve["points"]:
            maturity = date.fromisoformat(point["date"])
            survival = number(point["survival_probability"], "survival_probability", 0, 1)
            if survival == 0 or maturity <= previous_date or survival > previous_survival:
                raise ValueError("Credit curve dates must increase and positive survival must not increase")
            previous_date, previous_survival = maturity, survival
        if previous_date < horizon:
            raise ValueError("Credit curve must cover all trade maturities; extrapolation is disabled")

    for ma in agreements.values():
        if ma["counterparty_code"] not in counterparties:
            raise ValueError("MA references an unknown counterparty")
    allocated = set()
    for csa in csas.values():
        if csa["ma_id"] not in agreements or csa["ma_id"] in allocated:
            raise ValueError("Each CSA must reference a known MA with at most one CSA")
        allocated.add(csa["ma_id"])
        if csa["currency"] != currency:
            raise ValueError("Cross-currency collateral is not supported")
        integer(csa["margin_frequency_days"], "margin_frequency_days", 1)
        integer(csa["settlement_lag_days"], "settlement_lag_days", 0)
        for field in ("post_collateral", "receive_collateral"):
            if not isinstance(csa[field], bool):
                raise ValueError(f"{field} must be boolean")
        for field in ("posting_threshold", "receiving_threshold", "minimum_transfer_amount"):
            number(csa[field], field, 0)
        balance = number(csa["initial_balance"], "initial_balance")
        if (balance > 0 and not csa["receive_collateral"]) or (balance < 0 and not csa["post_collateral"]):
            raise ValueError("Initial collateral balance conflicts with posting/receiving rules")

    for trade in trades.values():
        ma = agreements.get(trade["ma_id"])
        if ma is None or ma["counterparty_code"] != trade["counterparty_code"]:
            raise ValueError("Trade counterparty must match its MA")
        csa_id = trade["csa_id"]
        matching = [c["csa_id"] for c in csas.values() if c["ma_id"] == trade["ma_id"]]
        if csa_id != (matching[0] if matching else None):
            raise ValueError("Trade CSA allocation must match its MA's CSA")
        if trade["trade_type"] != "interest_rate_swap":
            raise ValueError("Only interest_rate_swap trades are supported")
        if trade["currency"] != currency:
            raise ValueError("Cross-currency trades are not supported")
        if trade["direction"] not in ("pay_fixed", "receive_fixed"):
            raise ValueError("Swap direction must be pay_fixed or receive_fixed")
        number(trade["notional"], "notional", 0)
        if trade["notional"] == 0:
            raise ValueError("notional must be positive")
        number(trade["fixed_rate"], "fixed_rate", -0.1, 1)
        start = date.fromisoformat(trade["start_date"])
        maturity = date.fromisoformat(trade["maturity_date"])
        if not start <= valuation < maturity:
            raise ValueError("Trades must have started and mature after the valuation date")
    return inputs
