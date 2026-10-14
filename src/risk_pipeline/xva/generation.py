"""Synthetic portfolios, seeded variation, and deliberate data-quality faults."""

import argparse
import copy
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import json
import math
from pathlib import Path
import random

from risk_pipeline.xva.inputs import integer, number


FAULTS = ("counterparty_code", "recovery_rate", "notional", "ma_allocation", "credit_curve", "currency")


def current_valuation_date():
    """Return the current London calendar date for standalone synthetic generation."""
    return datetime.now(ZoneInfo("Europe/London")).date().isoformat()


def generate_portfolio(valuation_date, counterparty_count=2000, trade_count=10000, seed=42,
                       horizon_days=365, simulation_paths=20, collateral_fraction=0.7):
    """Generate a reproducible synthetic EUR portfolio with consistent cross-feed identifiers.

    Every counterparty receives one curve, one MA, and at least one trade.
    A controlled fraction receive CSAs. Standard maturity buckets keep the
    exposure reporting grid compact even with thousands of random trades.
    """
    integer(counterparty_count, "counterparty_count", 1)
    integer(trade_count, "trade_count", 1)
    integer(seed, "generation seed", 0)
    integer(horizon_days, "horizon_days", 1)
    integer(simulation_paths, "simulation_paths", 1)
    number(collateral_fraction, "collateral_fraction", 0, 1)
    if counterparty_count > 9_999_999:
        raise ValueError("counterparty_count exceeds the seven-character code capacity")
    if trade_count < counterparty_count:
        raise ValueError("trade_count must be at least counterparty_count so every counterparty has a trade")
    valuation = date.fromisoformat(valuation_date)
    rng = random.Random(seed)
    collateralized = set(rng.sample(range(counterparty_count), int(counterparty_count * collateral_fraction + 0.5)))
    maturities = sorted({max(1, horizon_days * quarter // 4) for quarter in (1, 2, 3, 4)})
    payload = {"valuation_date": valuation_date, "reporting_currency": "EUR",
               "market": {"flat_rate": 0.04, "rate_volatility": 0.01},
               "simulation": {"paths": simulation_paths, "seed": seed, "exposure_frequency_days": 30},
               "counterparties": [], "credit_curves": [], "master_agreements": [], "csas": [], "trades": []}
    for index in range(counterparty_count):
        code = f"{index + 1:07d}"
        ma_id, curve_id = f"MA_{code}", f"CREDIT_{code}"
        recovery, hazard = rng.uniform(0.25, 0.6), rng.uniform(0.005, 0.08)
        payload["counterparties"].append({"code": code, "name": f"Synthetic Counterparty {code}",
                                          "recovery_rate": recovery, "credit_curve_id": curve_id})
        payload["credit_curves"].append({"curve_id": curve_id, "valuation_date": valuation_date,
            "points": [{"date": (valuation + timedelta(days=day)).isoformat(),
                        "survival_probability": math.exp(-hazard * day / 365)} for day in maturities]})
        payload["master_agreements"].append({"ma_id": ma_id, "counterparty_code": code})
        if index in collateralized:
            payload["csas"].append({"csa_id": f"CSA_{code}", "ma_id": ma_id, "currency": "EUR",
                "margin_frequency_days": rng.choice([1, 7, 14]), "settlement_lag_days": rng.choice([0, 1, 2]),
                "post_collateral": True, "receive_collateral": True,
                "posting_threshold": rng.choice([0, 10000, 50000]), "receiving_threshold": rng.choice([0, 10000, 50000]),
                "minimum_transfer_amount": rng.choice([0, 1000, 5000]), "initial_balance": 0})
    for index in range(trade_count):
        counterparty_index = index if index < counterparty_count else rng.randrange(counterparty_count)
        code = f"{counterparty_index + 1:07d}"
        maturity = horizon_days if index == 0 else rng.choice(maturities)
        payload["trades"].append({"trade_id": f"IRS{index + 1:08d}", "trade_type": "interest_rate_swap",
            "counterparty_code": code, "ma_id": f"MA_{code}",
            "csa_id": f"CSA_{code}" if counterparty_index in collateralized else None, "currency": "EUR",
            "notional": round(math.exp(rng.uniform(math.log(100000), math.log(25000000))) / 1000) * 1000,
            "direction": rng.choice(["pay_fixed", "receive_fixed"]), "fixed_rate": rng.uniform(0.01, 0.065),
            "start_date": (valuation - timedelta(days=rng.randrange(366))).isoformat(),
            "maturity_date": (valuation + timedelta(days=maturity)).isoformat()})
    return payload


def generate_submissions(template=None, count=8, seed=42, noise=0.1, rejection_rate=0.25, valuation_date=None,
                         counterparty_count=None, trade_count=None, horizon_days=365, simulation_paths=20,
                         collateral_fraction=0.7):
    """Return varied portfolio submissions with a seeded, configurable number of invalid payloads.

    Bounded market/recovery variation and positive notional/threshold scaling
    keep ordinary noise valid. Explicit faults supply predictable rejections.
    Each payload is a complete alternative portfolio, not an incremental update.
    Without an in-memory template, generate all entities programmatically;
    templates are supported for tests and are never loaded from example files.
    """
    integer(count, "submission count", 1)
    integer(seed, "generation seed", 0)
    number(noise, "noise", 0, 0.5)
    number(rejection_rate, "rejection_rate", 0, 1)
    if template is None or counterparty_count is not None or trade_count is not None:
        template = generate_portfolio(valuation_date or (template["valuation_date"] if template else current_valuation_date()),
                                      2000 if counterparty_count is None else counterparty_count,
                                      10000 if trade_count is None else trade_count, seed,
                                      horizon_days, simulation_paths, collateral_fraction)
    rng = random.Random(seed)
    rejected = set(rng.sample(range(count), int(count * rejection_rate + 0.5)))
    shift = (date.fromisoformat(valuation_date or template["valuation_date"])
             - date.fromisoformat(template["valuation_date"]))
    payloads = []
    for index in range(count):
        payload = copy.deepcopy(template)
        payload["valuation_date"] = (date.fromisoformat(template["valuation_date"]) + shift).isoformat()
        market = payload["market"]
        market["flat_rate"] = min(1, max(-0.1, market["flat_rate"] + rng.uniform(-noise, noise) * 0.01))
        market["rate_volatility"] = min(0.1, max(0, market["rate_volatility"] * rng.uniform(1 - noise, 1 + noise)))
        payload["simulation"]["seed"] = rng.randrange(2**31)
        for counterparty in payload["counterparties"]:
            counterparty["recovery_rate"] = min(1, max(0, counterparty["recovery_rate"] + rng.uniform(-noise, noise)))
        for curve in payload["credit_curves"]:
            curve["valuation_date"] = payload["valuation_date"]
            hazard_scale = rng.uniform(1 - noise, 1 + noise)
            for point in curve["points"]:
                point["date"] = (date.fromisoformat(point["date"]) + shift).isoformat()
                point["survival_probability"] **= hazard_scale
        for trade in payload["trades"]:
            trade["notional"] *= rng.uniform(1 - noise, 1 + noise)
            trade["fixed_rate"] = min(1, max(-0.1, trade["fixed_rate"] + rng.uniform(-noise, noise) * 0.01))
            for field in ("start_date", "maturity_date"):
                trade[field] = (date.fromisoformat(trade[field]) + shift).isoformat()
        for csa in payload["csas"]:
            for field in ("posting_threshold", "receiving_threshold", "minimum_transfer_amount"):
                csa[field] *= rng.uniform(1 - noise, 1 + noise)
        if index in rejected:
            inject_fault(payload, rng.choice(FAULTS))
        payloads.append(payload)
    return payloads


def inject_fault(payload, fault):
    """Apply one supported input-contract violation to a generated portfolio in place."""
    if fault == "counterparty_code":
        payload["counterparties"][0]["code"] = "BAD"
    elif fault == "recovery_rate":
        payload["counterparties"][0]["recovery_rate"] = 1.2
    elif fault == "notional":
        payload["trades"][0]["notional"] = -1000
    elif fault == "ma_allocation":
        payload["trades"][0]["ma_id"] = "UNKNOWN_MA"
    elif fault == "credit_curve":
        payload["credit_curves"][0]["points"][0]["survival_probability"] = 1.1
    elif fault == "currency":
        payload["trades"][0]["currency"] = "USD"
    else:
        raise ValueError(f"Unknown demo fault: {fault}")


def write_feed_sets(payloads, root):
    """Materialize generated portfolios as independent source files and return their input-DAG configurations."""
    root = Path(root).expanduser().resolve()
    configurations = []
    for index, payload in enumerate(payloads):
        directory = root / f"submission-{index:03d}"
        directory.mkdir(parents=True, exist_ok=True)
        feeds = {}
        for kind in ("counterparties", "credit_curves", "master_agreements", "csas", "trades", "settings"):
            data = ({field: payload[field] for field in ("valuation_date", "reporting_currency", "market", "simulation")}
                    if kind == "settings" else {"valuation_date": payload["valuation_date"], "rows": payload[kind]})
            path = directory / f"{kind}.json"
            path.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")
            feeds[kind] = str(path)
        configuration = {"valuation_date": payload["valuation_date"], "feeds": feeds}
        (directory / "run_config.json").write_text(json.dumps(configuration, indent=2) + "\n")
        configurations.append(configuration)
    return configurations


def main():
    """Write generated portfolios at the requested scale as independent feeds or a legacy envelope."""
    parser = argparse.ArgumentParser(description="Generate varied XVA portfolios with intentional invalid submissions")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--output")
    target.add_argument("--feed-directory")
    parser.add_argument("--count", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--noise", type=float, default=0.1)
    parser.add_argument("--rejection-rate", type=float)
    parser.add_argument("--valuation-date")
    parser.add_argument("--counterparties", type=int)
    parser.add_argument("--trades", type=int)
    parser.add_argument("--horizon-days", type=int, default=365)
    parser.add_argument("--simulation-paths", type=int, default=20)
    parser.add_argument("--collateral-fraction", type=float, default=0.7)
    args = parser.parse_args()
    count = args.count if args.count is not None else 1
    rejection_rate = args.rejection_rate if args.rejection_rate is not None else 0
    payloads = generate_submissions(None, count,
                                   args.seed, args.noise, rejection_rate, args.valuation_date,
                                   args.counterparties, args.trades, args.horizon_days,
                                   args.simulation_paths, args.collateral_fraction)
    if args.feed_directory:
        root = Path(args.feed_directory).expanduser().resolve()
        write_feed_sets(payloads, root)
        print(str(root))
        return
    output = Path(args.output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"submissions": payloads}, indent=2, allow_nan=False) + "\n")
    print(str(output.resolve()))


if __name__ == "__main__":
    main()
