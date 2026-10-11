"""Seeded variation and deliberate data-quality faults for the ingestion demo."""

import argparse
import copy
from datetime import date
import json
from pathlib import Path
import random

from risk_pipeline.xva.inputs import integer, number


FAULTS = ("counterparty_code", "recovery_rate", "notional", "ma_allocation", "credit_curve", "currency")


def generate_submissions(template, count=8, seed=42, noise=0.1, rejection_rate=0.25, valuation_date=None):
    """Return varied portfolio submissions with a seeded, configurable number of invalid payloads.

    Bounded market/recovery variation and positive notional/threshold scaling
    keep ordinary noise valid. Explicit faults supply predictable rejections.
    Each payload is a complete alternative portfolio, not an incremental update.
    """
    integer(count, "submission count", 1)
    integer(seed, "generation seed", 0)
    number(noise, "noise", 0, 0.5)
    number(rejection_rate, "rejection_rate", 0, 1)
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


def main():
    """Write a generated submission envelope for later database ingestion by xva_inputs."""
    parser = argparse.ArgumentParser(description="Generate varied XVA portfolios with intentional invalid submissions")
    parser.add_argument("--template", default="examples/xva/inputs.json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--count", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--noise", type=float, default=0.1)
    parser.add_argument("--rejection-rate", type=float, default=0.25)
    parser.add_argument("--valuation-date")
    args = parser.parse_args()
    payloads = generate_submissions(json.loads(Path(args.template).expanduser().read_text()), args.count,
                                   args.seed, args.noise, args.rejection_rate, args.valuation_date)
    output = Path(args.output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"submissions": payloads}, indent=2, allow_nan=False) + "\n")
    print(str(output.resolve()))


if __name__ == "__main__":
    main()
