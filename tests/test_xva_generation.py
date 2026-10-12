import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from risk_pipeline.xva.generation import FAULTS, generate_portfolio, generate_submissions, inject_fault
from risk_pipeline.xva.inputs import validate


@pytest.fixture
def template():
    """Load a small EUR portfolio for deterministic generation tests."""
    payload = json.loads((Path(__file__).resolve().parents[1] / "examples/xva/inputs.json").read_text())
    payload["simulation"]["paths"] = 2
    return payload


def test_generation_is_seeded_and_has_exact_rejection_count(template):
    """Check repeatability, independent payloads, and controlled valid/invalid submission counts."""
    before = copy.deepcopy(template)
    payloads = generate_submissions(template, count=12, seed=73, rejection_rate=0.25)
    assert generate_submissions(template, count=12, seed=73, rejection_rate=0.25) == payloads
    assert generate_submissions(template, count=12, seed=74, rejection_rate=0.25) != payloads
    accepted, rejected = 0, 0
    for payload in payloads:
        try:
            validate(payload)
        except ValueError:
            rejected += 1
        else:
            accepted += 1
    assert (accepted, rejected) == (9, 3)
    assert template == before
    assert len({payload["simulation"]["seed"] for payload in payloads}) == 12


def test_maximum_noise_stays_valid_and_dates_shift_together(template):
    """Verify bounded variation preserves allocations and credit coverage when valuation dates change."""
    payloads = generate_submissions(template, count=50, noise=0.5, rejection_rate=0, valuation_date="2026-11-12")
    for payload in payloads:
        validate(payload)
        assert payload["valuation_date"] == "2026-11-12"
        assert all(curve["valuation_date"] == payload["valuation_date"] for curve in payload["credit_curves"])
        assert payload["trades"][0]["maturity_date"] == "2028-11-12"


@pytest.mark.parametrize("fault", FAULTS)
def test_each_fault_breaks_the_contract(template, fault):
    """Verify every deliberate fault produces a genuine validation rejection."""
    inject_fault(template, fault)
    with pytest.raises(ValueError):
        validate(template)


def test_unknown_fault_rejected(template):
    """Reject unsupported fault names instead of silently producing valid demo data."""
    with pytest.raises(ValueError, match="Unknown demo fault"):
        inject_fault(template, "unknown")


@pytest.mark.parametrize("options", [{"count": 0}, {"seed": True}, {"noise": -0.1}, {"noise": 0.6},
                                     {"rejection_rate": 1.1}, {"count": 1.5}])
def test_invalid_generation_options_rejected(template, options):
    """Reject invalid generator configuration before creating submissions."""
    with pytest.raises(ValueError):
        generate_submissions(template, **options)


def test_generator_cli_writes_submission_envelope(tmp_path):
    """Check the standalone generator creates a portable file consumable by the input DAG."""
    repository = Path(__file__).resolve().parents[1]
    output = tmp_path / "generated/submissions.json"
    subprocess.run([sys.executable, "-m", "risk_pipeline.xva.generation", "--template",
                    str(repository / "examples/xva/inputs.json"), "--output", str(output),
                    "--count", "4", "--seed", "17", "--rejection-rate", "0.5"], check=True, capture_output=True)
    assert len(json.loads(output.read_text())["submissions"]) == 4


def test_large_portfolio_has_exact_counts_unique_links_and_compact_maturities():
    """Verify a 2,000-counterparty/10,000-trade portfolio is varied, consistent, and valid before ingestion."""
    payload = generate_portfolio("2026-10-12")
    validate(payload)
    assert len(payload["counterparties"]) == len(payload["credit_curves"]) == len(payload["master_agreements"]) == 2000
    assert len(payload["trades"]) == 10000 and len(payload["csas"]) == 1400
    codes = {row["code"] for row in payload["counterparties"]}
    assert len(codes) == 2000 and all(len(code) == 7 for code in codes)
    assert {row["counterparty_code"] for row in payload["trades"]} == codes
    assert len({row["trade_id"] for row in payload["trades"]}) == 10000
    assert len({row["maturity_date"] for row in payload["trades"]}) == 4
    assert len({row["notional"] for row in payload["trades"]}) > 100
    assert {row["direction"] for row in payload["trades"]} == {"pay_fixed", "receive_fixed"}
    assert payload["simulation"]["paths"] == 20


def test_scaled_portfolios_are_seeded_and_keep_rejection_controls(template):
    """Check synthetic dimensions work with reproducible noise and explicit rejected alternatives."""
    options = {"counterparty_count": 6, "trade_count": 30, "simulation_paths": 1, "horizon_days": 7,
               "count": 4, "seed": 51, "rejection_rate": 0.5, "valuation_date": "2026-10-12"}
    payloads = generate_submissions(template, **options)
    assert generate_submissions(template, **options) == payloads
    assert generate_submissions(template, **{**options, "seed": 52}) != payloads
    accepted = 0
    for payload in payloads:
        assert len(payload["counterparties"]) == 6 and len(payload["trades"]) == 30
        try:
            validate(payload)
        except ValueError:
            pass
        else:
            accepted += 1
    assert accepted == 2


@pytest.mark.parametrize("options", [{"counterparty_count": 0}, {"counterparty_count": 10_000_000},
                                     {"trade_count": 1}, {"horizon_days": 0}, {"simulation_paths": 0},
                                     {"collateral_fraction": 1.1}, {"seed": True}])
def test_invalid_synthetic_dimensions_rejected(options):
    """Reject impossible synthetic sizes or settings before allocating portfolio rows."""
    with pytest.raises(ValueError):
        generate_portfolio("2026-10-12", **options)


@pytest.mark.parametrize("collateral_fraction", [0, 1])
def test_short_horizon_and_collateral_extremes(collateral_fraction):
    """Keep one-day datasets and entirely collateralized or uncollateralized portfolios valid."""
    payload = generate_portfolio("2026-10-12", 4, 10, horizon_days=1, simulation_paths=1,
                                 collateral_fraction=collateral_fraction)
    validate(payload)
    assert len(payload["csas"]) == int(4 * collateral_fraction)


def test_large_generator_cli_defaults_to_one_valid_set(tmp_path):
    """Produce a single full-size portfolio as independent feeds when size flags are supplied."""
    root = tmp_path / "scaled-feeds"
    subprocess.run([sys.executable, "-m", "risk_pipeline.xva.generation", "--feed-directory", str(root),
                    "--counterparties", "2000", "--trades", "10000", "--simulation-paths", "1",
                    "--horizon-days", "30", "--valuation-date", "2026-10-12"], check=True, capture_output=True)
    assert sorted(path.name for path in root.iterdir()) == ["submission-000"]
    configuration = json.loads((root / "submission-000/run_config.json").read_text())
    assert len(json.loads(Path(configuration["feeds"]["counterparties"]).read_text())["rows"]) == 2000
    assert len(json.loads(Path(configuration["feeds"]["trades"]).read_text())["rows"]) == 10000
