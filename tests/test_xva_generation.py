import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from risk_pipeline.xva.generation import FAULTS, generate_submissions, inject_fault
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
