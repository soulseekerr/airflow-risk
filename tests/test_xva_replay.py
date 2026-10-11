import copy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from risk_pipeline.xva.batch import freeze_inputs, read_json, simulate, value_batch, write_json
from risk_pipeline.xva import calculations


@pytest.fixture
def saved_batch(tmp_path):
    source = Path(__file__).resolve().parents[1] / "examples/xva/inputs.json"
    inputs = read_json(source)
    inputs["simulation"]["paths"] = 2
    snapshot = freeze_inputs(write_json(tmp_path / "source.json", inputs), tmp_path / "batches")
    scenarios = simulate(snapshot)
    value_batch(snapshot, scenarios)
    return Path(snapshot).parent


def test_replay_cli_reproduces_task_results_without_writing(saved_batch):
    before = {path: path.read_bytes() for path in saved_batch.iterdir()}
    process = subprocess.run(
        [sys.executable, "-m", "risk_pipeline.xva.calculations", "--batch-dir", str(saved_batch)],
        capture_output=True, text=True, check=True,
    )
    assert json.loads(process.stdout) == read_json(saved_batch / "results.json")["results"]
    assert {path: path.read_bytes() for path in saved_batch.iterdir()} == before


def test_replay_rejects_mismatched_scenarios(saved_batch, monkeypatch):
    payload = read_json(saved_batch / "scenarios.json")
    payload["batch_id"] = "another-batch"
    write_json(saved_batch / "scenarios.json", payload)
    monkeypatch.setattr(sys, "argv", ["calculations", "--batch-dir", str(saved_batch)])
    with pytest.raises(ValueError, match="another batch"):
        calculations.main()


@pytest.mark.parametrize("regenerate", [False, True])
def test_interactive_edits_recompute_in_memory(saved_batch, monkeypatch, regenerate):
    before = {path: path.read_bytes() for path in saved_batch.iterdir()}
    original_calculate = calculations.calculate
    results, scenarios_used = [], []
    current_inputs = None

    def observe(inputs, scenarios):
        nonlocal current_inputs
        current_inputs = inputs
        scenarios_used.append(copy.deepcopy(scenarios))
        result = original_calculate(inputs, scenarios)
        results.append(result)
        return result

    responses = iter(["invalid", "r" if regenerate else "", "q"])

    def edit_at_prompt(prompt):
        response = next(responses)
        if response in ("r", ""):
            if regenerate:
                current_inputs["market"]["flat_rate"] += 0.01
            else:
                current_inputs["trades"][0]["notional"] *= 2
        return response

    monkeypatch.setattr(calculations, "calculate", observe)
    monkeypatch.setattr("builtins.input", edit_at_prompt)
    monkeypatch.setattr(sys, "argv", ["calculations", "--batch-dir", str(saved_batch), "--interactive"])
    calculations.main()
    assert len(results) == 2
    assert results[0]["trade_pv"] != results[1]["trade_pv"]
    assert (scenarios_used[0] != scenarios_used[1]) == regenerate
    assert {path: path.read_bytes() for path in saved_batch.iterdir()} == before
