import json
from pathlib import Path

import pytest

from risk_pipeline.xva.batch import read_json, simulate, value_batch, publish
from risk_pipeline.xva.postgres import transaction
from risk_pipeline.xva.reports import generate_reports
from risk_pipeline.xva.workflow import generate_input_sources, ingest_inputs
from risk_pipeline.xva.feeds import validate_ingestion


REPOSITORY = Path(__file__).resolve().parents[1]


def test_generation_layer_defaults_and_immutable_retry(tmp_path, monkeypatch):
    """Generate one full-size portfolio and reuse its sources on retry without regenerating or overwriting them."""
    configuration = {"output_root": str(tmp_path)}
    handoff = generate_input_sources(configuration, REPOSITORY, "large-source-run", run_date="2026-10-12")
    assert handoff["valuation_date"] == "2026-10-12"
    assert handoff["generation_summary"] == {"counterparties": 2000, "trades": 10000, "csas": 1400,
                                             "seed": 42, "simulation_paths": 20}
    assert len(json.dumps(handoff)) < 5000  # Only file paths and small metadata travel through XCom.
    previous = {path: Path(path).read_bytes() for path in handoff["feeds"].values()}

    def forbidden(*args, **kwargs):
        """Detect unnecessary regeneration of already persisted source versions."""
        raise AssertionError("Retry must reuse existing source artifacts")

    monkeypatch.setattr("risk_pipeline.xva.workflow.generate_submissions", forbidden)
    assert generate_input_sources(configuration, REPOSITORY, "large-source-run", run_date="2026-10-12") == handoff
    assert {path: Path(path).read_bytes() for path in handoff["feeds"].values()} == previous
    with pytest.raises(ValueError, match="new generation run"):
        generate_input_sources({**configuration, "generation": {"seed": 99}}, REPOSITORY,
                               "large-source-run", run_date="2026-10-12")


def test_generation_layer_recovers_missing_source_file(tmp_path):
    """Reproduce a lost source from its seed and request while keeping the same source/version handoff."""
    configuration = {"output_root": str(tmp_path), "valuation_date": "2026-10-12",
                     "generation": {"counterparty_count": 3, "trade_count": 12}}
    original = generate_input_sources(configuration, REPOSITORY, "restore-source")
    path = Path(original["feeds"]["trades"])
    previous = path.read_bytes()
    path.unlink()
    assert generate_input_sources(configuration, REPOSITORY, "restore-source") == original
    assert path.read_bytes() == previous


@pytest.mark.parametrize("options", [{"count": 2}, {"counterparty_count": None}, {"trade_count": 0}])
def test_invalid_generation_layer_requests_rejected(tmp_path, options):
    """Reject incompatible or missing dimensions before generating large source files."""
    with pytest.raises(ValueError):
        generate_input_sources({"output_root": str(tmp_path), "generation": options}, REPOSITORY, "bad-generation")


def test_full_scale_generation_ingestion_calculation_and_reports(tmp_path, pg_connection):
    """Run 2,000 counterparties and 10,000 trades through the real database pipeline with a short test horizon."""
    configuration = {"valuation_date": "2026-10-12", "output_root": str(tmp_path),
                     "generation": {"counterparty_count": 2000, "trade_count": 10000,
                                    "horizon_days": 2, "simulation_paths": 1, "seed": 17}}
    sources = generate_input_sources(configuration, REPOSITORY, "full-scale-run")
    ingestion = ingest_inputs(sources, REPOSITORY, request_key="full-scale-inputs", connection_id=pg_connection)
    frozen = validate_ingestion(ingestion)
    assert (frozen["accepted_count"], frozen["rejected_count"]) == (1, 0)
    scenario_path = simulate(frozen["snapshot_path"])
    results_path = value_batch(frozen["snapshot_path"], scenario_path)
    publication = publish(frozen["snapshot_path"], results_path, pg_connection)
    report = read_json(generate_reports(publication))
    assert report["status"] == "reported" and report["batch_id"] == frozen["batch_id"]
    with transaction(pg_connection, readonly=True) as cursor:
        for table, expected in (("input_counterparties", 2000), ("input_trades", 10000),
                                ("trade_pv", 10000), ("counterparty_xva", 2000)):
            cursor.execute(f"SELECT COUNT(*) FROM airflow_xva.{table}")
            assert cursor.fetchone()[0] == expected
        cursor.execute("SELECT COUNT(*) FROM airflow_xva.input_feed_submissions")
        assert cursor.fetchone()[0] == 6


def test_scaled_generate_demo_defaults_to_one_portfolio(tmp_path, pg_connection):
    """Accept size controls in the existing demo entrypoint without implicitly making eight large alternatives."""
    ingestion = ingest_inputs({"generate_demo": True, "valuation_date": "2026-10-12", "output_root": str(tmp_path),
                               "generation": {"counterparty_count": 5, "trade_count": 25}},
                              REPOSITORY, connection_id=pg_connection)
    frozen = validate_ingestion(ingestion)
    assert (frozen["accepted_count"], frozen["rejected_count"]) == (1, 0)
    assert len(read_json(frozen["snapshot_path"])["inputs"]["trades"]) == 25
